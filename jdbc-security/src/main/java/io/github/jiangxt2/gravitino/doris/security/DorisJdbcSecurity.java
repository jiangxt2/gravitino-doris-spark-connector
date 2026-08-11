/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.jiangxt2.gravitino.doris.security;

import java.io.IOException;
import java.io.StringReader;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/** Validates the JDBC configuration accepted by the governed Doris connector. */
public final class DorisJdbcSecurity {

  private static final String JDBC_URL = "jdbc-url";
  private static final String JDBC_DRIVER = "jdbc-driver";
  private static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
  private static final String MYSQL_URL_PREFIX = "jdbc:mysql://";
  private static final String BYPASS_PREFIX = "gravitino.bypass.";
  private static final String CONNECTION_PROPERTIES = "connectionproperties";
  private static final int MAX_DECODE_PASSES = 5;

  private static final Map<String, String> UNSAFE_MYSQL_PARAMETERS =
      normalizedNames(
          "maxAllowedPacket",
          "autoDeserialize",
          "queryInterceptors",
          "statementInterceptors",
          "detectCustomCollations",
          "allowLoadLocalInfile",
          "allowUrlInLocalInfile",
          "allowLoadLocalInfileInPath");

  private static final Map<String, String> UNSAFE_POOL_PROPERTIES =
      normalizedNames(
          "connectionFactoryClassName",
          "evictionPolicyClassName",
          "driverClassName",
          "connectionInitSqls",
          "validationQuery",
          "accessToUnderlyingConnectionAllowed",
          "jmxName",
          "registerConnectionMBean",
          "url",
          "username",
          "password",
          "initialSize");

  private static final Set<String> CREDENTIAL_PARAMETERS = Set.of("user", "username", "password");
  private static final Set<String> PROTECTED_BYPASS_PROPERTIES =
      Set.of(JDBC_URL, JDBC_DRIVER, "jdbc-user", "jdbc-password");

  private DorisJdbcSecurity() {}

  /**
   * Validates the canonical Doris JDBC URL and driver without loading the driver or opening a
   * connection.
   *
   * @param url canonical JDBC URL
   * @param driver canonical JDBC driver class name
   * @throws IllegalArgumentException if the connection configuration is outside the supported
   *     security contract
   */
  public static void validateConnection(String url, String driver) {
    if (!MYSQL_DRIVER.equals(driver)) {
      throw new IllegalArgumentException("Doris JDBC driver must be com.mysql.cj.jdbc.Driver");
    }
    validateUrl(url);
  }

  /**
   * Validates the raw Gravitino catalog properties before they can reach JDBC or DBCP.
   *
   * @param properties raw Gravitino catalog properties
   * @throws IllegalArgumentException if canonical or bypass properties are unsafe
   */
  public static void validateServerCatalogProperties(Map<String, String> properties) {
    if (properties == null) {
      throw new IllegalArgumentException("Doris JDBC catalog properties are required");
    }
    validateConnection(properties.get(JDBC_URL), properties.get(JDBC_DRIVER));

    for (Map.Entry<String, String> entry : properties.entrySet()) {
      String rawName = entry.getKey();
      if (rawName == null || rawName.isEmpty()) {
        throw new IllegalArgumentException("JDBC property name is invalid");
      }
      String decodedName = stableDecode(rawName, "JDBC property name encoding is invalid");
      validatePropertyName(decodedName, entry.getValue(), true);
      if (decodedName.regionMatches(true, 0, BYPASS_PREFIX, 0, BYPASS_PREFIX.length())) {
        validateBypassPropertyName(decodedName.substring(BYPASS_PREFIX.length()), entry.getValue());
      }
    }
  }

  private static void validateUrl(String url) {
    if (url == null || url.isEmpty() || !url.equals(url.trim())) {
      throw invalidUrl();
    }

    String decoded = stableDecode(url, "Doris JDBC URL encoding is invalid");
    if (!decoded.startsWith(MYSQL_URL_PREFIX) || decoded.indexOf('#') >= 0) {
      throw invalidUrl();
    }

    String location = decoded.substring(MYSQL_URL_PREFIX.length());
    int querySeparator = location.indexOf('?');
    if (querySeparator != location.lastIndexOf('?')) {
      throw invalidUrl();
    }
    String addressAndDatabase =
        querySeparator < 0 ? location : location.substring(0, querySeparator);
    String query = querySeparator < 0 ? null : location.substring(querySeparator + 1);

    int databaseSeparator = addressAndDatabase.indexOf('/');
    String authority =
        databaseSeparator < 0
            ? addressAndDatabase
            : addressAndDatabase.substring(0, databaseSeparator);
    String database =
        databaseSeparator < 0 ? null : addressAndDatabase.substring(databaseSeparator + 1);
    if (database != null && database.indexOf('/') >= 0) {
      throw invalidUrl();
    }

    validateAuthority(authority);
    validateDatabase(database);
    validateQuery(query);
  }

  private static void validateAuthority(String authority) {
    if (authority.isEmpty()
        || containsWhitespace(authority)
        || containsAny(authority, '@', ',', '(', ')', '=', ';', '\\')) {
      throw invalidUrl();
    }

    String host;
    String portText;
    if (authority.startsWith("[")) {
      int closingBracket = authority.indexOf(']');
      if (closingBracket <= 1
          || closingBracket != authority.lastIndexOf(']')
          || closingBracket + 1 >= authority.length()
          || authority.charAt(closingBracket + 1) != ':') {
        throw invalidUrl();
      }
      host = authority.substring(1, closingBracket);
      portText = authority.substring(closingBracket + 2);
      if (!isValidIpv6Literal(host)) {
        throw invalidUrl();
      }
    } else {
      int portSeparator = authority.lastIndexOf(':');
      if (portSeparator <= 0
          || portSeparator != authority.indexOf(':')
          || portSeparator + 1 >= authority.length()) {
        throw invalidUrl();
      }
      host = authority.substring(0, portSeparator);
      portText = authority.substring(portSeparator + 1);
      if (!host.matches("[A-Za-z0-9._-]+")) {
        throw invalidUrl();
      }
    }

    if (!portText.matches("[0-9]+")) {
      throw invalidUrl();
    }
    try {
      int port = Integer.parseInt(portText);
      if (port < 1 || port > 65535) {
        throw invalidUrl();
      }
    } catch (NumberFormatException e) {
      throw invalidUrl();
    }
  }

  private static void validateDatabase(String database) {
    if (database == null || database.isEmpty()) {
      return;
    }
    if (containsWhitespace(database)
        || containsAny(database, '/', '?', '#', '@', ',', '(', ')', '\\')) {
      throw invalidUrl();
    }
  }

  private static void validateQuery(String query) {
    if (query == null) {
      return;
    }
    if (query.isEmpty()) {
      throw invalidUrl();
    }

    Set<String> seenNames = new java.util.HashSet<>();
    for (String assignment : query.split("&", -1)) {
      int equals = assignment.indexOf('=');
      if (equals <= 0) {
        throw invalidUrl();
      }
      String name = assignment.substring(0, equals);
      if (containsWhitespace(name) || containsAny(name, '=', '?', '#')) {
        throw invalidUrl();
      }
      String normalized = normalize(name, "Doris JDBC URL encoding is invalid");
      if (!seenNames.add(normalized)) {
        throw new IllegalArgumentException("Duplicate Doris JDBC URL parameter is not allowed");
      }
      rejectKnownParameter(normalized);
    }
  }

  private static void validateBypassPropertyName(String name, String value) {
    String normalized = normalize(name, "JDBC property name encoding is invalid");
    if (PROTECTED_BYPASS_PROPERTIES.contains(normalized)) {
      throw new IllegalArgumentException(
          "Protected JDBC property '" + normalized + "' must not use gravitino.bypass");
    }
    validatePropertyName(name, value, true);
  }

  private static void validatePropertyName(
      String name, String value, boolean allowConnectionProperties) {
    if (name == null || name.isEmpty()) {
      throw new IllegalArgumentException("JDBC property name is invalid");
    }
    String normalized = normalize(name, "JDBC property name encoding is invalid");
    String unsafePoolProperty = UNSAFE_POOL_PROPERTIES.get(normalized);
    if (unsafePoolProperty != null) {
      throw new IllegalArgumentException(
          "Unsafe JDBC connection pool property '" + unsafePoolProperty + "' is not allowed");
    }
    rejectKnownParameter(normalized);
    if (CONNECTION_PROPERTIES.equals(normalized)) {
      if (!allowConnectionProperties) {
        throw new IllegalArgumentException("Nested JDBC connectionProperties is not allowed");
      }
      validateConnectionProperties(value);
    }
  }

  private static void validateConnectionProperties(String value) {
    if (value == null) {
      throw new IllegalArgumentException("Unable to validate JDBC connectionProperties");
    }
    Properties parsed = new Properties();
    try {
      parsed.load(new StringReader(value.replace(';', '\n')));
    } catch (IOException | IllegalArgumentException e) {
      throw new IllegalArgumentException("Unable to validate JDBC connectionProperties");
    }
    for (String name : parsed.stringPropertyNames()) {
      validatePropertyName(name, parsed.getProperty(name), false);
    }
  }

  private static boolean isValidIpv6Literal(String host) {
    if (host.indexOf(':') < 0 || !host.matches("[0-9A-Fa-f:]+")) {
      return false;
    }

    int compression = host.indexOf("::");
    if (compression != host.lastIndexOf("::")) {
      return false;
    }
    if (compression < 0) {
      return countIpv6Hextets(host) == 8;
    }

    int left = countIpv6Hextets(host.substring(0, compression));
    int right = countIpv6Hextets(host.substring(compression + 2));
    return left >= 0 && right >= 0 && left + right < 8;
  }

  private static int countIpv6Hextets(String value) {
    if (value.isEmpty()) {
      return 0;
    }
    String[] hextets = value.split(":", -1);
    for (String hextet : hextets) {
      if (hextet.isEmpty() || hextet.length() > 4 || !hextet.matches("[0-9A-Fa-f]+")) {
        return -1;
      }
    }
    return hextets.length;
  }

  private static void rejectKnownParameter(String normalized) {
    if (CREDENTIAL_PARAMETERS.contains(normalized)) {
      throw new IllegalArgumentException(
          "JDBC credentials must not be embedded in connection configuration");
    }
    String unsafeParameter = UNSAFE_MYSQL_PARAMETERS.get(normalized);
    if (unsafeParameter != null) {
      throw new IllegalArgumentException(
          "Unsafe Doris JDBC parameter '" + unsafeParameter + "' is not allowed");
    }
  }

  private static String normalize(String value, String failureMessage) {
    return stableDecode(value, failureMessage).toLowerCase(Locale.ROOT);
  }

  private static String stableDecode(String value, String failureMessage) {
    String current = value;
    for (int pass = 0; pass < MAX_DECODE_PASSES; pass++) {
      String decoded = decode(current, failureMessage);
      if (decoded.equals(current)) {
        return current;
      }
      current = decoded;
    }
    if (!decode(current, failureMessage).equals(current)) {
      throw new IllegalArgumentException(failureMessage);
    }
    return current;
  }

  private static String decode(String value, String failureMessage) {
    try {
      return URLDecoder.decode(value, StandardCharsets.UTF_8);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(failureMessage);
    }
  }

  private static boolean containsWhitespace(String value) {
    return value.chars().anyMatch(Character::isWhitespace);
  }

  private static boolean containsAny(String value, char... characters) {
    for (char character : characters) {
      if (value.indexOf(character) >= 0) {
        return true;
      }
    }
    return false;
  }

  private static Map<String, String> normalizedNames(String... names) {
    Map<String, String> normalized = new LinkedHashMap<>();
    for (String name : names) {
      normalized.put(name.toLowerCase(Locale.ROOT), name);
    }
    return Map.copyOf(normalized);
  }

  private static IllegalArgumentException invalidUrl() {
    return new IllegalArgumentException(
        "Doris JDBC URL must use ordinary single-host jdbc:mysql://host:port syntax");
  }
}
