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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/** Validates the JDBC configuration accepted by the governed Doris connector. */
public final class DorisJdbcSecurity {

  private static final String JDBC_URL = "jdbc-url";
  private static final String JDBC_DRIVER = "jdbc-driver";
  public static final String READ_TRANSPORT = "doris-read-transport";
  public static final String HYBRID_TRANSPORT = "hybrid";
  public static final String STRICT_JDBC_TLS_TRANSPORT = "strict-jdbc-tls";
  public static final String DORIS_FE_NODES = "doris-fenodes";
  public static final String DORIS_QUERY_PORT = "doris-query-port";
  private static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
  private static final String MYSQL_URL_PREFIX = "jdbc:mysql://";
  private static final String BYPASS_PREFIX = "gravitino.bypass.";
  private static final String SPARK_BYPASS_PREFIX = "spark.bypass.";
  private static final String CONNECTION_PROPERTIES = "connectionproperties";
  private static final int MAX_DECODE_PASSES = 5;
  private static final Set<String> URL_PARAMETER_ALLOWLIST =
      Set.of("connecttimeout", "sockettimeout");
  private static final Set<String> STRICT_URL_PARAMETER_ALLOWLIST =
      Set.of("sslmode", "fallbacktosystemtruststore");
  private static final Set<String> CONNECTION_PROPERTY_ALLOWLIST =
      Set.of("connecttimeout", "sockettimeout");
  private static final Set<String> BYPASS_PROPERTY_ALLOWLIST =
      Set.of("maxidle", "connecttimeout", "sockettimeout", CONNECTION_PROPERTIES);

  private static final Map<String, String> UNSAFE_MYSQL_PARAMETERS =
      normalizedNames(
          "maxAllowedPacket",
          "autoDeserialize",
          "queryInterceptors",
          "statementInterceptors",
          "detectCustomCollations",
          "allowLoadLocalInfile",
          "allowUrlInLocalInfile",
          "allowLoadLocalInfileInPath",
          // Connector/J 8.0.33 class-loading and transport-rewrite parameters: after this
          // validator returns, these can instantiate arbitrary classes or replace endpoint, TLS,
          // or authentication behavior, breaking the verified transport contract.
          "propertiesTransform",
          "socketFactory",
          "protocol",
          "connectionLifecycleInterceptors",
          "exceptionInterceptors",
          "profilerEventHandler",
          "clientInfoProvider",
          "serverConfigCacheFactory",
          "queryInfoCacheFactory",
          "parseInfoCacheFactory",
          "logger",
          "useConfigs",
          "defaultAuthenticationPlugin",
          "disabledAuthenticationPlugins",
          "authenticationPlugins",
          "authenticationFidoCallbackHandler",
          "loadBalanceExceptionChecker",
          "dnsSrv",
          // Connector/J 8.0.33 exposes these as ha.loadBalanceStrategy/ha.enableJMX with the
          // haLoadBalanceStrategy/haEnableJMX aliases; there is no bare loadBalanceStrategy key.
          "ha.loadBalanceStrategy",
          "haLoadBalanceStrategy",
          "ha.enableJMX",
          "haEnableJMX",
          // Client keystore material is a persistent secret and a keystore-loading sink; the
          // strict profile never needs it because mTLS client authentication is out of scope.
          "clientCertificateKeyStoreUrl",
          "clientCertificateKeyStoreType",
          "clientCertificateKeyStorePassword",
          "fallbackToSystemKeyStore",
          "serverRSAPublicKeyFile",
          "ociConfigFile",
          "ociConfigProfile",
          "ldapServerHostname",
          "allowPublicKeyRetrieval",
          "localSocketAddress",
          // These diagnostic switches can copy complete SQL text, protocol packets, or process
          // thread state into logs, stderr, or exception text outside the connector's redaction
          // boundary.
          "autoGenerateTestcaseScript",
          "dumpQueriesOnException",
          "enablePacketDebug",
          "explainSlowQueries",
          "includeInnodbStatusInDeadlockExceptions",
          "includeThreadDumpInDeadlockExceptions",
          "includeThreadNamesAsStatementComment",
          "gatherPerfMetrics",
          "logXaCommands",
          "logSlowQueries",
          "profileSQL",
          "traceProtocol",
          "useUsageAdvisor",
          // Setting socksProxyHost alone makes ConnectionImpl rewrite socketFactory to
          // SocksProxySocketFactory, so the proxy family must be rejected with the factory it
          // can replace. createDatabaseIfNotExist and sessionVariables execute DDL or
          // SET SESSION statements during connection initialization (NativeProtocol), outside
          // the read-only capability boundary.
          "socksProxyHost",
          "socksProxyPort",
          "socksProxyRemoteDns",
          "createDatabaseIfNotExist",
          "sessionVariables");

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

  private static final Set<String> CREDENTIAL_PARAMETERS =
      Set.of("user", "username", "password", "password1", "password2", "password3");
  private static final Set<String> PROTECTED_BYPASS_PROPERTIES =
      Set.of(JDBC_URL, JDBC_DRIVER, "jdbc-user", "jdbc-password", READ_TRANSPORT);
  private static final Set<String> CANONICAL_PROFILE_PROPERTIES =
      Set.of(
          JDBC_URL,
          JDBC_DRIVER,
          "jdbc-user",
          "jdbc-password",
          READ_TRANSPORT,
          DORIS_FE_NODES,
          DORIS_QUERY_PORT);

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
    validateConnection(url, driver, HYBRID_TRANSPORT);
  }

  /**
   * Validates a canonical Doris JDBC connection for the selected read transport.
   *
   * @param url canonical JDBC URL
   * @param driver canonical JDBC driver class name
   * @param readTransport governed read transport value
   */
  public static void validateConnection(String url, String driver, String readTransport) {
    if (!MYSQL_DRIVER.equals(driver)) {
      throw new IllegalArgumentException("Doris JDBC driver must be com.mysql.cj.jdbc.Driver");
    }
    String transport = validateReadTransport(readTransport);
    validateUrl(url, STRICT_JDBC_TLS_TRANSPORT.equals(transport));
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
    Set<String> seenCanonicalProperties = new HashSet<>();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      String rawName = entry.getKey();
      if (rawName == null || rawName.isEmpty()) {
        throw new IllegalArgumentException("JDBC property name is invalid");
      }
      String decodedName = stableDecode(rawName, "JDBC property name encoding is invalid");
      String normalizedName = decodedName.toLowerCase(Locale.ROOT);
      if (CANONICAL_PROFILE_PROPERTIES.contains(normalizedName)) {
        if (!rawName.equals(normalizedName) || !seenCanonicalProperties.add(normalizedName)) {
          throw new IllegalArgumentException(
              "Governed Doris profile properties must use their canonical names once");
        }
      }
    }

    String readTransport = readTransport(properties);
    boolean strictTransport = STRICT_JDBC_TLS_TRANSPORT.equals(readTransport);
    validateConnection(properties.get(JDBC_URL), properties.get(JDBC_DRIVER), readTransport);
    validateTransportProperties(properties, strictTransport);

    for (Map.Entry<String, String> entry : properties.entrySet()) {
      String decodedName = stableDecode(entry.getKey(), "JDBC property name encoding is invalid");
      if (decodedName.regionMatches(true, 0, BYPASS_PREFIX, 0, BYPASS_PREFIX.length())) {
        validateBypassPropertyName(
            decodedName.substring(BYPASS_PREFIX.length()), entry.getValue(), strictTransport);
      } else if (decodedName.regionMatches(
          true, 0, SPARK_BYPASS_PREFIX, 0, SPARK_BYPASS_PREFIX.length())) {
        validateSparkBypassPropertyName(
            decodedName.substring(SPARK_BYPASS_PREFIX.length()), strictTransport);
      } else {
        validateDirectCatalogProperty(decodedName, entry.getValue(), strictTransport);
      }
    }
  }

  /** Returns the exact governed read transport, applying the compatible default when absent. */
  public static String readTransport(Map<String, String> properties) {
    if (properties == null) {
      throw new IllegalArgumentException("Doris JDBC catalog properties are required");
    }
    String value = properties.get(READ_TRANSPORT);
    return validateReadTransport(value == null ? HYBRID_TRANSPORT : value);
  }

  private static String validateReadTransport(String value) {
    if (!HYBRID_TRANSPORT.equals(value) && !STRICT_JDBC_TLS_TRANSPORT.equals(value)) {
      throw new IllegalArgumentException(
          "Catalog property doris-read-transport must be hybrid or strict-jdbc-tls");
    }
    return value;
  }

  private static void validateTransportProperties(
      Map<String, String> properties, boolean strictTransport) {
    if (strictTransport) {
      if (properties.containsKey(DORIS_FE_NODES) || properties.containsKey(DORIS_QUERY_PORT)) {
        throw new IllegalArgumentException(
            "Strict JDBC TLS transport must not configure native Doris endpoints");
      }
      return;
    }
    if (isBlank(properties.get(DORIS_FE_NODES))) {
      throw new IllegalArgumentException(
          "Hybrid Doris reads require catalog property doris-fenodes");
    }
    String queryPort = properties.get(DORIS_QUERY_PORT);
    if (queryPort == null || !queryPort.matches("[0-9]+")) {
      throw new IllegalArgumentException(
          "Hybrid Doris reads require catalog property doris-query-port");
    }
    try {
      int port = Integer.parseInt(queryPort);
      if (port < 1 || port > 65535) {
        throw new NumberFormatException("port out of range");
      }
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("doris-query-port must be between 1 and 65535");
    }
  }

  private static void validateUrl(String url, boolean strictTransport) {
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
    Map<String, String> parameters = validateQuery(query, strictTransport);
    if (strictTransport) {
      validateStrictTlsParameters(parameters);
    }
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

  private static Map<String, String> validateQuery(String query, boolean strictTransport) {
    Map<String, String> parameters = new LinkedHashMap<>();
    if (query == null) {
      return parameters;
    }
    if (query.isEmpty()) {
      throw invalidUrl();
    }

    Set<String> seenNames = new HashSet<>();
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
      if (!URL_PARAMETER_ALLOWLIST.contains(normalized)
          && !(strictTransport && STRICT_URL_PARAMETER_ALLOWLIST.contains(normalized))) {
        throw new IllegalArgumentException("Unreviewed Doris JDBC URL parameter is not allowed");
      }
      parameters.put(
          normalized,
          stableDecode(assignment.substring(equals + 1), "Doris JDBC URL encoding is invalid"));
    }
    return parameters;
  }

  private static void validateStrictTlsParameters(Map<String, String> parameters) {
    String sslMode = parameters.get("sslmode");
    if (!"VERIFY_IDENTITY".equals(sslMode)) {
      throw new IllegalArgumentException(
          "Strict JDBC TLS transport requires sslMode=VERIFY_IDENTITY");
    }
    for (Map.Entry<String, String> entry : parameters.entrySet()) {
      String name = entry.getKey();
      if ("sslmode".equals(name)) {
        continue;
      }
      if ("fallbacktosystemtruststore".equals(name)) {
        if (!"true".equals(entry.getValue())) {
          throw new IllegalArgumentException(
              "Strict JDBC TLS parameter fallbackToSystemTrustStore must be true");
        }
        continue;
      }
      if (URL_PARAMETER_ALLOWLIST.contains(name)) {
        continue;
      }
      throw new IllegalArgumentException(
          "Unreviewed JDBC TLS parameters are not allowed in strict transport");
    }
  }

  private static void validateBypassPropertyName(
      String name, String value, boolean strictTransport) {
    String normalized = normalize(name, "JDBC property name encoding is invalid");
    if (PROTECTED_BYPASS_PROPERTIES.contains(normalized)) {
      throw new IllegalArgumentException("Protected JDBC properties must not use gravitino.bypass");
    }
    if (!BYPASS_PROPERTY_ALLOWLIST.contains(normalized)) {
      throw new IllegalArgumentException("Unreviewed JDBC connection pool property is not allowed");
    }
    if (CONNECTION_PROPERTIES.equals(normalized)) {
      validateConnectionProperties(value, strictTransport);
    }
  }

  private static void validateSparkBypassPropertyName(String name, boolean strictTransport) {
    String normalized = normalize(name, "JDBC property name encoding is invalid");
    if (PROTECTED_BYPASS_PROPERTIES.contains(normalized)) {
      throw new IllegalArgumentException("Protected JDBC properties must not use spark.bypass");
    }
    if (strictTransport && normalized.startsWith("doris.")) {
      throw new IllegalArgumentException(
          "Strict JDBC TLS transport must not configure native Doris options");
    }
  }

  private static void validateDirectCatalogProperty(
      String name, String value, boolean strictTransport) {
    if (name == null || name.isEmpty()) {
      throw new IllegalArgumentException("JDBC property name is invalid");
    }
    String normalized = normalize(name, "JDBC property name encoding is invalid");
    if (UNSAFE_POOL_PROPERTIES.containsKey(normalized)) {
      throw new IllegalArgumentException("Unreviewed JDBC connection pool property is not allowed");
    }
    rejectKnownParameter(normalized);
    if (strictTransport && isTlsControlParameter(normalized)) {
      throw new IllegalArgumentException(
          "JDBC TLS properties are not allowed outside the canonical URL");
    }
    if (CONNECTION_PROPERTIES.equals(normalized)) {
      validateConnectionProperties(value, strictTransport);
    }
  }

  private static void validateConnectionProperties(String value, boolean strictTransport) {
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
      String normalized = normalize(name, "JDBC property name encoding is invalid");
      if (CONNECTION_PROPERTIES.equals(normalized)) {
        throw new IllegalArgumentException("Nested JDBC connectionProperties is not allowed");
      }
      if (!CONNECTION_PROPERTY_ALLOWLIST.contains(normalized)) {
        throw new IllegalArgumentException("Unreviewed JDBC connection property is not allowed");
      }
      if (strictTransport && isTlsControlParameter(normalized)) {
        throw new IllegalArgumentException(
            "JDBC TLS properties are not allowed outside the canonical URL");
      }
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
    if (UNSAFE_MYSQL_PARAMETERS.containsKey(normalized)) {
      throw new IllegalArgumentException("Unreviewed Doris JDBC parameter is not allowed");
    }
  }

  private static boolean isTlsControlParameter(String normalized) {
    return normalized.contains("ssl")
        || normalized.contains("tls")
        || normalized.contains("truststore")
        || normalized.contains("keystore")
        || normalized.contains("certificate");
  }

  private static boolean isBlank(String value) {
    return value == null || value.trim().isEmpty();
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
