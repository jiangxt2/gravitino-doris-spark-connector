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

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

public class TestDorisJdbcSecurity {

  private static final String DRIVER = "com.mysql.cj.jdbc.Driver";

  @ParameterizedTest
  @ValueSource(
      strings = {
        "jdbc:mysql://fe:9030/",
        "jdbc:mysql://127.0.0.1:9030/analytics",
        "jdbc:mysql://doris-fe.example.com:9030?socketTimeout=1000",
        "jdbc:mysql://[2001:db8:0:1:2:3:4:5]:9030/analytics",
        "jdbc:mysql://[2001:db8::1]:9030/analytics?connectTimeout=1000&sslMode=VERIFY_IDENTITY"
      })
  void acceptsSupportedOrdinaryUrls(String url) {
    assertThatCode(() -> DorisJdbcSecurity.validateConnection(url, DRIVER))
        .doesNotThrowAnyException();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "com.mysql.jdbc.Driver",
        "COM.MYSQL.CJ.JDBC.DRIVER",
        "org.mariadb.jdbc.Driver",
        "org.h2.Driver",
        "example.CustomDriver",
        ""
      })
  void rejectsEveryDriverExceptTheOfficialClass(String driver) {
    assertThatThrownBy(() -> DorisJdbcSecurity.validateConnection("jdbc:mysql://fe:9030/", driver))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Doris JDBC driver must be com.mysql.cj.jdbc.Driver");
  }

  @ParameterizedTest
  @MethodSource("unsupportedUrls")
  void rejectsUnsupportedOrMalformedUrls(String url) {
    assertThatThrownBy(() -> DorisJdbcSecurity.validateConnection(url, DRIVER))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageNotContaining("secret");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "maxAllowedPacket",
        "autoDeserialize",
        "queryInterceptors",
        "statementInterceptors",
        "detectCustomCollations",
        "allowLoadLocalInfile",
        "allowUrlInLocalInfile",
        "allowLoadLocalInfileInPath"
      })
  void rejectsEveryUnsafeMysqlParameterInUrlAndRawConfig(String parameter) {
    assertThatThrownBy(
            () ->
                DorisJdbcSecurity.validateConnection(
                    "jdbc:mysql://fe:9030/?" + parameter + "=true", DRIVER))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsafe Doris JDBC parameter")
        .hasMessageContaining(parameter);

    Map<String, String> properties = validServerProperties();
    properties.put("gravitino.bypass." + parameter.toUpperCase(Locale.ROOT), "true");
    assertThatThrownBy(() -> DorisJdbcSecurity.validateServerCatalogProperties(properties))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsafe Doris JDBC parameter")
        .hasMessageContaining(parameter);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
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
        "initialSize"
      })
  void rejectsUnsafeDbcpProperties(String property) {
    Map<String, String> direct = validServerProperties();
    direct.put(property.toUpperCase(Locale.ROOT), "secret-canary");
    assertThatThrownBy(() -> DorisJdbcSecurity.validateServerCatalogProperties(direct))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsafe JDBC connection pool property")
        .hasMessageNotContaining("secret-canary");

    Map<String, String> bypass = validServerProperties();
    bypass.put("gravitino.bypass." + property, "secret-canary");
    assertThatThrownBy(() -> DorisJdbcSecurity.validateServerCatalogProperties(bypass))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsafe JDBC connection pool property")
        .hasMessageNotContaining("secret-canary");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "gravitino.bypass.jdbc-url",
        "GRAVITINO.BYPASS.JDBC-DRIVER",
        "gravitino%2Ebypass%2Ejdbc-user",
        "gravitino%252Ebypass%252Ejdbc-password"
      })
  void rejectsCanonicalJdbcPropertiesThroughBypass(String property) {
    Map<String, String> properties = validServerProperties();
    properties.put(property, "bypass-secret-canary");

    assertThatThrownBy(() -> DorisJdbcSecurity.validateServerCatalogProperties(properties))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Protected JDBC property")
        .hasMessageNotContaining("bypass-secret-canary");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "GRAVITINO.BYPASS.connectionFactoryClassName",
        "Gravitino.Bypass.URL",
        "gravitino%2Ebypass%2EdriverClassName",
        "gravitino%252Ebypass%252EinitialSize",
        "gravitino%2Ebypass%2EconnectionInitSqls",
        "gravitino%252Ebypass%252EaccessToUnderlyingConnectionAllowed",
        "GRAVITINO.BYPASS.JMXNAME"
      })
  void rejectsCaseAndEncodedBypassPrefixes(String property) {
    Map<String, String> properties = validServerProperties();
    properties.put(property, "secret-canary");

    assertThatThrownBy(() -> DorisJdbcSecurity.validateServerCatalogProperties(properties))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsafe JDBC connection pool property")
        .hasMessageNotContaining("secret-canary");
  }

  @ParameterizedTest
  @MethodSource("unsafeConnectionProperties")
  void rejectsConnectionPropertiesSmuggling(String value) {
    Map<String, String> properties = validServerProperties();
    properties.put("gravitino.bypass.connectionProperties", value);

    assertThatThrownBy(() -> DorisJdbcSecurity.validateServerCatalogProperties(properties))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageNotContaining(value)
        .hasMessageNotContaining("secret-canary");
  }

  @Test
  void validatesConnectionPropertiesThroughCaseAndEncodedBypassPrefixes() {
    for (String key :
        new String[] {
          "GRAVITINO.BYPASS.ConnectionProperties", "gravitino%2Ebypass%2EconnectionProperties"
        }) {
      Map<String, String> properties = validServerProperties();
      properties.put(key, "auto%2544eserialize=true");
      assertThatThrownBy(() -> DorisJdbcSecurity.validateServerCatalogProperties(properties))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("autoDeserialize");
    }

    Map<String, String> nullValue = validServerProperties();
    nullValue.put("gravitino.bypass.connectionProperties", null);
    assertThatThrownBy(() -> DorisJdbcSecurity.validateServerCatalogProperties(nullValue))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unable to validate JDBC connectionProperties");
  }

  @Test
  void rejectsNestedConnectionPropertiesWithoutRecursion() {
    String nested = "socketTimeout=1000";
    for (int depth = 0; depth < 1000; depth++) {
      nested = "connectionProperties=" + nested;
    }
    Map<String, String> properties = validServerProperties();
    properties.put("gravitino.bypass.connectionProperties", nested);

    assertThatThrownBy(() -> DorisJdbcSecurity.validateServerCatalogProperties(properties))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("Nested JDBC connectionProperties is not allowed")
        .hasMessageNotContaining("socketTimeout");
  }

  @Test
  void acceptsSafePropertiesAndDoesNotMatchUnsafeWordsInValues() {
    Map<String, String> properties = validServerProperties();
    properties.put("gravitino.bypass.socketTimeout", "1000");
    properties.put(
        "gravitino.bypass.connectionProperties",
        "connectTimeout=1000;description=autoDeserialize;safeAutoDeserializeSuffix=true");

    assertThatCode(() -> DorisJdbcSecurity.validateServerCatalogProperties(properties))
        .doesNotThrowAnyException();
  }

  @Test
  void decodesUnsafeNamesRecursivelyAndRejectsExcessiveEncoding() {
    assertThatThrownBy(
            () ->
                DorisJdbcSecurity.validateConnection(
                    "jdbc:mysql://fe:9030/?auto%2544eserialize=true", DRIVER))
        .hasMessageContaining("autoDeserialize");
    assertThatThrownBy(
            () ->
                DorisJdbcSecurity.validateConnection(
                    "jdbc:mysql://fe:9030/?auto%252525252544eserialize=true", DRIVER))
        .hasMessage("Doris JDBC URL encoding is invalid");
  }

  @Test
  void usesLocaleRootForSecurityMatching() {
    Locale original = Locale.getDefault();
    try {
      Locale.setDefault(Locale.forLanguageTag("tr-TR"));
      Map<String, String> properties = validServerProperties();
      properties.put("gravitino.bypass.INITIALSIZE", "1");
      assertThatThrownBy(() -> DorisJdbcSecurity.validateServerCatalogProperties(properties))
          .hasMessageContaining("initialSize");
    } finally {
      Locale.setDefault(original);
    }
  }

  @Test
  void neverEchoesConnectionMaterialOnFailure() {
    String secretUrl = "jdbc:mysql://sensitive-user:secret@private-host:9030/";
    assertThatThrownBy(() -> DorisJdbcSecurity.validateConnection(secretUrl, DRIVER))
        .hasMessageNotContaining(secretUrl)
        .hasMessageNotContaining("sensitive-user")
        .hasMessageNotContaining("secret")
        .hasMessageNotContaining("private-host");
  }

  private static Stream<String> unsupportedUrls() {
    return Stream.of(
        "jdbc:mysql://fe:9030/ ",
        " jdbc:mysql://fe:9030/",
        "jdbc:mariadb://fe:9030/",
        "jdbc:mysql:loadbalance://fe1:9030,fe2:9030/",
        "jdbc:mysql:replication://fe1:9030,fe2:9030/",
        "jdbc:mysql+srv://fe/",
        "jdbc:mysql://fe1:9030,fe2:9030/",
        "jdbc:mysql://address=(host=fe)(port=9030)/",
        "jdbc:mysql://(host=fe,port=9030)/",
        "jdbc:mysql://user:secret@fe:9030/",
        "jdbc:mysql://fe:9030/?user=reader",
        "jdbc:mysql://fe:9030/?password=secret",
        "jdbc:mysql://fe:9030/?safe=true&safe=false",
        "jdbc:mysql://fe:9030/?=value",
        "jdbc:mysql://fe:9030/?safe",
        "jdbc:mysql://fe:9030/?",
        "jdbc:mysql://fe/",
        "jdbc:mysql://fe:0/",
        "jdbc:mysql://fe:65536/",
        "jdbc:mysql://fe:not-a-port/",
        "jdbc:mysql://[:::]:9030/",
        "jdbc:mysql://[2001:db8::1::2]:9030/",
        "jdbc:mysql://[2001:db8:0:1:2:3:4:5:6]:9030/",
        "jdbc:mysql://[2001:db8:00000::1]:9030/",
        "jdbc:mysql://[2001:db8:0:1:2:3:4]:9030/",
        "jdbc:mysql://fe:9030/db/extra",
        "jdbc:mysql://fe:9030/#fragment",
        "jdbc:mysql://fe:9030/?bad%encoding=true",
        "jdbc:mysql://fe:9030/?safe=x%26autoDeserialize%3Dtrue",
        "jdbc:mysql://%28host%3Dfe%2Cport%3D9030%29/");
  }

  private static Stream<String> unsafeConnectionProperties() {
    return Stream.of(
        "autoDeserialize=true",
        "socketTimeout=1;queryInterceptors=example.Evil",
        "socketTimeout=1\nallowLoadLocalInfile=true",
        "socketTimeout=1; autoDeserialize = true",
        "autoDese\\\nrialize=true",
        "auto\\u0044eserialize=true",
        "auto%2544eserialize=true",
        "password=secret-canary",
        "driverClassName=example.SecretCanary",
        "bad\\" + "u12G4=secret-canary");
  }

  private static Map<String, String> validServerProperties() {
    Map<String, String> properties = new HashMap<>();
    properties.put("jdbc-url", "jdbc:mysql://fe:9030/");
    properties.put("jdbc-driver", DRIVER);
    properties.put("jdbc-user", "reader");
    properties.put("jdbc-password", "secret-canary");
    return properties;
  }
}
