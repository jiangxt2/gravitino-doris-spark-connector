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
        "jdbc:mysql://[2001:db8::1]:9030/analytics?connectTimeout=1000"
      })
  void acceptsSupportedOrdinaryUrls(String url) {
    assertThatCode(() -> DorisJdbcSecurity.validateConnection(url, DRIVER))
        .doesNotThrowAnyException();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "jdbc:mysql://fe:9030/?description=secret-canary",
        "jdbc:mysql://fe:9030/?connectTimeout=1000&unknown-secret-canary=true"
      })
  void rejectsUnknownUrlParametersWithoutDisclosingTheirNames(String url) {
    assertThatThrownBy(() -> DorisJdbcSecurity.validateConnection(url, DRIVER))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unreviewed Doris JDBC URL parameter is not allowed")
        .hasMessageNotContaining("secret-canary");
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
        "allowLoadLocalInfileInPath",
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
        "ha.loadBalanceStrategy",
        "haLoadBalanceStrategy",
        "ha.enableJMX",
        "haEnableJMX",
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
        "socksProxyHost",
        "socksProxyPort",
        "socksProxyRemoteDns",
        "createDatabaseIfNotExist",
        "sessionVariables"
      })
  void rejectsEveryUnsafeMysqlParameterInUrlAndRawConfig(String parameter) {
    assertThatThrownBy(
            () ->
                DorisJdbcSecurity.validateConnection(
                    "jdbc:mysql://fe:9030/?" + parameter + "=true", DRIVER))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unreviewed Doris JDBC URL parameter is not allowed")
        .hasMessageNotContaining(parameter);

    Map<String, String> properties = validServerProperties();
    properties.put("gravitino.bypass." + parameter.toUpperCase(Locale.ROOT), "true");
    assertThatThrownBy(() -> DorisJdbcSecurity.validateServerCatalogProperties(properties))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unreviewed JDBC connection pool property is not allowed")
        .hasMessageNotContaining(parameter);

    Map<String, String> direct = validServerProperties();
    direct.put(parameter.toUpperCase(Locale.ROOT), "true");
    assertThatThrownBy(() -> DorisJdbcSecurity.validateServerCatalogProperties(direct))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unreviewed Doris JDBC parameter is not allowed")
        .hasMessageNotContaining(parameter);

    Map<String, String> connectionProperties = validServerProperties();
    connectionProperties.put("gravitino.bypass.connectionProperties", parameter + "=true");
    assertThatThrownBy(
            () -> DorisJdbcSecurity.validateServerCatalogProperties(connectionProperties))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unreviewed JDBC connection property is not allowed")
        .hasMessageNotContaining(parameter);
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
        .hasMessage("Unreviewed JDBC connection pool property is not allowed")
        .hasMessageNotContaining("secret-canary");

    Map<String, String> bypass = validServerProperties();
    bypass.put("gravitino.bypass." + property, "secret-canary");
    assertThatThrownBy(() -> DorisJdbcSecurity.validateServerCatalogProperties(bypass))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unreviewed JDBC connection pool property is not allowed")
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
        .hasMessage("Protected JDBC properties must not use gravitino.bypass")
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
        .hasMessage("Unreviewed JDBC connection pool property is not allowed")
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
          .hasMessage("Unreviewed JDBC connection property is not allowed")
          .hasMessageNotContaining("autoDeserialize");
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
  void acceptsAllowedPropertiesAndDoesNotMatchUnsafeWordsInValues() {
    Map<String, String> properties = validServerProperties();
    properties.put("gravitino.bypass.socketTimeout", "1000");
    properties.put("gravitino.bypass.maxIdle", "0");
    properties.put("gravitino.bypass.connectionProperties", "connectTimeout=autoDeserialize");

    assertThatCode(() -> DorisJdbcSecurity.validateServerCatalogProperties(properties))
        .doesNotThrowAnyException();
  }

  @ParameterizedTest
  @ValueSource(strings = {"gravitino.bypass.description", "gravitino.bypass.unknown-secret-canary"})
  void rejectsUnknownBypassPropertiesWithoutDisclosingTheirNames(String property) {
    Map<String, String> properties = validServerProperties();
    properties.put(property, "value-secret-canary");

    assertThatThrownBy(() -> DorisJdbcSecurity.validateServerCatalogProperties(properties))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unreviewed JDBC connection pool property is not allowed")
        .hasMessageNotContaining("secret-canary");
  }

  @Test
  void rejectsUnknownConnectionPropertiesWithoutDisclosingTheirNames() {
    Map<String, String> properties = validServerProperties();
    properties.put(
        "gravitino.bypass.connectionProperties", "unknown-secret-canary=value-secret-canary");

    assertThatThrownBy(() -> DorisJdbcSecurity.validateServerCatalogProperties(properties))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unreviewed JDBC connection property is not allowed")
        .hasMessageNotContaining("secret-canary");
  }

  @Test
  void acceptsStrictVerifiedIdentityWithoutNativeEndpoints() {
    Map<String, String> properties = strictServerProperties();

    assertThatCode(() -> DorisJdbcSecurity.validateServerCatalogProperties(properties))
        .doesNotThrowAnyException();

    properties.put("jdbc-url", properties.get("jdbc-url") + "&fallbackToSystemTrustStore=true");
    assertThatCode(() -> DorisJdbcSecurity.validateServerCatalogProperties(properties))
        .doesNotThrowAnyException();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "jdbc:mysql://fe:9030/",
        "jdbc:mysql://fe:9030/?sslMode=PREFERRED",
        "jdbc:mysql://fe:9030/?sslMode=REQUIRED",
        "jdbc:mysql://fe:9030/?sslMode=VERIFY_CA",
        "jdbc:mysql://fe:9030/?sslMode=verify_identity",
        "jdbc:mysql://fe:9030/?sslMode=VERIFY_IDENTITY&fallbackToSystemTrustStore=false",
        "jdbc:mysql://fe:9030/?sslMode=VERIFY_IDENTITY&useSSL=true",
        "jdbc:mysql://fe:9030/?sslMode=VERIFY_IDENTITY&requireSSL=true",
        "jdbc:mysql://fe:9030/?sslMode=VERIFY_IDENTITY&verifyServerCertificate=true",
        "jdbc:mysql://fe:9030/?sslMode=VERIFY_IDENTITY&trustCertificateKeyStoreUrl=file:test",
        "jdbc:mysql://fe:9030/?sslMode=VERIFY_IDENTITY&trustCertificateKeyStoreType=PKCS12",
        "jdbc:mysql://fe:9030/?sslMode=VERIFY_IDENTITY&trustCertificateKeyStorePassword=secret-canary",
        "jdbc:mysql://fe:9030/?sslMode=VERIFY_IDENTITY&sslTrustStorePassword=secret-canary",
        "jdbc:mysql://fe:9030/?sslMode=VERIFY_IDENTITY&enabledTLSProtocols=TLSv1.2",
        "jdbc:mysql://fe:9030/?sslMode=VERIFY_IDENTITY&propertiesTransform=example.Transform",
        "jdbc:mysql://fe:9030/?sslMode=VERIFY_IDENTITY&socketFactory=example.SocketFactory",
        "jdbc:mysql://fe:9030/?sslMode=VERIFY_IDENTITY&protocol=PIPE",
        "jdbc:mysql://fe:9030/?sslMode=VERIFY_IDENTITY&useConfigs=serverPerformance",
        "jdbc:mysql://fe:9030/?sslMode=VERIFY_IDENTITY&parseInfoCacheFactory=example.CacheFactory",
        "jdbc:mysql://fe:9030/?sslMode=VERIFY_IDENTITY&haLoadBalanceStrategy=example.Strategy",
        "jdbc:mysql://fe:9030/?sslMode=VERIFY_IDENTITY&password1=secret-canary",
        "jdbc:mysql://fe:9030/?sslMode=VERIFY_IDENTITY&socksProxyHost=proxy.example",
        "jdbc:mysql://fe:9030/?sslMode=VERIFY_IDENTITY&createDatabaseIfNotExist=true"
      })
  void rejectsWeakConflictingOrPersistentStrictTlsParameters(String url) {
    Map<String, String> properties = strictServerProperties();
    properties.put("jdbc-url", url);

    assertThatThrownBy(() -> DorisJdbcSecurity.validateServerCatalogProperties(properties))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageNotContaining("secret-canary")
        .hasMessageNotContaining(url);
  }

  @Test
  void doesNotEchoSecretMaterialEmbeddedInTlsPropertyNames() {
    Map<String, String> urlProperties = strictServerProperties();
    urlProperties.put(
        "jdbc-url", urlProperties.get("jdbc-url") + "&sslTrustStore-secret-canary=value");
    assertThatThrownBy(() -> DorisJdbcSecurity.validateServerCatalogProperties(urlProperties))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageNotContaining("secret-canary");

    Map<String, String> bypassProperties = strictServerProperties();
    bypassProperties.put("gravitino.bypass.trustStore-secret-canary", "value");
    assertThatThrownBy(() -> DorisJdbcSecurity.validateServerCatalogProperties(bypassProperties))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageNotContaining("secret-canary");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "doris-fenodes",
        "doris-query-port",
        "gravitino.bypass.sslMode",
        "gravitino.bypass.connectionProperties",
        "spark.bypass.doris.fenodes"
      })
  void rejectsNativeOrTlsBypassConfigurationInStrictTransport(String property) {
    Map<String, String> properties = strictServerProperties();
    String value =
        "gravitino.bypass.connectionProperties".equals(property)
            ? "fallbackToSystemTrustStore=false"
            : "secret-canary";
    properties.put(property, value);

    assertThatThrownBy(() -> DorisJdbcSecurity.validateServerCatalogProperties(properties))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageNotContaining("secret-canary");
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "HYBRID", " hybrid", "hybrid ", "strict", "STRICT-JDBC-TLS"})
  void rejectsUnknownOrNonCanonicalTransportValues(String transport) {
    Map<String, String> properties = validServerProperties();
    properties.put(DorisJdbcSecurity.READ_TRANSPORT, transport);

    assertThatThrownBy(() -> DorisJdbcSecurity.validateServerCatalogProperties(properties))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(DorisJdbcSecurity.READ_TRANSPORT);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "DORIS-READ-TRANSPORT",
        "doris%2Dread%2Dtransport",
        "DORIS-FENODES",
        "doris%2Dquery%2Dport"
      })
  void rejectsNonCanonicalProfilePropertyNames(String property) {
    Map<String, String> properties = validServerProperties();
    String canonical = property.toLowerCase(Locale.ROOT).replace("%2d", "-");
    String value = properties.remove(canonical);
    properties.put(property, value);

    assertThatThrownBy(() -> DorisJdbcSecurity.validateServerCatalogProperties(properties))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Governed Doris profile properties must use their canonical names once")
        .hasMessageNotContaining(canonical);
  }

  @Test
  void decodesUnsafeNamesRecursivelyAndRejectsExcessiveEncoding() {
    assertThatThrownBy(
            () ->
                DorisJdbcSecurity.validateConnection(
                    "jdbc:mysql://fe:9030/?auto%2544eserialize=true", DRIVER))
        .hasMessage("Unreviewed Doris JDBC URL parameter is not allowed")
        .hasMessageNotContaining("autoDeserialize");
    assertThatThrownBy(
            () ->
                DorisJdbcSecurity.validateConnection(
                    "jdbc:mysql://fe:9030/?auto%252525252544eserialize=true", DRIVER))
        .hasMessage("Doris JDBC URL encoding is invalid");
  }

  @Test
  void decodesStrictTlsValuesBeforeExactMatching() {
    Map<String, String> encodedIdentity = strictServerProperties();
    encodedIdentity.put("jdbc-url", "jdbc:mysql://fe:9030/?sslMode=VERIFY%5FIDENTITY");
    assertThatCode(() -> DorisJdbcSecurity.validateServerCatalogProperties(encodedIdentity))
        .doesNotThrowAnyException();

    Map<String, String> encodedWeakFallback = strictServerProperties();
    encodedWeakFallback.put(
        "jdbc-url",
        "jdbc:mysql://fe:9030/?sslMode=VERIFY_IDENTITY&fallbackToSystemTrustStore=%66alse");
    assertThatThrownBy(() -> DorisJdbcSecurity.validateServerCatalogProperties(encodedWeakFallback))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("fallbackToSystemTrustStore");
  }

  @Test
  void usesLocaleRootForSecurityMatching() {
    Locale original = Locale.getDefault();
    try {
      Locale.setDefault(Locale.forLanguageTag("tr-TR"));
      Map<String, String> properties = validServerProperties();
      properties.put("gravitino.bypass.INITIALSIZE", "1");
      assertThatThrownBy(() -> DorisJdbcSecurity.validateServerCatalogProperties(properties))
          .hasMessage("Unreviewed JDBC connection pool property is not allowed")
          .hasMessageNotContaining("initialSize");
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
        "jdbc:mysql://fe:9030/?password1=secret",
        "jdbc:mysql://fe:9030/?password2=secret",
        "jdbc:mysql://fe:9030/?password3=secret",
        "jdbc:mysql://fe:9030/?PASSWORD1=secret",
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
        "bad\\" + "u12G4=secret-canary",
        "socketTimeout=1;propertiesTransform=example.Transform",
        "socketTimeout=1;socketFactory=example.SocketFactory",
        "socketTimeout=1;logger=example.Logger",
        "socketTimeout=1;queryInfoCacheFactory=example.CacheFactory",
        "socketTimeout=1;parseInfoCacheFactory=example.CacheFactory",
        "socketTimeout=1;protocol=PIPE",
        "socketTimeout=1;useConfigs=serverPerformance",
        "socketTimeout=1;ha.loadBalanceStrategy=example.Strategy",
        "socketTimeout=1;haLoadBalanceStrategy=example.Strategy",
        "socketTimeout=1;password1=secret-canary",
        "socketTimeout=1;password2=secret-canary",
        "socketTimeout=1;password3=secret-canary",
        "socketTimeout=1;clientCertificateKeyStorePassword=secret-canary",
        "socketTimeout=1;socksProxyHost=proxy.example",
        "socketTimeout=1;createDatabaseIfNotExist=true",
        "socketTimeout=1;sessionVariables=time_zone='+00:00'");
  }

  @Test
  void validatesManagedArrowAndWriteCombinations() {
    Map<String, String> arrow = validServerProperties();
    arrow.put(DorisJdbcSecurity.ARROW_FLIGHT_SQL_MODE, DorisJdbcSecurity.ARROW_PREFERRED);
    arrow.put(DorisJdbcSecurity.ARROW_FLIGHT_SQL_PORT, "8070");
    arrow.put(DorisJdbcSecurity.WRITE_MODE, DorisJdbcSecurity.WRITE_BATCH);
    arrow.put(DorisJdbcSecurity.WRITE_OVERWRITE_MODE, DorisJdbcSecurity.WRITE_OVERWRITE_TRUNCATE);
    assertThatCode(() -> DorisJdbcSecurity.validateServerCatalogProperties(arrow))
        .doesNotThrowAnyException();

    Map<String, String> missingPort = validServerProperties();
    missingPort.put(DorisJdbcSecurity.ARROW_FLIGHT_SQL_MODE, DorisJdbcSecurity.ARROW_PREFERRED);
    assertThatThrownBy(() -> DorisJdbcSecurity.validateServerCatalogProperties(missingPort))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("1 and 65535");

    Map<String, String> disabledWithPort = validServerProperties();
    disabledWithPort.put(DorisJdbcSecurity.ARROW_FLIGHT_SQL_PORT, "8070");
    assertThatThrownBy(() -> DorisJdbcSecurity.validateServerCatalogProperties(disabledWithPort))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("requires preferred");

    Map<String, String> disabledTruncate = validServerProperties();
    disabledTruncate.put(
        DorisJdbcSecurity.WRITE_OVERWRITE_MODE, DorisJdbcSecurity.WRITE_OVERWRITE_TRUNCATE);
    assertThatThrownBy(() -> DorisJdbcSecurity.validateServerCatalogProperties(disabledTruncate))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("requires doris-write-mode=batch");
  }

  @Test
  void strictJdbcTlsRejectsArrowAndWritesBeforePhysicalInitialization() {
    Map<String, String> arrow = strictServerProperties();
    arrow.put(DorisJdbcSecurity.ARROW_FLIGHT_SQL_MODE, DorisJdbcSecurity.ARROW_PREFERRED);
    arrow.put(DorisJdbcSecurity.ARROW_FLIGHT_SQL_PORT, "8070");
    assertThatThrownBy(() -> DorisJdbcSecurity.validateServerCatalogProperties(arrow))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("does not support Arrow");

    Map<String, String> write = strictServerProperties();
    write.put(DorisJdbcSecurity.WRITE_MODE, DorisJdbcSecurity.WRITE_BATCH);
    assertThatThrownBy(() -> DorisJdbcSecurity.validateServerCatalogProperties(write))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("does not support governed Doris writes");
  }

  @Test
  void rejectsRawAndSparkBypassConnectorControlsWithoutEchoingValues() {
    for (String property :
        new String[] {
          "doris.read.mode",
          "doris.read.arrow-flight-sql.port",
          "doris.fe.auto.fetch",
          "doris.sink.mode",
          "doris.sink.enable-2pc",
          "doris.sink.properties.strict_mode",
          "doris.sink.properties.format"
        }) {
      Map<String, String> direct = validServerProperties();
      direct.put(property, "secret-canary");
      assertThatThrownBy(() -> DorisJdbcSecurity.validateServerCatalogProperties(direct))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("Raw Doris connector options are not allowed")
          .hasMessageNotContaining("secret-canary");

      Map<String, String> bypass = validServerProperties();
      bypass.put("spark.bypass." + property, "secret-canary");
      assertThatThrownBy(() -> DorisJdbcSecurity.validateServerCatalogProperties(bypass))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage(
              property.equals("doris.sink.properties.format")
                  ? "Unreviewed Doris Spark bypass option is not allowed"
                  : "Protected JDBC properties must not use spark.bypass")
          .hasMessageNotContaining("secret-canary");
    }
  }

  private static Map<String, String> validServerProperties() {
    Map<String, String> properties = new HashMap<>();
    properties.put("jdbc-url", "jdbc:mysql://fe:9030/");
    properties.put("jdbc-driver", DRIVER);
    properties.put("jdbc-user", "reader");
    properties.put("jdbc-password", "secret-canary");
    properties.put("doris-fenodes", "fe:8030");
    properties.put("doris-query-port", "9030");
    return properties;
  }

  private static Map<String, String> strictServerProperties() {
    Map<String, String> properties = new HashMap<>();
    properties.put("jdbc-url", "jdbc:mysql://fe:9030/?sslMode=VERIFY_IDENTITY");
    properties.put("jdbc-driver", DRIVER);
    properties.put("jdbc-user", "reader");
    properties.put("jdbc-password", "secret-canary");
    properties.put(DorisJdbcSecurity.READ_TRANSPORT, DorisJdbcSecurity.STRICT_JDBC_TLS_TRANSPORT);
    return properties;
  }
}
