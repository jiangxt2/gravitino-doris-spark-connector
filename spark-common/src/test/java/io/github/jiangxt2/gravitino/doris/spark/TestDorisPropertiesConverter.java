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

package io.github.jiangxt2.gravitino.doris.spark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;

public class TestDorisPropertiesConverter {

  private final DorisPropertiesConverter converter = DorisPropertiesConverter.getInstance();

  @Test
  void testCatalogEndpointsAndOptionPrecedence() {
    Map<String, String> catalogProperties =
        ImmutableMap.of(
            DorisConnectorConstants.GRAVITINO_DORIS_FE_NODES,
            "fe-1:8030,fe-2:8030",
            DorisConnectorConstants.GRAVITINO_DORIS_QUERY_PORT,
            "9030",
            PropertiesConverter.SPARK_PROPERTY_PREFIX + "doris.request.retries",
            "1");
    CaseInsensitiveStringMap options =
        new CaseInsensitiveStringMap(ImmutableMap.of("DORIS.REQUEST.RETRIES", "2"));

    Map<String, String> result = converter.toSparkCatalogProperties(options, catalogProperties);

    assertEquals("fe-1:8030,fe-2:8030", result.get("doris.fenodes"));
    assertEquals("9030", result.get("doris.query.port"));
    assertEquals("2", result.get("doris.request.retries"));
    assertEquals(3, result.size());
  }

  @Test
  void testCatalogMappingIsWhitelistOnly() {
    Map<String, String> result =
        converter.toSparkCatalogProperties(
            ImmutableMap.of(
                DorisConnectorConstants.GRAVITINO_DORIS_FE_NODES,
                "fe:8030",
                DorisConnectorConstants.JDBC_URL,
                "jdbc:mysql://fe:9030/",
                DorisConnectorConstants.JDBC_DRIVER,
                "com.mysql.cj.jdbc.Driver",
                "unrelated-property",
                "value"));

    assertEquals(ImmutableMap.of("doris.fenodes", "fe:8030"), result);
  }

  @Test
  void testRequiresFeNodesForAdapterInitialization() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                converter.toSparkCatalogProperties(
                    new CaseInsensitiveStringMap(ImmutableMap.of()), ImmutableMap.of()));

    assertTrue(exception.getMessage().contains("doris-fenodes"));

    IllegalArgumentException missingQueryPort =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                converter.toSparkCatalogProperties(
                    new CaseInsensitiveStringMap(ImmutableMap.of()),
                    ImmutableMap.of(DorisConnectorConstants.GRAVITINO_DORIS_FE_NODES, "fe:8030")));
    assertTrue(missingQueryPort.getMessage().contains("doris-query-port"));
  }

  @Test
  void testStrictProfileRejectsNativeEndpointsAndOptions() {
    Map<String, String> strict =
        ImmutableMap.of(
            DorisConnectorConstants.READ_TRANSPORT,
            DorisConnectorConstants.STRICT_JDBC_TLS_TRANSPORT);

    assertEquals(
        ImmutableMap.of(),
        converter.toSparkCatalogProperties(
            new CaseInsensitiveStringMap(ImmutableMap.of()), strict));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            converter.toSparkCatalogProperties(
                new CaseInsensitiveStringMap(ImmutableMap.of()),
                ImmutableMap.of(
                    DorisConnectorConstants.READ_TRANSPORT,
                    DorisConnectorConstants.STRICT_JDBC_TLS_TRANSPORT,
                    DorisConnectorConstants.GRAVITINO_DORIS_FE_NODES,
                    "fe:8030")));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            converter.toSparkCatalogProperties(
                new CaseInsensitiveStringMap(
                    ImmutableMap.of("doris.request.connect.timeout.ms", "1000")),
                strict));
  }

  @Test
  void testTransportProfileCannotBeOverriddenBySparkOrBypassOptions() {
    Map<String, String> strict =
        ImmutableMap.of(
            DorisConnectorConstants.READ_TRANSPORT,
            DorisConnectorConstants.STRICT_JDBC_TLS_TRANSPORT);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            converter.toSparkCatalogProperties(
                new CaseInsensitiveStringMap(
                    ImmutableMap.of(DorisConnectorConstants.READ_TRANSPORT, "hybrid")),
                strict));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            converter.toSparkCatalogProperties(
                new CaseInsensitiveStringMap(ImmutableMap.of()),
                ImmutableMap.of(
                    DorisConnectorConstants.READ_TRANSPORT,
                    DorisConnectorConstants.STRICT_JDBC_TLS_TRANSPORT,
                    PropertiesConverter.SPARK_PROPERTY_PREFIX
                        + DorisConnectorConstants.READ_TRANSPORT,
                    "hybrid")));
  }

  @Test
  void testValidatesFeNodesAndPorts() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            converter.toSparkCatalogProperties(
                ImmutableMap.of(DorisConnectorConstants.GRAVITINO_DORIS_FE_NODES, "missing-port")));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            converter.toSparkCatalogProperties(
                ImmutableMap.of(
                    DorisConnectorConstants.GRAVITINO_DORIS_FE_NODES, "invalid/path:8030")));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            converter.toSparkCatalogProperties(
                ImmutableMap.of(DorisConnectorConstants.GRAVITINO_DORIS_FE_NODES, "fe:0")));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            converter.toSparkCatalogProperties(
                ImmutableMap.of(DorisConnectorConstants.GRAVITINO_DORIS_FE_NODES, "fe:65536")));
    assertEquals(
        "fe:8030,fe2:8031",
        converter
            .toSparkCatalogProperties(
                ImmutableMap.of(
                    DorisConnectorConstants.GRAVITINO_DORIS_FE_NODES, " fe:8030, fe2:8031 "))
            .get(DorisConnectorConstants.DORIS_FE_NODES));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            converter.toSparkCatalogProperties(
                ImmutableMap.of(
                    DorisConnectorConstants.GRAVITINO_DORIS_FE_NODES,
                    "fe:8030",
                    DorisConnectorConstants.GRAVITINO_DORIS_QUERY_PORT,
                    "invalid")));
  }

  @Test
  void testRejectsProtectedOptionsWithoutExposingValues() {
    IllegalArgumentException optionFailure =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                converter.toSparkCatalogProperties(
                    new CaseInsensitiveStringMap(ImmutableMap.of("DORIS.USER", "option-secret")),
                    ImmutableMap.of(DorisConnectorConstants.GRAVITINO_DORIS_FE_NODES, "fe:8030")));
    assertTrue(optionFailure.getMessage().contains("protected Doris connector options"));
    assertFalse(optionFailure.getMessage().contains("option-secret"));

    IllegalArgumentException bypassFailure =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                converter.toSparkCatalogProperties(
                    new CaseInsensitiveStringMap(ImmutableMap.of()),
                    ImmutableMap.of(
                        DorisConnectorConstants.GRAVITINO_DORIS_FE_NODES,
                        "fe:8030",
                        PropertiesConverter.SPARK_PROPERTY_PREFIX + "doris.password",
                        "bypass-secret")));
    assertTrue(bypassFailure.getMessage().contains("protected Doris connector options"));
    assertFalse(bypassFailure.getMessage().contains("bypass-secret"));
  }

  @Test
  void testRejectsJdbcIdentityAndCredentialOptionsAcrossSources() {
    for (String property :
        new String[] {
          DorisConnectorConstants.JDBC_URL,
          DorisConnectorConstants.JDBC_DRIVER,
          DorisConnectorConstants.JDBC_USER,
          DorisConnectorConstants.JDBC_PASSWORD
        }) {
      String secret = "option-secret-" + property;
      IllegalArgumentException optionFailure =
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  converter.toSparkCatalogProperties(
                      new CaseInsensitiveStringMap(ImmutableMap.of(property, secret)),
                      validEndpoints()));
      assertTrue(optionFailure.getMessage().contains("protected Doris connector options"));
      assertFalse(optionFailure.getMessage().contains(property));
      assertFalse(optionFailure.getMessage().contains(secret));

      IllegalArgumentException bypassFailure =
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  converter.toSparkCatalogProperties(
                      new CaseInsensitiveStringMap(ImmutableMap.of()),
                      ImmutableMap.<String, String>builder()
                          .putAll(validEndpoints())
                          .put(PropertiesConverter.SPARK_PROPERTY_PREFIX + property, secret)
                          .build()));
      assertTrue(bypassFailure.getMessage().contains("protected Doris connector options"));
      assertFalse(bypassFailure.getMessage().contains(property));
      assertFalse(bypassFailure.getMessage().contains(secret));
    }
  }

  @Test
  void testRejectsCredentialBackfillWithoutExposingValues() {
    IllegalArgumentException failure =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                converter.toSparkCatalogProperties(
                    ImmutableMap.of(
                        DorisConnectorConstants.GRAVITINO_DORIS_FE_NODES,
                        "fe:8030",
                        DorisConnectorConstants.JDBC_USER,
                        "backfilled-secret")));

    assertTrue(failure.getMessage().contains("jdbc-user"));
    assertFalse(failure.getMessage().contains("backfilled-secret"));
  }

  @Test
  void testRejectsAliasesUnsupportedBlankAndDuplicateOptions() {
    assertRejectedOption("fenodes", "value");
    assertRejectedOption("doris_request_retries", "1");
    assertRejectedOption("doris.unknown.option", "1");
    assertRejectedOption("doris.request.retries", " ");

    Map<String, String> duplicateBypass = new LinkedHashMap<>();
    duplicateBypass.put(PropertiesConverter.SPARK_PROPERTY_PREFIX + "doris.request.retries", "1");
    duplicateBypass.put(PropertiesConverter.SPARK_PROPERTY_PREFIX + "DORIS.REQUEST.RETRIES", "2");
    duplicateBypass.put(DorisConnectorConstants.GRAVITINO_DORIS_FE_NODES, "fe:8030");

    assertThrows(
        IllegalArgumentException.class,
        () ->
            converter.toSparkCatalogProperties(
                new CaseInsensitiveStringMap(ImmutableMap.of()), duplicateBypass));
  }

  @Test
  void testRejectedOptionsDoNotDiscloseUserControlledNames() {
    for (String key :
        new String[] {
          "secret-canary",
          "doris.secret-canary",
          "DORIS.SECRET-CANARY",
          "doris_request_secret-canary"
        }) {
      IllegalArgumentException failure =
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  converter.toSparkCatalogProperties(
                      new CaseInsensitiveStringMap(ImmutableMap.of(key, "value-secret-canary")),
                      validEndpoints()));
      assertFalse(failure.getMessage().contains("secret-canary"));
    }
  }

  @Test
  void testTablePropertiesRemoveConnectionAndCredentialKeys() {
    Map<String, String> result =
        converter.toSparkTableProperties(
            ImmutableMap.<String, String>builder()
                .put("replication_num", "1")
                .put(DorisConnectorConstants.JDBC_PASSWORD, "jdbc-secret")
                .put("doris.password", "connector-secret")
                .put("password", "generic-secret")
                .put("dbtable", "credential-bearing-query")
                .put(PropertiesConverter.SPARK_PROPERTY_PREFIX + "doris.request.retries", "3")
                .build());

    assertEquals(ImmutableMap.of("replication_num", "1"), result);
  }

  private void assertRejectedOption(String key, String value) {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            converter.toSparkCatalogProperties(
                new CaseInsensitiveStringMap(ImmutableMap.of(key, value)),
                ImmutableMap.of(
                    DorisConnectorConstants.GRAVITINO_DORIS_FE_NODES,
                    "fe:8030",
                    DorisConnectorConstants.GRAVITINO_DORIS_QUERY_PORT,
                    "9030")));
  }

  private static Map<String, String> validEndpoints() {
    return ImmutableMap.of(
        DorisConnectorConstants.GRAVITINO_DORIS_FE_NODES,
        "fe:8030",
        DorisConnectorConstants.GRAVITINO_DORIS_QUERY_PORT,
        "9030");
  }
}
