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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import org.apache.doris.spark.config.DorisConfig;
import org.apache.doris.spark.rest.models.Field;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;
import scala.Option;

public class TestSchemaSeededDorisTableCatalog35 {

  @Test
  @SuppressWarnings("deprecation")
  void testSeededTableReturnsSchemaWithoutPhysicalMetadataRequest() {
    SchemaSeededDorisTableCatalog35 catalog = initializedCatalog();
    Identifier identifier = Identifier.of(new String[] {"analytics"}, "events");
    StructType schema =
        DataTypes.createStructType(
            new org.apache.spark.sql.types.StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, true)
            });

    Table table =
        catalog.loadTable(identifier, readSchema(schema), connectionInfo(), readOptions());

    assertInstanceOf(DorisHybridTable35.class, table);
    assertSame(schema, assertDoesNotThrow(table::schema));
  }

  @Test
  void testRequiresExactlyOneSchema() {
    SchemaSeededDorisTableCatalog35 catalog = initializedCatalog();
    StructType schema = new StructType();

    assertThrows(
        IllegalArgumentException.class,
        () ->
            catalog.loadTable(
                Identifier.of(new String[0], "events"),
                readSchema(schema),
                connectionInfo(),
                readOptions()));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            catalog.loadTable(
                Identifier.of(new String[] {"parent", "analytics"}, "events"),
                readSchema(schema),
                connectionInfo(),
                readOptions()));
  }

  @Test
  void testNativeTableConstructionFailureIsSanitized() {
    SchemaSeededDorisTableCatalog35 catalog = new FailingSchemaSeededDorisTableCatalog35();
    initialize(catalog);

    IllegalArgumentException failure =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                catalog.loadTable(
                    Identifier.of(new String[] {"analytics"}, "events"),
                    readSchema(new StructType()),
                    connectionInfo(),
                    readOptions()));

    assertTrue(failure.getMessage().contains("analytics.events"));
    assertFalse(failure.getMessage().contains("native-secret"));
  }

  @Test
  void testUnknownPhysicalTypeUsesStringFallback() {
    assertSame(
        DataTypes.StringType,
        SchemaSeededDorisTableCatalog35.toCatalystTypeOrString("DECIMAL256", 76, 6));
    assertSame(
        DataTypes.IntegerType, SchemaSeededDorisTableCatalog35.toCatalystTypeOrString("INT", 0, 0));
    assertThrows(
        NullPointerException.class,
        () -> SchemaSeededDorisTableCatalog35.toCatalystTypeOrString(null, 0, 0));
  }

  @Test
  void testKnownPhysicalTypeConversionFailureDoesNotUseStringFallback() {
    assertThrows(
        ArithmeticException.class,
        () -> SchemaSeededDorisTableCatalog35.toCatalystTypeOrString("DECIMAL", 39, 0));
    IllegalArgumentException failure =
        assertThrows(
            IllegalArgumentException.class,
            () -> SchemaSeededDorisTableCatalog35.toCatalystTypeOrString("DECIMAL", 3, 4));
    assertEquals("Failed to convert Doris FE type", failure.getMessage());
  }

  @Test
  void testRetainsFeTypeParametersNeededForCompatibilityChecks() {
    assertEquals(
        "DATETIMEV2",
        SchemaSeededDorisTableCatalog35.typeNameWithParameters(
            new Field("event_time", "DATETIMEV2", "", 6, 0, "")));
    assertEquals(
        "DECIMAL256(76,6)",
        SchemaSeededDorisTableCatalog35.typeNameWithParameters(
            new Field("amount", "DECIMAL256", "", 76, 6, "")));
    assertEquals(
        "DATETIME(3)",
        SchemaSeededDorisTableCatalog35.typeNameWithParameters(
            new Field("event_time", "DATETIME(3)", "", 0, 0, "")));
  }

  private static SchemaSeededDorisTableCatalog35 initializedCatalog() {
    SchemaSeededDorisTableCatalog35 catalog = new SchemaSeededDorisTableCatalog35();
    initialize(catalog);
    return catalog;
  }

  private static void initialize(SchemaSeededDorisTableCatalog35 catalog) {
    catalog.initialize(
        "doris",
        new CaseInsensitiveStringMap(
            ImmutableMap.of(
                DorisConnectorConstants.DORIS_FE_NODES,
                "localhost:8030",
                DorisConnectorConstants.DORIS_QUERY_PORT,
                "9030",
                DorisConnectorConstants.DORIS_USER,
                "root",
                DorisConnectorConstants.DORIS_PASSWORD,
                "non-empty-test-password")));
  }

  private static DorisReadSchema readSchema(StructType schema) {
    java.util.List<String> projections = new java.util.ArrayList<>();
    for (org.apache.spark.sql.types.StructField field : schema.fields()) {
      projections.add(DorisReadSchema.quoteIdentifier(field.name()));
    }
    return new DorisReadSchema(schema, projections, false);
  }

  private static DorisJdbcConnectionInfo connectionInfo() {
    return new DorisJdbcConnectionInfo(
        "jdbc:mysql://localhost:9030/",
        "com.mysql.cj.jdbc.Driver",
        "root",
        "non-empty-test-password");
  }

  private static DorisJdbcReadOptions readOptions() {
    return DorisJdbcReadOptions.from(ImmutableMap.of());
  }

  @SuppressWarnings("overrides")
  private static class FailingSchemaSeededDorisTableCatalog35
      extends SchemaSeededDorisTableCatalog35 {

    @Override
    public Table newTableInstance(
        Identifier identifier, DorisConfig config, Option<StructType> schema) {
      throw new IllegalArgumentException("password=native-secret");
    }
  }
}
