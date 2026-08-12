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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTable;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;

public class TestJdbcOnlyTableCatalog35 {

  @Test
  void testBuildsPhysicalSchemaFromDatabaseMetadata() throws Exception {
    ResultSet columns = mock(ResultSet.class);
    when(columns.next()).thenReturn(true, true, true, false);
    when(columns.getString("TABLE_NAME")).thenReturn("events", "events", "events_backup");
    when(columns.getString("TYPE_NAME")).thenReturn("INT", "DECIMAL");
    when(columns.getString("COLUMN_NAME")).thenReturn("id", "amount");
    when(columns.getInt("DATA_TYPE")).thenReturn(Types.INTEGER, Types.DECIMAL);
    when(columns.getInt("COLUMN_SIZE")).thenReturn(10, 18);
    when(columns.getInt("DECIMAL_DIGITS")).thenReturn(0, 3);
    when(columns.getInt("NULLABLE"))
        .thenReturn(DatabaseMetaData.columnNoNulls, DatabaseMetaData.columnNullable);

    DorisPhysicalSchema physicalSchema =
        JdbcOnlyTableCatalog35.physicalSchema(
            Identifier.of(new String[] {"analytics"}, "events"), columns);

    assertThat(physicalSchema.schema().fieldNames()).containsExactly("id", "amount");
    assertThat(physicalSchema.schema().apply("id").dataType()).isEqualTo(DataTypes.IntegerType);
    assertThat(physicalSchema.schema().apply("id").nullable()).isFalse();
    assertThat(physicalSchema.schema().apply("amount").dataType())
        .isEqualTo(DataTypes.createDecimalType(18, 3));
    assertThat(physicalSchema.schema().apply("amount").nullable()).isTrue();
    assertThat(physicalSchema.dorisTypeName(0)).isEqualTo("INT");
    assertThat(physicalSchema.dorisTypeName(1)).isEqualTo("DECIMAL(18,3)");
  }

  @Test
  void testEscapesMetadataPatternCharacters() {
    assertThat(JdbcOnlyTableCatalog35.escapeMetadataPattern("\\", "events_100%\\archive"))
        .isEqualTo("events\\_100\\%\\\\archive");
    assertThat(JdbcOnlyTableCatalog35.escapeMetadataPattern("", "events_100%"))
        .isEqualTo("events_100%");
  }

  @Test
  void testRejectsMetadataWithoutTargetColumns() throws Exception {
    ResultSet columns = mock(ResultSet.class);
    when(columns.next()).thenReturn(true, false);
    when(columns.getString("TABLE_NAME")).thenReturn("events_backup");

    assertThatThrownBy(
            () ->
                JdbcOnlyTableCatalog35.physicalSchema(
                    Identifier.of(new String[] {"analytics"}, "events"), columns))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("returned no columns")
        .hasMessageContaining("analytics.events");
  }

  @Test
  void testOnlyJdbcBitOneMapsToBoolean() {
    assertThat(JdbcOnlyTableCatalog35.toCatalystJdbcTypeOrString("BIT", Types.BIT, 1, 0))
        .isEqualTo(DataTypes.BooleanType);
    assertThat(JdbcOnlyTableCatalog35.toCatalystJdbcTypeOrString("BIT", Types.BIT, 8, 0))
        .isEqualTo(DataTypes.StringType);
    assertThat(JdbcOnlyTableCatalog35.toCatalystJdbcTypeOrString("BIT", Types.BINARY, 1, 0))
        .isEqualTo(DataTypes.StringType);
    assertThat(JdbcOnlyTableCatalog35.toCatalystJdbcTypeOrString("BINARY", Types.BIT, 1, 0))
        .isEqualTo(DataTypes.BinaryType);
  }

  @Test
  @SuppressWarnings("deprecation")
  void testSchemaSeededTableUsesOnlySparkJdbcV2() {
    JdbcOnlyTableCatalog35 catalog = new JdbcOnlyTableCatalog35();
    catalog.initialize(
        "doris",
        new CaseInsensitiveStringMap(
            Map.of(
                "url",
                "jdbc:mysql://doris-fe:9030/?sslMode=VERIFY_IDENTITY",
                "driver",
                "com.mysql.cj.jdbc.Driver",
                "user",
                "reader",
                "password",
                "secret-canary")));
    StructType schema =
        DataTypes.createStructType(
            new org.apache.spark.sql.types.StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, true)
            });
    DorisReadSchema readSchema =
        new DorisReadSchema(schema, List.of(DorisReadSchema.quoteIdentifier("id")), false);
    DorisJdbcConnectionInfo connectionInfo =
        new DorisJdbcConnectionInfo(
            "jdbc:mysql://doris-fe:9030/?sslMode=VERIFY_IDENTITY",
            "com.mysql.cj.jdbc.Driver",
            "reader",
            "secret-canary",
            DorisReadTransport.STRICT_JDBC_TLS);

    Table table =
        catalog.loadTable(
            Identifier.of(new String[] {"analytics"}, "events"),
            readSchema,
            connectionInfo,
            DorisJdbcReadOptions.from(Map.of()));

    assertThat(table).isExactlyInstanceOf(JDBCTable.class).isInstanceOf(SupportsRead.class);
    assertThat(table.schema()).isSameAs(schema);
    assertThat(table.toString()).doesNotContain("secret-canary");
  }
}
