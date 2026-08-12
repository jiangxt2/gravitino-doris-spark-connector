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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.spark.connector.ConnectorConstants;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

public class TestDorisSchemaCompatibility {

  private static final Identifier IDENTIFIER = Identifier.of(new String[] {"analytics"}, "events");
  private static final SparkTypeConverter TYPE_CONVERTER = new TestDorisTypeConverter();

  @Test
  void testPlansEvidenceBackedTypeFamilies() {
    Column[] logicalColumns =
        new Column[] {
          column("bool_col", Types.BooleanType.get(), null, true),
          column("tiny_col", Types.ByteType.get(), null, false),
          column("small_col", Types.ShortType.get(), null, true),
          column("int_col", Types.IntegerType.get(), null, true),
          column("big_col", Types.LongType.get(), null, true),
          column("float_col", Types.FloatType.get(), null, true),
          column("double_col", Types.DoubleType.get(), null, true),
          column("decimal_col", Types.DecimalType.of(18, 3), null, true),
          column("date_col", Types.DateType.get(), null, true),
          column("Char_Col", Types.FixedCharType.of(8), "fixed comment", false),
          column("varchar_col", Types.VarCharType.of(64), null, true),
          column("string_col", Types.StringType.get(), null, true)
        };
    StructType physicalSchema =
        schema(
            field("bool_col", DataTypes.BooleanType, true),
            field("tiny_col", DataTypes.ByteType, true),
            field("small_col", DataTypes.ShortType, true),
            field("int_col", DataTypes.IntegerType, true),
            field("big_col", DataTypes.LongType, true),
            field("float_col", DataTypes.FloatType, true),
            field("double_col", DataTypes.DoubleType, true),
            field("decimal_col", DataTypes.createDecimalType(18, 3), true),
            field("date_col", DataTypes.DateType, true),
            field("char_col", DataTypes.StringType, true),
            field("varchar_col", DataTypes.StringType, true),
            field("string_col", DataTypes.StringType, true));

    DorisReadSchema readSchema =
        DorisSchemaCompatibility.planReadSchema(
            IDENTIFIER, table(logicalColumns), physicalSchema, TYPE_CONVERTER);
    StructType result = readSchema.schema();

    assertEquals(physicalSchema.length(), result.length());
    assertFalse(readSchema.requiresSqlExecution());
    assertEquals("char_col", result.fields()[9].name());
    assertEquals(DataTypes.StringType, result.fields()[9].dataType());
    assertTrue(result.fields()[9].nullable());
    assertEquals(
        "fixed comment", result.fields()[9].metadata().getString(ConnectorConstants.COMMENT));
  }

  @Test
  void testAcceptsJdbcBitOnlyForBooleanExecutionType() {
    Column[] logicalColumns =
        new Column[] {column("bool_col", Types.BooleanType.get(), null, true)};
    DorisPhysicalSchema jdbcBitSchema =
        new DorisPhysicalSchema(
            schema(field("bool_col", DataTypes.BooleanType, true)), Arrays.asList("BIT"));

    DorisReadSchema result =
        DorisSchemaCompatibility.planReadSchema(
            IDENTIFIER, table(logicalColumns), jdbcBitSchema, TYPE_CONVERTER);

    assertFalse(result.requiresSqlExecution());
    assertEquals(DataTypes.BooleanType, result.schema().apply("bool_col").dataType());
    assertIncompatible(
        new Column[] {column("bool_col", Types.ByteType.get(), null, true)}, jdbcBitSchema);
    assertIncompatible(
        logicalColumns,
        new DorisPhysicalSchema(
            schema(field("bool_col", DataTypes.BinaryType, true)), Arrays.asList("BIT")));
  }

  @Test
  void testNormalizesDatetimeBinaryComplexAndExternalTypesToStrings() {
    Column[] logicalColumns =
        new Column[] {
          column("event_time", Types.TimestampType.withoutTimeZone(6), "naive timestamp", true),
          column("payload", Types.BinaryType.get(), null, true),
          column("json_col", Types.ExternalType.of("JSON"), null, true),
          column("bitmap_col", Types.ExternalType.of("BITMAP"), null, true),
          column("hll_col", Types.ExternalType.of("HLL"), null, true),
          column("unsigned_col", Types.IntegerType.unsigned(), null, true),
          column("array_col", Types.ListType.of(Types.IntegerType.get(), true), null, true)
        };
    StructType physicalSchema =
        schema(
            field("event_time", DataTypes.TimestampType, true),
            field("payload", DataTypes.BinaryType, true),
            field("json_col", DataTypes.StringType, true),
            field("bitmap_col", DataTypes.StringType, true),
            field("hll_col", DataTypes.StringType, true),
            field("unsigned_col", DataTypes.LongType, true),
            field("array_col", DataTypes.createArrayType(DataTypes.IntegerType), true));

    DorisReadSchema result =
        DorisSchemaCompatibility.planReadSchema(
            IDENTIFIER,
            table(logicalColumns),
            new DorisPhysicalSchema(
                physicalSchema,
                Arrays.asList(
                    "DATETIME(6)",
                    "BINARY",
                    "JSON",
                    "BITMAP",
                    "HLL",
                    "INT UNSIGNED",
                    "ARRAY<INT>")),
            TYPE_CONVERTER);

    assertTrue(result.requiresSqlExecution());
    assertEquals(
        ImmutableSet.of(
            "event_time",
            "payload",
            "json_col",
            "bitmap_col",
            "hll_col",
            "unsigned_col",
            "array_col"),
        result.normalizedColumns());
    Arrays.stream(result.schema().fields())
        .forEach(field -> assertEquals(DataTypes.StringType, field.dataType()));
    assertEquals(
        Arrays.asList(
            "`event_time` AS `event_time`",
            "TO_BASE64(`payload`) AS `payload`",
            "`json_col` AS `json_col`",
            "BITMAP_TO_BASE64(`bitmap_col`) AS `bitmap_col`",
            "HLL_TO_BASE64(`hll_col`) AS `hll_col`",
            "`unsigned_col` AS `unsigned_col`",
            "`array_col` AS `array_col`"),
        result.projections());
    assertEquals(
        "(SELECT `event_time` AS `event_time`, "
            + "TO_BASE64(`payload`) AS `payload`, "
            + "`json_col` AS `json_col`, "
            + "BITMAP_TO_BASE64(`bitmap_col`) AS `bitmap_col`, "
            + "HLL_TO_BASE64(`hll_col`) AS `hll_col`, "
            + "`unsigned_col` AS `unsigned_col`, "
            + "`array_col` AS `array_col` FROM `analytics`.`events`) "
            + "gravitino_doris_source",
        result.tableOrQuery(IDENTIFIER));
  }

  @Test
  void testRetainsExternalDecimalExecutionType() {
    DorisReadSchema result =
        DorisSchemaCompatibility.planReadSchema(
            IDENTIFIER,
            table(
                new Column[] {
                  column("legacy", Types.ExternalType.of("DECIMALV2(18,3)"), null, true)
                }),
            schema(field("legacy", DataTypes.createDecimalType(18, 3), true)),
            TYPE_CONVERTER);

    assertFalse(result.requiresSqlExecution());
    assertEquals(DataTypes.createDecimalType(18, 3), result.schema().fields()[0].dataType());
    assertEquals(Arrays.asList("`legacy`"), result.projections());
  }

  @Test
  void testRejectsExternalDecimalPrecisionOrScaleDrift() {
    Column[] logicalColumns =
        new Column[] {column("legacy", Types.ExternalType.of("DECIMAL(18,3)"), null, true)};

    IllegalArgumentException failure =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                DorisSchemaCompatibility.planReadSchema(
                    IDENTIFIER,
                    table(logicalColumns),
                    schema(field("legacy", DataTypes.createDecimalType(18, 2), true)),
                    TYPE_CONVERTER));

    assertTrue(failure.getMessage().contains("precision or scale differs"));
    assertTrue(failure.getMessage().contains("analytics.events"));
  }

  @Test
  void testNormalizesDecimal256ToString() {
    DorisReadSchema result =
        DorisSchemaCompatibility.planReadSchema(
            IDENTIFIER,
            table(
                new Column[] {column("wide", Types.ExternalType.of("DECIMAL(76,6)"), null, true)}),
            new DorisPhysicalSchema(
                schema(field("wide", DataTypes.StringType, true)), Arrays.asList("DECIMAL256")),
            TYPE_CONVERTER);

    assertTrue(result.requiresSqlExecution());
    assertEquals(DataTypes.StringType, result.schema().apply("wide").dataType());
    assertEquals(Arrays.asList("`wide` AS `wide`"), result.projections());
    assertEquals(ImmutableSet.of("wide"), result.normalizedColumns());
  }

  @Test
  void testNormalizesLegacyDecimalFamilyAboveCatalystPrecisionLimit() {
    DorisReadSchema result =
        DorisSchemaCompatibility.planReadSchema(
            IDENTIFIER,
            table(
                new Column[] {
                  column("wide", Types.ExternalType.of("DECIMALV2(76,6)"), null, true)
                }),
            new DorisPhysicalSchema(
                schema(field("wide", DataTypes.StringType, true)), Arrays.asList("DECIMALV2")),
            TYPE_CONVERTER);

    assertTrue(result.requiresSqlExecution());
    assertEquals(DataTypes.StringType, result.schema().apply("wide").dataType());
    assertEquals(Arrays.asList("`wide` AS `wide`"), result.projections());
  }

  @Test
  void testRejectsWideDecimalPrecisionOrScaleDrift() {
    Column logical = column("wide", Types.ExternalType.of("DECIMAL256(76,12)"), null, true);

    assertIncompatible(
        new Column[] {logical},
        new DorisPhysicalSchema(
            schema(field("wide", DataTypes.StringType, true)), Arrays.asList("DECIMAL256(76,11)")));
    assertIncompatible(
        new Column[] {logical},
        new DorisPhysicalSchema(
            schema(field("wide", DataTypes.StringType, true)), Arrays.asList("DECIMAL256(75,12)")));
  }

  @Test
  void testUsesFeTypeNamesWhenJdbcMetadataLosesNormalizedTypeInformation() {
    Column[] logicalColumns =
        new Column[] {
          column("array_col", Types.IntegerType.get(), null, true),
          column("map_col", Types.StringType.get(), null, true),
          column("struct_col", Types.StringType.get(), null, true),
          column("large_col", Types.IntegerType.get(), null, true),
          column("bitmap_col", Types.ExternalType.of("BIT"), null, true),
          column("hll_col", Types.StringType.get(), null, true)
        };
    StructType physicalSchema =
        schema(
            field("array_col", DataTypes.StringType, true),
            field("map_col", DataTypes.StringType, true),
            field("struct_col", DataTypes.StringType, true),
            field("large_col", DataTypes.StringType, true),
            field("bitmap_col", DataTypes.StringType, true),
            field("hll_col", DataTypes.StringType, true));

    DorisReadSchema result =
        DorisSchemaCompatibility.planReadSchema(
            IDENTIFIER,
            table(logicalColumns),
            new DorisPhysicalSchema(
                physicalSchema,
                Arrays.asList("ARRAY", "MAP", "STRUCT", "LARGEINT", "BITMAP", "HLL")),
            TYPE_CONVERTER);

    assertTrue(result.requiresSqlExecution());
    assertEquals(
        Arrays.asList(
            "`array_col` AS `array_col`",
            "`map_col` AS `map_col`",
            "`struct_col` AS `struct_col`",
            "`large_col` AS `large_col`",
            "BITMAP_TO_BASE64(`bitmap_col`) AS `bitmap_col`",
            "HLL_TO_BASE64(`hll_col`) AS `hll_col`"),
        result.projections());

    assertIncompatible(
        new Column[] {column("future_col", Types.ExternalType.of("UNKNOWN"), null, true)},
        new DorisPhysicalSchema(
            schema(field("future_col", DataTypes.StringType, true)), Arrays.asList("FUTURE_TYPE")));
  }

  @Test
  void testPlansWithoutFeTypeNamesConservatively() {
    Column[] supported =
        new Column[] {
          column("id", Types.IntegerType.get(), null, true),
          column("event_time", Types.TimestampType.withoutTimeZone(6), null, true),
          column("payload", Types.BinaryType.get(), null, true)
        };
    DorisReadSchema result =
        DorisSchemaCompatibility.planReadSchema(
            IDENTIFIER,
            table(supported),
            schema(
                field("id", DataTypes.IntegerType, true),
                field("event_time", DataTypes.TimestampType, true),
                field("payload", DataTypes.BinaryType, true)),
            TYPE_CONVERTER);

    assertTrue(result.requiresSqlExecution());
    assertEquals(
        Arrays.asList("`id`", "`event_time` AS `event_time`", "TO_BASE64(`payload`) AS `payload`"),
        result.projections());

    assertIncompatible(
        new Column[] {column("value", Types.ExternalType.of("JSON"), null, true)},
        schema(field("value", DataTypes.StringType, true)));
    assertIncompatible(
        new Column[] {
          column("value", Types.ListType.of(Types.IntegerType.get(), true), null, true)
        },
        schema(field("value", DataTypes.createArrayType(DataTypes.IntegerType), true)));
    assertIncompatible(
        new Column[] {column("value", Types.IntegerType.unsigned(), null, true)},
        schema(field("value", DataTypes.LongType, true)));
  }

  @Test
  void testNormalizesJsonbVariantAndIpFamiliesFromFeTypeNames() {
    Column[] logicalColumns =
        new Column[] {
          column("jsonb_col", Types.ExternalType.of("JSON"), null, true),
          column("variant_col", Types.ExternalType.of("UNKNOWN"), null, true),
          column("ipv4_col", Types.ExternalType.of("UNKNOWN"), null, true),
          column("ipv6_col", Types.ExternalType.of("UNKNOWN"), null, true)
        };
    StructType physicalSchema =
        schema(
            field("jsonb_col", DataTypes.StringType, true),
            field("variant_col", DataTypes.StringType, true),
            field("ipv4_col", DataTypes.StringType, true),
            field("ipv6_col", DataTypes.StringType, true));

    DorisReadSchema result =
        DorisSchemaCompatibility.planReadSchema(
            IDENTIFIER,
            table(logicalColumns),
            new DorisPhysicalSchema(
                physicalSchema, Arrays.asList("JSONB", "VARIANT", "IPV4", "IPV6")),
            TYPE_CONVERTER);

    assertTrue(result.requiresSqlExecution());
    assertEquals(
        ImmutableSet.of("jsonb_col", "variant_col", "ipv4_col", "ipv6_col"),
        result.normalizedColumns());
    Arrays.stream(result.schema().fields())
        .forEach(field -> assertEquals(DataTypes.StringType, field.dataType()));
    assertEquals(
        Arrays.asList(
            "`jsonb_col` AS `jsonb_col`",
            "`variant_col` AS `variant_col`",
            "`ipv4_col` AS `ipv4_col`",
            "`ipv6_col` AS `ipv6_col`"),
        result.projections());
  }

  @Test
  void testNormalizesBinaryVarbinaryAndTimeProbeTypes() {
    Column[] logicalColumns =
        new Column[] {
          column("binary_col", Types.BinaryType.get(), null, true),
          column("varbinary_col", Types.BinaryType.get(), null, true),
          column("time_col", Types.ExternalType.of("time"), null, true)
        };
    StructType physicalSchema =
        schema(
            field("binary_col", DataTypes.BinaryType, true),
            field("varbinary_col", DataTypes.BinaryType, true),
            field("time_col", DataTypes.StringType, true));

    DorisReadSchema result =
        DorisSchemaCompatibility.planReadSchema(
            IDENTIFIER,
            table(logicalColumns),
            new DorisPhysicalSchema(physicalSchema, Arrays.asList("BINARY", "VARBINARY", "TIME")),
            TYPE_CONVERTER);

    assertTrue(result.requiresSqlExecution());
    Arrays.stream(result.schema().fields())
        .forEach(field -> assertEquals(DataTypes.StringType, field.dataType()));
    assertEquals(
        Arrays.asList(
            "TO_BASE64(`binary_col`) AS `binary_col`",
            "TO_BASE64(`varbinary_col`) AS `varbinary_col`",
            "`time_col` AS `time_col`"),
        result.projections());
  }

  @Test
  void testRejectsLogicalDriftIntoStringNormalizedPhysicalTypes() {
    assertIncompatible(
        new Column[] {column("value", Types.TimestampType.withoutTimeZone(), null, true)},
        schema(field("value", DataTypes.LongType, true)));
    assertIncompatible(
        new Column[] {column("value", Types.LongType.get(), null, true)},
        new DorisPhysicalSchema(
            schema(field("value", DataTypes.TimestampType, true)), Arrays.asList("DATETIME")));
    assertIncompatible(
        new Column[] {column("value", Types.StringType.get(), null, true)},
        new DorisPhysicalSchema(
            schema(field("value", DataTypes.BinaryType, true)), Arrays.asList("BINARY")));
    assertIncompatible(
        new Column[] {column("value", Types.LongType.get(), null, true)},
        new DorisPhysicalSchema(
            schema(field("value", DataTypes.StringType, true)), Arrays.asList("BIGINT UNSIGNED")));
    assertIncompatible(
        new Column[] {column("value", Types.LongType.get(), null, true)},
        new DorisPhysicalSchema(
            schema(field("value", DataTypes.StringType, true)), Arrays.asList("VARIANT")));
    assertIncompatible(
        new Column[] {column("value", Types.StringType.get(), null, true)},
        new DorisPhysicalSchema(
            schema(field("value", DataTypes.StringType, true)), Arrays.asList("FUTURE_TYPE")));
  }

  @Test
  void testRejectsDirectionalSchemaDrift() {
    Column logical = column("id", Types.IntegerType.get(), null, true);

    assertIncompatible(new Column[] {logical}, new StructType());
    assertIncompatible(
        new Column[] {logical}, schema(field("different", DataTypes.IntegerType, true)));
    assertIncompatible(new Column[] {logical}, schema(field("id", DataTypes.LongType, true)));
    assertIncompatible(new Column[] {logical}, schema(field("id", DataTypes.IntegerType, false)));

    Column[] ordered =
        new Column[] {
          column("first", Types.IntegerType.get(), null, true),
          column("second", Types.IntegerType.get(), null, true)
        };
    assertIncompatible(
        ordered,
        schema(
            field("second", DataTypes.IntegerType, true),
            field("first", DataTypes.IntegerType, true)));
  }

  @Test
  void testQuotesDorisIdentifiersInSqlProjection() {
    Identifier quotedIdentifier = Identifier.of(new String[] {"sales`db"}, "order`table");
    DorisReadSchema result =
        DorisSchemaCompatibility.planReadSchema(
            quotedIdentifier,
            table(
                new Column[] {
                  column("odd`column", Types.TimestampType.withoutTimeZone(), null, true)
                }),
            schema(field("odd`column", DataTypes.TimestampType, true)),
            TYPE_CONVERTER);

    assertEquals(
        "(SELECT `odd``column` AS `odd``column` "
            + "FROM `sales``db`.`order``table`) gravitino_doris_source",
        result.tableOrQuery(quotedIdentifier));
  }

  private static void assertIncompatible(Column[] columns, StructType physicalSchema) {
    assertIncompatible(columns, DorisPhysicalSchema.withoutTypeNames(physicalSchema));
  }

  private static void assertIncompatible(Column[] columns, DorisPhysicalSchema physicalSchema) {
    assertIncompatible(table(columns), physicalSchema);
  }

  private static void assertIncompatible(Table logicalTable, DorisPhysicalSchema physicalSchema) {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                DorisSchemaCompatibility.planReadSchema(
                    IDENTIFIER, logicalTable, physicalSchema, TYPE_CONVERTER));
    assertTrue(exception.getMessage().contains("analytics.events"));
  }

  private static Table table(Column[] columns) {
    Table table = mock(Table.class);
    when(table.columns()).thenReturn(columns);
    when(table.properties()).thenReturn(ImmutableMap.of());
    return table;
  }

  private static Column column(String name, Type type, String comment, boolean nullable) {
    return Column.of(name, type, comment, nullable, false, Column.DEFAULT_VALUE_NOT_SET);
  }

  private static StructField field(String name, DataType type, boolean nullable) {
    return DataTypes.createStructField(name, type, nullable);
  }

  private static StructType schema(StructField... fields) {
    return DataTypes.createStructType(Arrays.asList(fields));
  }

  private static class TestDorisTypeConverter extends SparkTypeConverter {
    // This version-neutral test converter mirrors DorisSparkTypeConverter35's character-type
    // normalization without making spark-common tests depend on a Spark 3.5 source set.
    @Override
    public DataType toSparkType(Type gravitinoType) {
      if (gravitinoType instanceof Types.FixedCharType
          || gravitinoType instanceof Types.VarCharType) {
        return DataTypes.StringType;
      }
      return super.toSparkType(gravitinoType);
    }
  }
}
