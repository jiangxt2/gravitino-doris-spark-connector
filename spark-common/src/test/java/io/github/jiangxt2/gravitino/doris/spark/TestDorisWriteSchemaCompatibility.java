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

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;

public class TestDorisWriteSchemaCompatibility {

  @Test
  void acceptsExactDirectTypesAndCertifiedDatetimeStrings() {
    Table direct =
        table(
            column("id", Types.IntegerType.get(), false),
            column("name", Types.StringType.get(), true));
    DorisReadSchema directRead =
        readSchema(
            schema(
                field("id", DataTypes.IntegerType, false),
                field("name", DataTypes.StringType, true)),
            Set.of());
    assertThatCode(
            () ->
                DorisWriteSchemaCompatibility.validate(
                    direct,
                    directRead,
                    schema(
                        field("id", DataTypes.IntegerType, false),
                        field("name", DataTypes.StringType, true))))
        .doesNotThrowAnyException();

    Table datetime = table(column("event_time", Types.TimestampType.withoutTimeZone(6), true));
    DorisReadSchema datetimeRead =
        readSchema(schema(field("event_time", DataTypes.StringType, true)), Set.of("event_time"));
    assertThatThrownBy(
            () ->
                DorisWriteSchemaCompatibility.validate(
                    datetime,
                    datetimeRead,
                    schema(field("event_time", DataTypes.TimestampType, true))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("not writable");
    DorisWriteSchemaCompatibility.Validator validator =
        DorisWriteSchemaCompatibility.validate(
            datetime, datetimeRead, schema(field("event_time", DataTypes.StringType, true)));
    assertThatCode(() -> validator.validate(row("2024-02-29 23:59:59.123456")))
        .doesNotThrowAnyException();
    assertThatCode(() -> validator.validate(new GenericInternalRow(new Object[] {null})))
        .doesNotThrowAnyException();
  }

  @Test
  void validatesDatetimeGrammarCalendarAndExactPrecisionWithoutLeakingValues() {
    Table datetime = table(column("event_time", Types.TimestampType.withoutTimeZone(3), true));
    DorisReadSchema datetimeRead =
        readSchema(schema(field("event_time", DataTypes.StringType, true)), Set.of("event_time"));
    DorisWriteSchemaCompatibility.Validator validator =
        DorisWriteSchemaCompatibility.validate(
            datetime, datetimeRead, schema(field("event_time", DataTypes.StringType, true)));

    assertThatCode(() -> validator.validate(row("1969-12-31 23:59:59.001")))
        .doesNotThrowAnyException();
    for (String invalid :
        List.of(
            "2024-02-30 00:00:00.001",
            "2024-02-29T00:00:00.001",
            "2024-02-29 00:00:00.01",
            "2024-02-29 00:00:00.0010",
            "secret-value")) {
      assertThatThrownBy(() -> validator.validate(row(invalid)))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("precision-specific format")
          .hasMessageNotContaining(invalid);
    }
  }

  @Test
  void rejectsDriftNullableToRequiredAndNonDatetimeNormalization() {
    Table target = table(column("id", Types.IntegerType.get(), false));
    DorisReadSchema read = readSchema(schema(field("id", DataTypes.IntegerType, false)), Set.of());
    assertThatThrownBy(
            () ->
                DorisWriteSchemaCompatibility.validate(
                    target, read, schema(field("other", DataTypes.IntegerType, false))))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () ->
                DorisWriteSchemaCompatibility.validate(
                    target, read, schema(field("id", DataTypes.LongType, false))))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () ->
                DorisWriteSchemaCompatibility.validate(
                    target, read, schema(field("id", DataTypes.IntegerType, true))))
        .isInstanceOf(IllegalArgumentException.class);

    Table binary = table(column("payload", Types.BinaryType.get(), true));
    DorisReadSchema normalized =
        readSchema(schema(field("payload", DataTypes.StringType, true)), Set.of("payload"));
    assertThatThrownBy(
            () ->
                DorisWriteSchemaCompatibility.validate(
                    binary, normalized, schema(field("payload", DataTypes.StringType, true))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("not writable");
  }

  private static DorisReadSchema readSchema(StructType schema, Set<String> normalized) {
    return new DorisReadSchema(
        schema,
        Arrays.stream(schema.fieldNames()).map(DorisReadSchema::quoteIdentifier).toList(),
        !normalized.isEmpty(),
        normalized);
  }

  private static Table table(Column... columns) {
    Table table = mock(Table.class);
    when(table.columns()).thenReturn(columns);
    return table;
  }

  private static Column column(String name, Type type, boolean nullable) {
    return Column.of(name, type, null, nullable, false, Column.DEFAULT_VALUE_NOT_SET);
  }

  private static StructField field(String name, DataType type, boolean nullable) {
    return DataTypes.createStructField(name, type, nullable);
  }

  private static StructType schema(StructField... fields) {
    return DataTypes.createStructType(List.of(fields));
  }

  private static GenericInternalRow row(String value) {
    return new GenericInternalRow(new Object[] {UTF8String.fromString(value)});
  }
}
