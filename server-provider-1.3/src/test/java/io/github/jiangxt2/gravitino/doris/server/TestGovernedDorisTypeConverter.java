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

package io.github.jiangxt2.gravitino.doris.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter.JdbcTypeBean;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestGovernedDorisTypeConverter {

  private final GovernedDorisTypeConverter converter = new GovernedDorisTypeConverter();

  @Test
  void mapsCommonDorisTypesAndPreservesParameterizedTypes() {
    assertThat(convert("BOOLEAN")).isEqualTo(Types.BooleanType.get());
    assertThat(convert("TINYINT")).isEqualTo(Types.ByteType.get());
    assertThat(convert("SMALLINT")).isEqualTo(Types.ShortType.get());
    assertThat(convert("INT")).isEqualTo(Types.IntegerType.get());
    assertThat(convert("BIGINT")).isEqualTo(Types.LongType.get());
    assertThat(convert("FLOAT")).isEqualTo(Types.FloatType.get());
    assertThat(convert("DOUBLE")).isEqualTo(Types.DoubleType.get());
    assertThat(convert("DATEV2")).isEqualTo(Types.DateType.get());
    assertThat(convert("STRING")).isEqualTo(Types.StringType.get());
    assertThat(convert("CHAR(8)")).isEqualTo(Types.FixedCharType.of(8));
    assertThat(convert("VARCHAR(64)")).isEqualTo(Types.VarCharType.of(64));
    assertThat(convert("DECIMAL(18,3)")).isEqualTo(Types.DecimalType.of(18, 3));
    assertThat(convert("BINARY")).isEqualTo(Types.BinaryType.get());
    assertThat(convert("VARBINARY")).isEqualTo(Types.BinaryType.get());
    assertThat(convert("TIME")).isEqualTo(Types.ExternalType.of("time"));
  }

  @Test
  void mapsGravitinoTypesBackToDorisWithoutLosingParameters() {
    assertThat(converter.fromGravitino(Types.IntegerType.get())).isEqualTo("int");
    assertThat(converter.fromGravitino(Types.DecimalType.of(18, 3))).isEqualTo("decimal(18,3)");
    assertThat(converter.fromGravitino(Types.FixedCharType.of(8))).isEqualTo("char(8)");
    assertThat(converter.fromGravitino(Types.VarCharType.of(64))).isEqualTo("varchar(64)");
    assertThat(converter.fromGravitino(Types.TimestampType.withoutTimeZone(6)))
        .isEqualTo("datetime(6)");
    assertThatThrownBy(() -> converter.fromGravitino(Types.ExternalType.of("variant")))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> converter.fromGravitino(Types.BinaryType.get()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"DECIMALV2(27,9)", "DECIMAL32(9,2)", "DECIMAL64(18,3)", "DECIMAL128(38,6)"})
  void mapsSupportedDecimalFamilies(String typeName) {
    JdbcTypeBean bean = new JdbcTypeBean(typeName);
    assertThat(converter.toGravitino(bean)).isInstanceOf(Types.DecimalType.class);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "DECIMAL256(76,12)",
        "ARRAY<INT>",
        "MAP<STRING,INT>",
        "JSONB",
        "VARIANT",
        "IPV4",
        "IPV6"
      })
  void preservesWideAndComplexTypesAsExternal(String typeName) {
    JdbcTypeBean bean = new JdbcTypeBean(typeName);
    assertThat(converter.toGravitino(bean)).isInstanceOf(Types.ExternalType.class);
  }

  @ParameterizedTest
  @ValueSource(strings = {"DATETIME", "DATETIMEV2(6)"})
  void preservesDorisDatetimeAsTimestampWithoutTimezone(String typeName) {
    JdbcTypeBean bean = new JdbcTypeBean(typeName);
    Types.TimestampType type = (Types.TimestampType) converter.toGravitino(bean);
    assertThat(type.hasTimeZone()).isFalse();
  }

  private Type convert(String typeName) {
    return converter.toGravitino(new JdbcTypeBean(typeName));
  }
}
