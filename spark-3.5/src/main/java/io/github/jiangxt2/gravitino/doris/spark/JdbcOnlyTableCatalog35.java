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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTable;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import scala.Tuple2;
import scala.collection.immutable.Map$;

/** Spark JDBC V2 catalog used only by the strict verified transport profile. */
// JDBCTableCatalog implements Spark varargs methods with array parameters, which javac reports on
// subclasses even though this adapter does not override those methods.
@SuppressWarnings("overrides")
final class JdbcOnlyTableCatalog35 extends JDBCTableCatalog {

  /** Loads one physical schema snapshot using the already-validated JDBC connection material. */
  DorisPhysicalSchema loadPhysicalSchema(
      Identifier identifier, DorisJdbcConnectionInfo connectionInfo) {
    DorisChecks.checkArgument(
        identifier.namespace().length == 1,
        "Doris table identifiers require exactly one schema: %s",
        identifier);
    JDBCOptions options =
        jdbcOptions(
            connectionInfo,
            String.format(
                "%s.%s",
                DorisReadSchema.quoteIdentifier(identifier.namespace()[0]),
                DorisReadSchema.quoteIdentifier(identifier.name())),
            DorisJdbcReadOptions.from(Map.of()));
    JdbcDialect dialect = JdbcDialects.get(connectionInfo.url());
    try (Connection connection = dialect.createConnectionFactory(options).apply(0)) {
      connection.setCatalog(identifier.namespace()[0]);
      DatabaseMetaData metadata = connection.getMetaData();
      String tablePattern =
          escapeMetadataPattern(metadata.getSearchStringEscape(), identifier.name());
      try (ResultSet columns =
          metadata.getColumns(
              connection.getCatalog(), connection.getSchema(), tablePattern, null)) {
        return physicalSchema(identifier, columns);
      }
    } catch (SQLException | RuntimeException e) {
      throw new IllegalArgumentException(
          String.format("Failed to load verified JDBC schema for %s", identifier));
    }
  }

  /** Creates a schema-seeded Spark JDBC V2 table without another metadata request. */
  Table loadTable(
      Identifier identifier,
      DorisReadSchema readSchema,
      DorisJdbcConnectionInfo connectionInfo,
      DorisJdbcReadOptions readOptions) {
    return new JDBCTable(
        identifier,
        readSchema.schema(),
        jdbcOptions(connectionInfo, readSchema.tableOrQuery(identifier), readOptions));
  }

  static DorisPhysicalSchema physicalSchema(Identifier identifier, ResultSet columns)
      throws SQLException {
    List<StructField> fields = new ArrayList<>();
    List<String> typeNames = new ArrayList<>();
    while (columns.next()) {
      if (!identifier.name().equals(columns.getString("TABLE_NAME"))) {
        continue;
      }
      String typeName = columns.getString("TYPE_NAME");
      int jdbcType = columns.getInt("DATA_TYPE");
      int precision = columns.getInt("COLUMN_SIZE");
      int scale = columns.getInt("DECIMAL_DIGITS");
      DataType dataType = toCatalystJdbcTypeOrString(typeName, jdbcType, precision, scale);
      fields.add(
          DataTypes.createStructField(
              columns.getString("COLUMN_NAME"),
              dataType,
              columns.getInt("NULLABLE") != DatabaseMetaData.columnNoNulls));
      typeNames.add(typeNameWithParameters(typeName, precision, scale));
    }
    DorisChecks.checkArgument(
        !fields.isEmpty(), "Verified JDBC metadata returned no columns for %s", identifier);
    return new DorisPhysicalSchema(DataTypes.createStructType(fields), typeNames);
  }

  static String escapeMetadataPattern(String escape, String identifier) {
    if (escape == null || escape.isEmpty()) {
      return identifier;
    }
    DorisChecks.checkArgument(
        escape.length() == 1, "JDBC metadata search escape must be a single character");
    char escapeCharacter = escape.charAt(0);
    StringBuilder escaped = new StringBuilder(identifier.length());
    for (int index = 0; index < identifier.length(); index++) {
      char character = identifier.charAt(index);
      if (character == escapeCharacter || character == '_' || character == '%') {
        escaped.append(escapeCharacter);
      }
      escaped.append(character);
    }
    return escaped.toString();
  }

  static DataType toCatalystJdbcTypeOrString(
      String typeName, int jdbcType, int precision, int scale) {
    if ("BIT".equalsIgnoreCase(typeName) && jdbcType == Types.BIT && precision == 1 && scale == 0) {
      return DataTypes.BooleanType;
    }
    return SchemaSeededDorisTableCatalog35.toCatalystTypeOrString(typeName, precision, scale);
  }

  private static String typeNameWithParameters(String typeName, int precision, int scale) {
    if (typeName != null
        && typeName.toLowerCase(Locale.ROOT).startsWith("decimal")
        && typeName.indexOf('(') < 0
        && precision > 0) {
      return String.format(Locale.ROOT, "%s(%d,%d)", typeName, precision, scale);
    }
    return typeName;
  }

  private static JDBCOptions jdbcOptions(
      DorisJdbcConnectionInfo connectionInfo,
      String tableOrQuery,
      DorisJdbcReadOptions readOptions) {
    scala.collection.immutable.Map<String, String> parameters = Map$.MODULE$.empty();
    parameters = parameters.$plus(new Tuple2<>("driver", connectionInfo.driver()));
    parameters = parameters.$plus(new Tuple2<>("user", connectionInfo.user()));
    parameters = parameters.$plus(new Tuple2<>("password", connectionInfo.password()));
    for (Map.Entry<String, String> entry : readOptions.asSparkOptions().entrySet()) {
      parameters = parameters.$plus(new Tuple2<>(entry.getKey(), entry.getValue()));
    }
    return new JDBCOptions(connectionInfo.url(), tableOrQuery, parameters);
  }
}
