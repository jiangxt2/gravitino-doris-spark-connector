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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import org.apache.doris.spark.catalog.DorisTableCatalog;
import org.apache.doris.spark.config.DorisConfig;
import org.apache.doris.spark.config.DorisOptions;
import org.apache.doris.spark.exception.OptionRequiredException;
import org.apache.doris.spark.rest.models.Field;
import org.apache.doris.spark.rest.models.Schema;
import org.apache.doris.spark.util.SchemaConvertors;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTable;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import scala.Option;
import scala.Tuple2;
import scala.collection.immutable.Map$;

/** Official Doris 26.0.0 catalog adapter that can seed one validated Spark schema snapshot. */
// DorisTableCatalog 26.0.0 implements Spark varargs methods with array parameters, which javac
// reports on subclasses even though this adapter does not override those methods.
@SuppressWarnings("overrides")
class SchemaSeededDorisTableCatalog35 extends DorisTableCatalog {

  private Map<String, String> catalogOptions;

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    super.initialize(name, options);
    catalogOptions = new HashMap<>(options.asCaseSensitiveMap());
  }

  /**
   * Creates a Doris table with native and JDBC SQL execution lanes.
   *
   * @param identifier the authorized Spark table identifier
   * @param readSchema the validated schema and SQL normalization plan
   * @param connectionInfo the credential-vended JDBC connection material
   * @return a read-only hybrid Doris table
   */
  Table loadTable(
      Identifier identifier,
      DorisReadSchema readSchema,
      DorisJdbcConnectionInfo connectionInfo,
      DorisJdbcReadOptions readOptions) {
    DorisChecks.checkState(catalogOptions != null, "Doris table catalog is not initialized");
    DorisChecks.checkArgument(
        identifier.namespace().length == 1,
        "Doris table identifiers require exactly one schema: %s",
        identifier);

    Map<String, String> tableOptions = new HashMap<>(catalogOptions);
    tableOptions.put(
        DorisOptions.DORIS_TABLE_IDENTIFIER.getName(),
        String.format(
            "%s.%s",
            DorisReadSchema.quoteIdentifier(identifier.namespace()[0]),
            DorisReadSchema.quoteIdentifier(identifier.name())));
    try {
      DorisConfig tableConfig = DorisConfig.fromMap(tableOptions, false);
      Table nativeTable =
          newTableInstance(identifier, tableConfig, Option.apply(readSchema.schema()));
      JDBCTable sqlTable =
          new JDBCTable(
              identifier,
              readSchema.schema(),
              new JDBCOptions(
                  connectionInfo.url(),
                  readSchema.tableOrQuery(identifier),
                  jdbcParameters(connectionInfo, readOptions)));
      return new DorisHybridTable35(nativeTable, sqlTable, identifier, readSchema);
    } catch (OptionRequiredException | RuntimeException e) {
      // Do not retain third-party configuration exception text because the options include the
      // vended credential.
      throw new IllegalArgumentException(
          String.format("Failed to create Doris table configuration for %s", identifier));
    }
  }

  /** Loads one FE schema response while retaining the original Doris type names. */
  DorisPhysicalSchema loadPhysicalSchema(Identifier identifier) {
    DorisChecks.checkArgument(
        identifier.namespace().length == 1,
        "Doris table identifiers require exactly one schema: %s",
        identifier);
    try {
      Schema dorisSchema = frontend().getTableSchema(identifier.namespace()[0], identifier.name());
      List<StructField> fields = new ArrayList<>(dorisSchema.size());
      List<String> typeNames = new ArrayList<>(dorisSchema.size());
      for (Field field : dorisSchema.getProperties()) {
        DataType dataType =
            toCatalystTypeOrString(field.getType(), field.getPrecision(), field.getScale());
        fields.add(DataTypes.createStructField(field.getName(), dataType, true));
        typeNames.add(typeNameWithParameters(field));
      }
      return new DorisPhysicalSchema(DataTypes.createStructType(fields), typeNames);
    } catch (Exception e) {
      // Do not retain the Connector exception because its request context includes credentials.
      throw new IllegalArgumentException(
          String.format("Failed to load Doris physical schema for %s", identifier));
    }
  }

  static DataType toCatalystTypeOrString(String typeName, int precision, int scale) {
    Objects.requireNonNull(typeName, "Doris FE type name is required");
    try {
      return SchemaConvertors.toCatalystType(typeName, precision, scale);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      // Connector 26.0.0 throws a checked Exception for unrecognized FE types even though the
      // generated Java signature only declares IllegalArgumentException. Match that fixed upstream
      // failure shape narrowly so that malformed known types and other conversion failures still
      // fail closed.
      if (e.getClass().equals(Exception.class)
          && e.getMessage() != null
          && e.getMessage().startsWith("Unrecognized Doris type")) {
        return DataTypes.StringType;
      }
      throw new IllegalArgumentException("Failed to convert Doris FE type");
    }
  }

  static String typeNameWithParameters(Field field) {
    String typeName = field.getType();
    if (typeName == null || typeName.indexOf('(') >= 0) {
      return typeName;
    }
    String baseType = typeName.toLowerCase(Locale.ROOT);
    if (baseType.startsWith("decimal") && field.getPrecision() > 0) {
      return String.format(
          Locale.ROOT, "%s(%d,%d)", typeName, field.getPrecision(), field.getScale());
    }
    return typeName;
  }

  private scala.collection.immutable.Map<String, String> jdbcParameters(
      DorisJdbcConnectionInfo connectionInfo, DorisJdbcReadOptions readOptions) {
    scala.collection.immutable.Map<String, String> parameters = Map$.MODULE$.empty();
    parameters = parameters.$plus(new Tuple2<>("driver", connectionInfo.driver()));
    parameters = parameters.$plus(new Tuple2<>("user", connectionInfo.user()));
    parameters = parameters.$plus(new Tuple2<>("password", connectionInfo.password()));
    for (Map.Entry<String, String> entry : readOptions.asSparkOptions().entrySet()) {
      parameters = parameters.$plus(new Tuple2<>(entry.getKey(), entry.getValue()));
    }
    return parameters;
  }
}
