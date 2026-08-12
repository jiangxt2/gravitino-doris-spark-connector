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

import java.util.Set;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.gravitino.spark.connector.SparkTableChangeConverter;
import org.apache.gravitino.spark.connector.SparkTransformConverter;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.StructType;

/** Spark 3.5 implementation of the governed, read-only Doris Spark catalog. */
public class GovernedDorisCatalogSpark35 extends GovernedDorisCatalog {

  @Override
  protected SparkTypeConverter getSparkTypeConverter() {
    return new DorisSparkTypeConverter35();
  }

  @Override
  protected SparkTableChangeConverter getSparkTableChangeConverter(
      SparkTypeConverter sparkTypeConverter) {
    return new SparkTableChangeConverter(sparkTypeConverter);
  }

  @Override
  protected TableCatalog createDorisTableCatalog() {
    return new SchemaSeededDorisTableCatalog35();
  }

  @Override
  protected TableCatalog createDorisTableCatalog(DorisReadTransport transport) {
    return transport.allowsNativeLane()
        ? new SchemaSeededDorisTableCatalog35()
        : new JdbcOnlyTableCatalog35();
  }

  @Override
  protected DorisPhysicalSchema loadPhysicalSchema(
      TableCatalog sparkCatalog, Identifier identifier, Table sparkTable) {
    DorisChecks.checkArgument(
        sparkCatalog instanceof SchemaSeededDorisTableCatalog35,
        "Unexpected Doris table catalog implementation: %s",
        sparkCatalog.getClass().getName());
    return ((SchemaSeededDorisTableCatalog35) sparkCatalog).loadPhysicalSchema(identifier);
  }

  @Override
  protected Table createSchemaSeededDorisTable(
      TableCatalog sparkCatalog,
      Identifier identifier,
      DorisReadSchema readSchema,
      DorisJdbcConnectionInfo connectionInfo,
      DorisJdbcReadOptions readOptions) {
    DorisChecks.checkArgument(
        sparkCatalog instanceof SchemaSeededDorisTableCatalog35,
        "Unexpected Doris table catalog implementation: %s",
        sparkCatalog.getClass().getName());
    return ((SchemaSeededDorisTableCatalog35) sparkCatalog)
        .loadTable(identifier, readSchema, connectionInfo, readOptions);
  }

  @Override
  protected DorisPhysicalSchema loadStrictPhysicalSchema(
      TableCatalog sparkCatalog, Identifier identifier, DorisJdbcConnectionInfo connectionInfo) {
    DorisChecks.checkArgument(
        sparkCatalog instanceof JdbcOnlyTableCatalog35,
        "Unexpected strict Doris catalog implementation: %s",
        sparkCatalog.getClass().getName());
    return ((JdbcOnlyTableCatalog35) sparkCatalog).loadPhysicalSchema(identifier, connectionInfo);
  }

  @Override
  protected Table createStrictJdbcTable(
      TableCatalog sparkCatalog,
      Identifier identifier,
      DorisReadSchema readSchema,
      DorisJdbcConnectionInfo connectionInfo,
      DorisJdbcReadOptions readOptions) {
    DorisChecks.checkArgument(
        sparkCatalog instanceof JdbcOnlyTableCatalog35,
        "Unexpected strict Doris catalog implementation: %s",
        sparkCatalog.getClass().getName());
    return ((JdbcOnlyTableCatalog35) sparkCatalog)
        .loadTable(identifier, readSchema, connectionInfo, readOptions);
  }

  @Override
  protected Table createGovernedDorisTable(
      Identifier identifier,
      org.apache.gravitino.rel.Table gravitinoTable,
      Table readDelegate,
      StructType validatedSchema,
      PropertiesConverter propertiesConverter,
      SparkTransformConverter sparkTransformConverter,
      SparkTypeConverter sparkTypeConverter) {
    return new GovernedDorisTable35(
        identifier,
        gravitinoTable,
        readDelegate,
        validatedSchema,
        propertiesConverter,
        sparkTransformConverter,
        sparkTypeConverter,
        getCapabilityPolicy());
  }

  /**
   * Rejects Spark's write-aware table loading without linking to patch-specific API types.
   *
   * <p>{@code TableWritePrivilege} and the corresponding {@code TableCatalog} overload are absent
   * from early Spark 3.5 patch releases. The raw {@link Set} keeps the erased method descriptor
   * used by later patches while allowing this adapter to compile and load across Spark 3.5.x.
   */
  @SuppressWarnings("rawtypes")
  public Table loadTable(Identifier ident, Set writePrivileges) throws NoSuchTableException {
    if (!getCapabilityPolicy().allowsTableWrites()) {
      throw getCapabilityPolicy().reject("table writes");
    }
    return loadTableForWriting(ident);
  }
}
