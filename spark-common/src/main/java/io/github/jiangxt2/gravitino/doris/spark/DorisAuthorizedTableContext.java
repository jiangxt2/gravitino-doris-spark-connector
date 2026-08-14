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

import java.util.Objects;
import java.util.stream.Stream;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.StructType;

/** Authorized table state made available to a future governed Doris write delegate. */
public final class DorisAuthorizedTableContext {

  private final Identifier identifier;
  private final org.apache.gravitino.rel.Table gravitinoTable;
  private final TableCatalog physicalCatalog;
  private final Table physicalTable;
  private final Table readDelegate;
  private final DorisReadSchema readSchema;
  private final DorisJdbcConnectionInfo connectionInfo;
  private final DorisJdbcReadOptions readOptions;
  private final DorisWritePolicy writePolicy;

  /**
   * Creates a read-only context from the original extension-seam contract.
   *
   * <p>This overload preserves source compatibility for existing factories. It does not authorize
   * writes and treats every field as a direct, non-normalized Doris projection.
   */
  public DorisAuthorizedTableContext(
      Identifier identifier,
      org.apache.gravitino.rel.Table gravitinoTable,
      TableCatalog physicalCatalog,
      Table physicalTable,
      Table readDelegate,
      StructType validatedSchema,
      DorisJdbcConnectionInfo connectionInfo,
      DorisJdbcReadOptions readOptions) {
    this(
        identifier,
        gravitinoTable,
        physicalCatalog,
        physicalTable,
        readDelegate,
        new DorisReadSchema(
            validatedSchema,
            Stream.of(validatedSchema.fieldNames()).map(DorisReadSchema::quoteIdentifier).toList(),
            false),
        connectionInfo,
        readOptions,
        DorisWritePolicy.disabled());
  }

  /** Creates an immutable context after SELECT_TABLE authorization and schema validation. */
  public DorisAuthorizedTableContext(
      Identifier identifier,
      org.apache.gravitino.rel.Table gravitinoTable,
      TableCatalog physicalCatalog,
      Table physicalTable,
      Table readDelegate,
      DorisReadSchema readSchema,
      DorisJdbcConnectionInfo connectionInfo,
      DorisJdbcReadOptions readOptions,
      DorisWritePolicy writePolicy) {
    this.identifier = Objects.requireNonNull(identifier, "identifier");
    this.gravitinoTable = Objects.requireNonNull(gravitinoTable, "gravitinoTable");
    this.physicalCatalog = Objects.requireNonNull(physicalCatalog, "physicalCatalog");
    this.physicalTable = Objects.requireNonNull(physicalTable, "physicalTable");
    this.readDelegate = Objects.requireNonNull(readDelegate, "readDelegate");
    this.readSchema = Objects.requireNonNull(readSchema, "readSchema");
    this.connectionInfo = Objects.requireNonNull(connectionInfo, "connectionInfo");
    this.readOptions = Objects.requireNonNull(readOptions, "readOptions");
    this.writePolicy = Objects.requireNonNull(writePolicy, "writePolicy");
  }

  /** Returns the authorized identifier. */
  public Identifier identifier() {
    return identifier;
  }

  /** Returns the governed Gravitino table metadata. */
  public org.apache.gravitino.rel.Table gravitinoTable() {
    return gravitinoTable;
  }

  /** Returns the initialized official Doris catalog. */
  public TableCatalog physicalCatalog() {
    return physicalCatalog;
  }

  /** Returns the original official Doris table, including its native write implementation. */
  public Table physicalTable() {
    return physicalTable;
  }

  /** Returns the validated read delegate. */
  public Table readDelegate() {
    return readDelegate;
  }

  /** Returns the Spark-visible validated schema. */
  public StructType validatedSchema() {
    return readSchema.schema();
  }

  /** Returns the validated read plan used to reject lossy write mappings. */
  public DorisReadSchema readSchema() {
    return readSchema;
  }

  /** Returns credential-vended JDBC connection material for trusted delegate construction. */
  public DorisJdbcConnectionInfo connectionInfo() {
    return connectionInfo;
  }

  /** Returns the validated JDBC read tuning options. */
  public DorisJdbcReadOptions readOptions() {
    return readOptions;
  }

  /** Returns the catalog-managed write policy authorized for this table instance. */
  public DorisWritePolicy writePolicy() {
    return writePolicy;
  }

  @Override
  public String toString() {
    return "DorisAuthorizedTableContext{" + identifier + ", credentials=redacted}";
  }
}
