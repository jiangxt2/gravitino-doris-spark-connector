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

import java.util.List;
import java.util.Set;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.types.StructType;

/** Validated Doris read schema and the SQL projection needed to produce it. */
public final class DorisReadSchema {

  private static final String SQL_SOURCE_ALIAS = "gravitino_doris_source";

  private final StructType schema;
  private final List<String> projections;
  private final boolean requiresSqlExecution;
  private final Set<String> normalizedColumns;

  /**
   * Creates a validated read schema.
   *
   * @param schema the Spark-visible schema
   * @param projections the Doris SQL projections in schema order
   * @param requiresSqlExecution whether the SQL lane is required for value normalization
   */
  public DorisReadSchema(
      StructType schema, List<String> projections, boolean requiresSqlExecution) {
    this(schema, projections, requiresSqlExecution, Set.of());
  }

  /**
   * Creates a validated read schema with columns whose Spark-visible values require SQL-lane
   * normalization.
   *
   * @param schema the Spark-visible schema
   * @param projections the Doris SQL projections in schema order
   * @param requiresSqlExecution whether the SQL lane is required by the unpruned schema
   * @param normalizedColumns columns whose Doris and Spark-visible operator semantics differ
   */
  public DorisReadSchema(
      StructType schema,
      List<String> projections,
      boolean requiresSqlExecution,
      Set<String> normalizedColumns) {
    DorisChecks.checkArgument(
        schema.fields().length == projections.size(),
        "Doris schema and SQL projection counts differ");
    this.schema = schema;
    this.projections = List.copyOf(projections);
    this.requiresSqlExecution = requiresSqlExecution;
    this.normalizedColumns = Set.copyOf(normalizedColumns);
  }

  /** Returns the Spark-visible schema. */
  public StructType schema() {
    return schema;
  }

  /** Returns the validated SQL projections in schema order. */
  public List<String> projections() {
    return projections;
  }

  /** Returns whether value normalization requires the Doris SQL execution lane. */
  public boolean requiresSqlExecution() {
    return requiresSqlExecution;
  }

  /** Returns columns whose values use the SQL lane's explicit Spark representation. */
  public Set<String> normalizedColumns() {
    return normalizedColumns;
  }

  /**
   * Returns the JDBC table subquery used by Spark's JDBC scan builder.
   *
   * @param identifier the authorized Doris identifier
   * @return a credential-free table subquery
   */
  public String tableOrQuery(Identifier identifier) {
    DorisChecks.checkArgument(
        identifier.namespace().length == 1,
        "Doris table identifiers require exactly one schema: %s",
        identifier);
    return String.format(
        "(SELECT %s FROM %s.%s) %s",
        String.join(", ", projections),
        quoteIdentifier(identifier.namespace()[0]),
        quoteIdentifier(identifier.name()),
        SQL_SOURCE_ALIAS);
  }

  static String quoteIdentifier(String identifier) {
    return "`" + identifier.replace("`", "``") + "`";
  }
}
