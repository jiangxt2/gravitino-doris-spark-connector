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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownAggregates;
import org.apache.spark.sql.connector.read.SupportsPushDownLimit;
import org.apache.spark.sql.connector.read.SupportsPushDownOffset;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.connector.read.SupportsPushDownTopN;
import org.apache.spark.sql.connector.read.SupportsPushDownV2Filters;
import org.apache.spark.sql.types.StructType;

/** Selects the SQL lane only when normalization or a SQL-only pushdown requires it. */
final class DorisHybridScanBuilder35
    implements ScanBuilder,
        SupportsPushDownRequiredColumns,
        SupportsPushDownV2Filters,
        SupportsPushDownLimit,
        SupportsPushDownAggregates,
        SupportsPushDownTopN,
        SupportsPushDownOffset {

  private final ScanBuilder nativeBuilder;
  private final SupportsPushDownRequiredColumns nativeColumns;
  private final SupportsPushDownV2Filters nativeFilters;
  private final SupportsPushDownLimit nativeLimit;
  private final ScanBuilder sqlBuilder;
  private final SupportsPushDownRequiredColumns sqlColumns;
  private final SupportsPushDownV2Filters sqlFilters;
  private final SupportsPushDownLimit sqlLimit;
  private final SupportsPushDownAggregates sqlAggregates;
  private final SupportsPushDownTopN sqlTopN;
  private final SupportsPushDownOffset sqlOffset;
  private final Set<String> normalizedColumns;

  private boolean normalizationRequiresSql;
  private boolean sqlOperatorSelected;
  private Integer sqlPushedUpperBound;
  private Predicate[] commonPushedPredicates = new Predicate[0];

  DorisHybridScanBuilder35(ScanBuilder nativeBuilder, ScanBuilder sqlBuilder, boolean sqlRequired) {
    this(nativeBuilder, sqlBuilder, sqlRequired, Collections.emptySet());
  }

  DorisHybridScanBuilder35(
      ScanBuilder nativeBuilder,
      ScanBuilder sqlBuilder,
      boolean sqlRequired,
      Set<String> normalizedColumns) {
    validateSqlBuilder(sqlBuilder);
    if (nativeBuilder != null) {
      validateNativeBuilder(nativeBuilder);
    }
    this.nativeBuilder = nativeBuilder;
    this.nativeColumns =
        nativeBuilder == null ? null : (SupportsPushDownRequiredColumns) nativeBuilder;
    this.nativeFilters = nativeBuilder == null ? null : (SupportsPushDownV2Filters) nativeBuilder;
    this.nativeLimit = nativeBuilder == null ? null : (SupportsPushDownLimit) nativeBuilder;
    this.sqlBuilder = sqlBuilder;
    this.sqlColumns = (SupportsPushDownRequiredColumns) sqlBuilder;
    this.sqlFilters = (SupportsPushDownV2Filters) sqlBuilder;
    this.sqlLimit = (SupportsPushDownLimit) sqlBuilder;
    this.sqlAggregates = (SupportsPushDownAggregates) sqlBuilder;
    this.sqlTopN = (SupportsPushDownTopN) sqlBuilder;
    this.sqlOffset = (SupportsPushDownOffset) sqlBuilder;
    this.normalizedColumns = new HashSet<>();
    normalizedColumns.forEach(
        column -> this.normalizedColumns.add(column.toLowerCase(Locale.ROOT)));
    this.normalizationRequiresSql = sqlRequired;
  }

  @Override
  public Scan build() {
    return selectedBuilder().build();
  }

  @Override
  public void pruneColumns(StructType requiredSchema) {
    sqlColumns.pruneColumns(requiredSchema);
    if (nativeColumns != null) {
      nativeColumns.pruneColumns(requiredSchema);
    }
    normalizationRequiresSql =
        Arrays.stream(requiredSchema.fieldNames())
            .map(column -> column.toLowerCase(Locale.ROOT))
            .anyMatch(normalizedColumns::contains);
  }

  @Override
  public Predicate[] pushPredicates(Predicate[] predicates) {
    Predicate[] safePredicates =
        Arrays.stream(predicates)
            .filter(predicate -> !referencesNormalizedColumn(predicate))
            .toArray(Predicate[]::new);
    Predicate[] sqlResidual = sqlFilters.pushPredicates(safePredicates);
    if (nativeFilters == null) {
      Predicate[] residual = residualInInputOrder(predicates, sqlResidual);
      commonPushedPredicates = subtract(predicates, residual);
      return residual;
    }

    Predicate[] nativeResidual = nativeFilters.pushPredicates(safePredicates);
    List<Predicate> commonResidual = new ArrayList<>();
    for (Predicate predicate : predicates) {
      if (referencesNormalizedColumn(predicate)
          || contains(nativeResidual, predicate)
          || contains(sqlResidual, predicate)) {
        commonResidual.add(predicate);
      }
    }
    Predicate[] residual = commonResidual.toArray(new Predicate[0]);
    commonPushedPredicates = subtract(predicates, residual);
    return residual;
  }

  @Override
  public Predicate[] pushedPredicates() {
    return Arrays.copyOf(commonPushedPredicates, commonPushedPredicates.length);
  }

  @Override
  public boolean pushLimit(int limit) {
    boolean sqlAccepted = sqlLimit.pushLimit(limit);
    if (sqlAccepted) {
      sqlPushedUpperBound = limit;
      // Connector 26.0.0 applies native LIMIT to each scan partition but reports complete
      // pushdown. Select Spark JDBC V2 so a global LIMIT keeps exact direct-JDBC semantics.
      sqlOperatorSelected = true;
      return true;
    }
    // If a future SQL delegate rejects LIMIT, native pushdown is only a pruning hint. Returning
    // false keeps Spark's authoritative global operator in the plan.
    if (nativeLimit != null) {
      nativeLimit.pushLimit(limit);
    }
    return false;
  }

  @Override
  public boolean supportCompletePushDown(Aggregation aggregation) {
    return !referencesNormalizedColumn(aggregation)
        && sqlAggregates.supportCompletePushDown(aggregation);
  }

  @Override
  public boolean pushAggregation(Aggregation aggregation) {
    if (referencesNormalizedColumn(aggregation)) {
      return false;
    }
    boolean accepted = sqlAggregates.pushAggregation(aggregation);
    if (accepted) {
      sqlOperatorSelected = true;
    }
    return accepted;
  }

  @Override
  public boolean pushTopN(SortOrder[] orders, int limit) {
    if (Arrays.stream(orders).anyMatch(this::referencesNormalizedColumn)) {
      return false;
    }
    boolean accepted = sqlTopN.pushTopN(orders, limit);
    if (accepted) {
      sqlPushedUpperBound = limit;
      sqlOperatorSelected = true;
    }
    return accepted;
  }

  @Override
  public boolean isPartiallyPushed() {
    return sqlTopN.isPartiallyPushed();
  }

  @Override
  public boolean pushOffset(int offset) {
    // Spark can invoke pushLimit/pushTopN before pushOffset. Retaining the accepted upper bound
    // allows this adapter to distinguish offset-only queries from an offset that consumes it.
    // JDBCScanBuilder subtracts OFFSET from its previously pushed LIMIT. If the result is zero or
    // negative, the MySQL dialect emits an unbounded offset-only query. Rejecting the offset keeps
    // it in Spark, where applying it above the bounded scan correctly produces an empty result.
    if (sqlPushedUpperBound != null && offset >= sqlPushedUpperBound) {
      return false;
    }
    boolean accepted = sqlOffset.pushOffset(offset);
    if (accepted) {
      if (sqlPushedUpperBound != null) {
        sqlPushedUpperBound -= offset;
      }
      sqlOperatorSelected = true;
    }
    return accepted;
  }

  private ScanBuilder selectedBuilder() {
    return normalizationRequiresSql || sqlOperatorSelected || nativeBuilder == null
        ? sqlBuilder
        : nativeBuilder;
  }

  private Predicate[] residualInInputOrder(Predicate[] predicates, Predicate[] delegateResidual) {
    return Arrays.stream(predicates)
        .filter(
            predicate ->
                referencesNormalizedColumn(predicate) || contains(delegateResidual, predicate))
        .toArray(Predicate[]::new);
  }

  private boolean referencesNormalizedColumn(Aggregation aggregation) {
    return Arrays.stream(aggregation.aggregateExpressions())
            .anyMatch(this::referencesNormalizedColumn)
        || Arrays.stream(aggregation.groupByExpressions())
            .anyMatch(this::referencesNormalizedColumn);
  }

  private boolean referencesNormalizedColumn(Expression expression) {
    for (NamedReference reference : expression.references()) {
      String[] fieldNames = reference.fieldNames();
      if (fieldNames.length > 0
          && normalizedColumns.contains(fieldNames[0].toLowerCase(Locale.ROOT))) {
        return true;
      }
    }
    return false;
  }

  private static void validateNativeBuilder(ScanBuilder builder) {
    DorisChecks.checkArgument(
        builder instanceof SupportsPushDownRequiredColumns,
        "Native Doris scan builder does not support required columns");
    DorisChecks.checkArgument(
        builder instanceof SupportsPushDownV2Filters,
        "Native Doris scan builder does not support V2 filters");
    DorisChecks.checkArgument(
        builder instanceof SupportsPushDownLimit,
        "Native Doris scan builder does not support limit");
  }

  private static void validateSqlBuilder(ScanBuilder builder) {
    DorisChecks.checkArgument(
        builder instanceof SupportsPushDownRequiredColumns,
        "Doris SQL scan builder does not support required columns");
    DorisChecks.checkArgument(
        builder instanceof SupportsPushDownV2Filters,
        "Doris SQL scan builder does not support V2 filters");
    DorisChecks.checkArgument(
        builder instanceof SupportsPushDownLimit, "Doris SQL scan builder does not support limit");
    DorisChecks.checkArgument(
        builder instanceof SupportsPushDownAggregates,
        "Doris SQL scan builder does not support aggregates");
    DorisChecks.checkArgument(
        builder instanceof SupportsPushDownTopN, "Doris SQL scan builder does not support Top-N");
    DorisChecks.checkArgument(
        builder instanceof SupportsPushDownOffset,
        "Doris SQL scan builder does not support offset");
  }

  private static boolean contains(Predicate[] predicates, Predicate target) {
    return Arrays.stream(predicates).anyMatch(target::equals);
  }

  private static Predicate[] subtract(Predicate[] predicates, Predicate[] removed) {
    List<Predicate> result = new ArrayList<>(Arrays.asList(predicates));
    for (Predicate predicate : removed) {
      result.remove(predicate);
    }
    return result.toArray(new Predicate[0]);
  }
}
