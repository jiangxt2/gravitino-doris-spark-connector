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
import java.util.List;
import java.util.Set;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Literal;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.connector.expressions.filter.AlwaysFalse;
import org.apache.spark.sql.connector.expressions.filter.AlwaysTrue;
import org.apache.spark.sql.connector.expressions.filter.And;
import org.apache.spark.sql.connector.expressions.filter.Not;
import org.apache.spark.sql.connector.expressions.filter.Or;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownAggregates;
import org.apache.spark.sql.connector.read.SupportsPushDownLimit;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.connector.read.SupportsPushDownTopN;
import org.apache.spark.sql.connector.read.SupportsPushDownV2Filters;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/** Restricts Doris predicate pushdown to the adapter's verified semantic contract. */
public class GovernedDorisScanBuilder
    implements ScanBuilder,
        SupportsPushDownRequiredColumns,
        SupportsPushDownV2Filters,
        SupportsPushDownLimit,
        SupportsPushDownAggregates,
        SupportsPushDownTopN {

  private static final Set<String> VERIFIED_LEAF_PREDICATES =
      Set.of(
          "=",
          "!=",
          "<>",
          "<",
          "<=",
          ">",
          ">=",
          "IN",
          "IS_NULL",
          "IS_NOT_NULL",
          "STARTS_WITH",
          "ENDS_WITH",
          "CONTAINS");

  private final ScanBuilder delegate;
  private final SupportsPushDownRequiredColumns requiredColumnsDelegate;
  private final SupportsPushDownV2Filters filtersDelegate;
  private final SupportsPushDownLimit limitDelegate;
  private final SupportsPushDownAggregates aggregatesDelegate;
  private final SupportsPushDownTopN topNDelegate;

  /**
   * Creates a governed wrapper around the official Doris scan builder.
   *
   * @param delegate the official Doris scan builder
   */
  public GovernedDorisScanBuilder(ScanBuilder delegate) {
    DorisChecks.checkArgument(
        delegate instanceof SupportsPushDownRequiredColumns,
        "Doris scan builder does not support required-column pushdown");
    DorisChecks.checkArgument(
        delegate instanceof SupportsPushDownV2Filters,
        "Doris scan builder does not support V2 filter pushdown");
    DorisChecks.checkArgument(
        delegate instanceof SupportsPushDownLimit,
        "Doris scan builder does not support limit pushdown");
    DorisChecks.checkArgument(
        delegate instanceof SupportsPushDownAggregates,
        "Doris scan builder does not support aggregate pushdown");
    DorisChecks.checkArgument(
        delegate instanceof SupportsPushDownTopN,
        "Doris scan builder does not support Top-N pushdown");
    this.delegate = delegate;
    this.requiredColumnsDelegate = (SupportsPushDownRequiredColumns) delegate;
    this.filtersDelegate = (SupportsPushDownV2Filters) delegate;
    this.limitDelegate = (SupportsPushDownLimit) delegate;
    this.aggregatesDelegate = (SupportsPushDownAggregates) delegate;
    this.topNDelegate = (SupportsPushDownTopN) delegate;
  }

  @Override
  public Scan build() {
    return delegate.build();
  }

  @Override
  public void pruneColumns(StructType requiredSchema) {
    requiredColumnsDelegate.pruneColumns(requiredSchema);
  }

  @Override
  public Predicate[] pushPredicates(Predicate[] predicates) {
    List<Predicate> verified = new ArrayList<>();
    for (Predicate predicate : predicates) {
      if (isVerifiedPredicate(predicate)) {
        verified.add(predicate);
      }
    }

    List<Predicate> delegateResidual =
        new ArrayList<>(
            Arrays.asList(filtersDelegate.pushPredicates(verified.toArray(new Predicate[0]))));
    List<Predicate> residual = new ArrayList<>();
    for (Predicate predicate : predicates) {
      if (!isVerifiedPredicate(predicate) || removeFirst(delegateResidual, predicate)) {
        residual.add(predicate);
      }
    }
    return residual.toArray(new Predicate[0]);
  }

  @Override
  public Predicate[] pushedPredicates() {
    return filtersDelegate.pushedPredicates();
  }

  @Override
  public boolean pushLimit(int limit) {
    return limitDelegate.pushLimit(limit);
  }

  @Override
  public boolean supportCompletePushDown(Aggregation aggregation) {
    return aggregatesDelegate.supportCompletePushDown(aggregation);
  }

  @Override
  public boolean pushAggregation(Aggregation aggregation) {
    return aggregatesDelegate.pushAggregation(aggregation);
  }

  @Override
  public boolean pushTopN(SortOrder[] orders, int limit) {
    return topNDelegate.pushTopN(orders, limit);
  }

  @Override
  public boolean isPartiallyPushed() {
    return topNDelegate.isPartiallyPushed();
  }

  private static boolean isVerifiedPredicate(Predicate predicate) {
    if (containsBackslashStringLiteral(predicate)) {
      return false;
    }
    if (predicate instanceof AlwaysTrue || predicate instanceof AlwaysFalse) {
      return true;
    }
    if (predicate instanceof And) {
      And and = (And) predicate;
      return isVerifiedPredicate(and.left()) && isVerifiedPredicate(and.right());
    }
    if (predicate instanceof Or) {
      Or or = (Or) predicate;
      return isVerifiedPredicate(or.left()) && isVerifiedPredicate(or.right());
    }
    if (predicate instanceof Not) {
      return isVerifiedPredicate(((Not) predicate).child());
    }
    if ("STARTS_WITH".equals(predicate.name())
        || "ENDS_WITH".equals(predicate.name())
        || "CONTAINS".equals(predicate.name())) {
      return hasLikeSafeStringLiteral(predicate);
    }
    return VERIFIED_LEAF_PREDICATES.contains(predicate.name());
  }

  private static boolean hasLikeSafeStringLiteral(Predicate predicate) {
    Expression[] children = predicate.children();
    if (children.length != 2) {
      return false;
    }
    String literal = stringLiteralValue(children[1]);
    if (literal == null) {
      return false;
    }
    // Connector 26.0.0 translates these expressions to LIKE without escaping Doris LIKE
    // metacharacters. Keep such values as Spark residuals instead of changing their meaning.
    return literal.indexOf('%') < 0 && literal.indexOf('_') < 0 && literal.indexOf('\\') < 0;
  }

  private static boolean containsBackslashStringLiteral(Expression expression) {
    String literal = stringLiteralValue(expression);
    if (literal != null) {
      return literal.indexOf('\\') >= 0;
    }
    return Arrays.stream(expression.children())
        .anyMatch(GovernedDorisScanBuilder::containsBackslashStringLiteral);
  }

  private static String stringLiteralValue(Expression expression) {
    if (!(expression instanceof Literal<?>)) {
      return null;
    }
    Literal<?> literal = (Literal<?>) expression;
    Object value = literal.value();
    // Catalyst uses UTF8String-valued literals while connector-facing tests commonly use Java
    // Strings. The declared Spark data type is stable across both representations.
    return value != null && DataTypes.StringType.equals(literal.dataType())
        ? value.toString()
        : null;
  }

  private static boolean removeFirst(List<Predicate> predicates, Predicate target) {
    for (int index = 0; index < predicates.size(); index++) {
      if (predicates.get(index).equals(target)) {
        predicates.remove(index);
        return true;
      }
    }
    return false;
  }
}
