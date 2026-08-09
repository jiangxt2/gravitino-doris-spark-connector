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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.Arrays;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.connector.expressions.filter.AlwaysTrue;
import org.apache.spark.sql.connector.expressions.filter.And;
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
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;

public class TestGovernedDorisScanBuilder {

  @Test
  void testOnlyVerifiedPredicatesReachTheConnector() {
    Predicate equals = predicate("=", "id", 1);
    Predicate in =
        new Predicate(
            "IN",
            new Expression[] {
              Expressions.column("id"), Expressions.literal(1), Expressions.literal(2)
            });
    Predicate startsWith = predicate("STARTS_WITH", "label", "alp");
    Predicate endsWith = predicate("ENDS_WITH", "label", "ta");
    Predicate contains = predicate("CONTAINS", "label", "ph");
    Predicate wildcardStartsWith = predicate("STARTS_WITH", "label", "a_");
    Predicate wildcardContains = predicate("CONTAINS", "label", "%");
    Predicate escapedContains = predicate("CONTAINS", "label", "\\");
    Predicate escapedEquals = predicate("=", "label", "back\\slash");
    Predicate catalystEscapedContains = catalystStringPredicate("CONTAINS", "label", "\\");
    Predicate catalystEscapedEquals = catalystStringPredicate("=", "label", "back\\slash");
    Predicate safeAnd = new And(equals, new AlwaysTrue());
    Predicate safeStringAnd = new And(equals, startsWith);
    Predicate unknown = predicate("UNKNOWN", "id", 1);

    RecordingScanBuilder delegate = new RecordingScanBuilder();
    delegate.residual = new Predicate[] {in};
    delegate.pushed =
        new Predicate[] {equals, startsWith, endsWith, contains, safeAnd, safeStringAnd};
    GovernedDorisScanBuilder builder = new GovernedDorisScanBuilder(delegate);

    Predicate[] residual =
        builder.pushPredicates(
            new Predicate[] {
              equals,
              in,
              startsWith,
              endsWith,
              contains,
              wildcardStartsWith,
              wildcardContains,
              escapedContains,
              escapedEquals,
              catalystEscapedContains,
              catalystEscapedEquals,
              safeAnd,
              safeStringAnd,
              unknown
            });

    assertArrayEquals(
        new Predicate[] {equals, in, startsWith, endsWith, contains, safeAnd, safeStringAnd},
        delegate.receivedPredicates);
    assertArrayEquals(
        new Predicate[] {
          in,
          wildcardStartsWith,
          wildcardContains,
          escapedContains,
          escapedEquals,
          catalystEscapedContains,
          catalystEscapedEquals,
          unknown
        },
        residual);
    assertArrayEquals(delegate.pushed, builder.pushedPredicates());
  }

  @Test
  void testProjectionLimitAndBuildDelegateWithoutChangingTheirSemantics() {
    RecordingScanBuilder delegate = new RecordingScanBuilder();
    GovernedDorisScanBuilder builder = new GovernedDorisScanBuilder(delegate);
    StructType requiredSchema =
        DataTypes.createStructType(
            new org.apache.spark.sql.types.StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, true)
            });

    builder.pruneColumns(requiredSchema);

    assertSame(requiredSchema, delegate.requiredSchema);
    assertTrue(builder.pushLimit(3));
    assertSame(delegate.scan, builder.build());
  }

  @Test
  void testAggregateAndTopNDelegateWithoutWeakeningCompleteness() {
    RecordingScanBuilder delegate = new RecordingScanBuilder();
    GovernedDorisScanBuilder builder = new GovernedDorisScanBuilder(delegate);
    Aggregation aggregation = new Aggregation(new AggregateFunc[0], new Expression[0]);
    SortOrder[] orders = new SortOrder[] {mock(SortOrder.class)};

    assertTrue(builder.supportCompletePushDown(aggregation));
    assertTrue(builder.pushAggregation(aggregation));
    assertSame(aggregation, delegate.aggregation);
    assertTrue(builder.pushTopN(orders, 5));
    assertSame(orders, delegate.orders);
    assertTrue(builder.isPartiallyPushed());
  }

  @Test
  void testRejectsADelegateOutsideThePinnedConnectorContract() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new GovernedDorisScanBuilder(mock(ScanBuilder.class)));
  }

  private static Predicate predicate(String name, String column, Object value) {
    return new Predicate(
        name, new Expression[] {Expressions.column(column), Expressions.literal(value)});
  }

  private static Predicate catalystStringPredicate(String name, String column, String value) {
    return new Predicate(
        name,
        new Expression[] {
          Expressions.column(column),
          new LiteralValue<>(UTF8String.fromString(value), DataTypes.StringType)
        });
  }

  private static class RecordingScanBuilder
      implements ScanBuilder,
          SupportsPushDownRequiredColumns,
          SupportsPushDownV2Filters,
          SupportsPushDownLimit,
          SupportsPushDownAggregates,
          SupportsPushDownTopN {

    private final Scan scan = mock(Scan.class);
    private StructType requiredSchema;
    private Predicate[] receivedPredicates = new Predicate[0];
    private Predicate[] residual = new Predicate[0];
    private Predicate[] pushed = new Predicate[0];
    private Aggregation aggregation;
    private SortOrder[] orders;

    @Override
    public Scan build() {
      return scan;
    }

    @Override
    public void pruneColumns(StructType schema) {
      requiredSchema = schema;
    }

    @Override
    public Predicate[] pushPredicates(Predicate[] predicates) {
      receivedPredicates = Arrays.copyOf(predicates, predicates.length);
      return residual;
    }

    @Override
    public Predicate[] pushedPredicates() {
      return pushed;
    }

    @Override
    public boolean pushLimit(int limit) {
      return limit == 3;
    }

    @Override
    public boolean supportCompletePushDown(Aggregation pushedAggregation) {
      return true;
    }

    @Override
    public boolean pushAggregation(Aggregation pushedAggregation) {
      aggregation = pushedAggregation;
      return true;
    }

    @Override
    public boolean pushTopN(SortOrder[] pushedOrders, int limit) {
      orders = pushedOrders;
      return limit == 5;
    }

    @Override
    public boolean isPartiallyPushed() {
      return true;
    }
  }
}
