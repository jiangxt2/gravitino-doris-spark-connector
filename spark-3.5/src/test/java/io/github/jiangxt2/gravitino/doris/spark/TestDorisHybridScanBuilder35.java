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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.doris.spark.config.DorisConfig;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.NullOrdering;
import org.apache.spark.sql.connector.expressions.SortDirection;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc;
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
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

public class TestDorisHybridScanBuilder35 {

  @Test
  void testScalarScanKeepsNativeLaneAndAppliesCommonPushdownsToBothLanes() {
    RecordingNativeBuilder nativeBuilder = new RecordingNativeBuilder();
    RecordingSqlBuilder sqlBuilder = new RecordingSqlBuilder();
    DorisHybridScanBuilder35 builder =
        new DorisHybridScanBuilder35(nativeBuilder, sqlBuilder, false);
    Predicate predicate = predicate("=", "id", 1);
    StructType schema = new StructType();

    builder.pruneColumns(schema);
    assertArrayEquals(new Predicate[0], builder.pushPredicates(new Predicate[] {predicate}));

    assertSame(schema, nativeBuilder.requiredSchema);
    assertSame(schema, sqlBuilder.requiredSchema);
    assertArrayEquals(new Predicate[] {predicate}, builder.pushedPredicates());
    assertSame(nativeBuilder.scan, builder.build());
  }

  @Test
  void testGlobalLimitSelectsSqlLaneInsteadOfNativePerPartitionLimit() {
    RecordingNativeBuilder nativeBuilder = new RecordingNativeBuilder();
    RecordingSqlBuilder sqlBuilder = new RecordingSqlBuilder();
    DorisHybridScanBuilder35 builder =
        new DorisHybridScanBuilder35(nativeBuilder, sqlBuilder, false);

    assertTrue(builder.pushLimit(10));

    assertEquals(0, nativeBuilder.limit);
    assertEquals(10, sqlBuilder.limit);
    assertSame(sqlBuilder.scan, builder.build());
  }

  @Test
  void testAggregateTopNAndOffsetSelectSqlLane() {
    RecordingNativeBuilder nativeBuilder = new RecordingNativeBuilder();
    RecordingSqlBuilder aggregateSql = new RecordingSqlBuilder();
    DorisHybridScanBuilder35 aggregateBuilder =
        new DorisHybridScanBuilder35(nativeBuilder, aggregateSql, false);
    Aggregation aggregation = new Aggregation(new AggregateFunc[0], new Expression[0]);

    assertTrue(aggregateBuilder.supportCompletePushDown(aggregation));
    assertTrue(aggregateBuilder.pushAggregation(aggregation));
    assertSame(aggregation, aggregateSql.aggregation);
    assertSame(aggregateSql.scan, aggregateBuilder.build());

    RecordingSqlBuilder topNSql = new RecordingSqlBuilder();
    DorisHybridScanBuilder35 topNBuilder =
        new DorisHybridScanBuilder35(new RecordingNativeBuilder(), topNSql, false);
    SortOrder[] orders =
        new SortOrder[] {
          Expressions.sort(
              Expressions.column("id"), SortDirection.DESCENDING, NullOrdering.NULLS_LAST)
        };
    assertTrue(topNBuilder.pushTopN(orders, 5));
    assertTrue(topNBuilder.pushOffset(2));
    assertFalse(topNBuilder.isPartiallyPushed());
    assertSame(orders, topNSql.orders);
    assertSame(topNSql.scan, topNBuilder.build());
  }

  @Test
  void testRejectsOffsetThatConsumesPushedLimitOrTopN() {
    RecordingSqlBuilder limitedSql = new RecordingSqlBuilder();
    DorisHybridScanBuilder35 limitedBuilder =
        new DorisHybridScanBuilder35(new RecordingNativeBuilder(), limitedSql, false);

    assertTrue(limitedBuilder.pushLimit(3));
    assertTrue(limitedBuilder.pushOffset(2));
    assertEquals(1, limitedSql.offsetCalls);
    assertFalse(limitedBuilder.pushOffset(1));
    assertEquals(1, limitedSql.offsetCalls);

    RecordingSqlBuilder equalLimitSql = new RecordingSqlBuilder();
    DorisHybridScanBuilder35 equalLimitBuilder =
        new DorisHybridScanBuilder35(new RecordingNativeBuilder(), equalLimitSql, false);
    assertTrue(equalLimitBuilder.pushLimit(3));
    assertFalse(equalLimitBuilder.pushOffset(3));
    assertEquals(0, equalLimitSql.offsetCalls);

    RecordingSqlBuilder smallerLimitSql = new RecordingSqlBuilder();
    DorisHybridScanBuilder35 smallerLimitBuilder =
        new DorisHybridScanBuilder35(new RecordingNativeBuilder(), smallerLimitSql, false);
    assertTrue(smallerLimitBuilder.pushLimit(3));
    assertFalse(smallerLimitBuilder.pushOffset(4));
    assertEquals(0, smallerLimitSql.offsetCalls);

    RecordingSqlBuilder topNSql = new RecordingSqlBuilder();
    DorisHybridScanBuilder35 topNBuilder =
        new DorisHybridScanBuilder35(new RecordingNativeBuilder(), topNSql, false);
    SortOrder[] orders =
        new SortOrder[] {Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING)};
    assertTrue(topNBuilder.pushTopN(orders, 2));
    assertFalse(topNBuilder.pushOffset(2));
    assertEquals(0, topNSql.offsetCalls);
    assertSame(topNSql.scan, topNBuilder.build());
  }

  @Test
  void testSqlRequiredAndResidualUnionRemainFailSafe() {
    RecordingNativeBuilder nativeBuilder = new RecordingNativeBuilder();
    RecordingSqlBuilder sqlBuilder = new RecordingSqlBuilder();
    Predicate first = predicate("=", "id", 1);
    Predicate second = predicate(">", "id", 0);
    nativeBuilder.residual = new Predicate[] {first};
    sqlBuilder.residual = new Predicate[] {second};
    DorisHybridScanBuilder35 builder =
        new DorisHybridScanBuilder35(nativeBuilder, sqlBuilder, false);

    assertArrayEquals(
        new Predicate[] {first, second}, builder.pushPredicates(new Predicate[] {first, second}));
    assertArrayEquals(new Predicate[0], builder.pushedPredicates());

    RecordingSqlBuilder requiredSql = new RecordingSqlBuilder();
    DorisHybridScanBuilder35 requiredBuilder =
        new DorisHybridScanBuilder35(null, requiredSql, true);
    assertSame(requiredSql.scan, requiredBuilder.build());
  }

  @Test
  void testSpark35GovernedWrapperExposesOffset() {
    RecordingSqlBuilder delegate = new RecordingSqlBuilder();
    GovernedDorisScanBuilder35 builder = new GovernedDorisScanBuilder35(delegate);

    assertTrue(builder.pushOffset(4));
    assertEquals(4, delegate.offset);
  }

  @Test
  void testKeepsNormalizedOperatorsAsSparkResiduals() {
    RecordingNativeBuilder nativeBuilder = new RecordingNativeBuilder();
    RecordingSqlBuilder sqlBuilder = new RecordingSqlBuilder();
    DorisHybridScanBuilder35 builder =
        new DorisHybridScanBuilder35(
            nativeBuilder, sqlBuilder, true, Collections.singleton("event_time"));
    Predicate scalarPredicate = predicate("=", "id", 1);
    Predicate normalizedPredicate = predicate("=", "event_time", "2026-01-02 03:04:05");

    assertArrayEquals(
        new Predicate[] {normalizedPredicate},
        builder.pushPredicates(new Predicate[] {scalarPredicate, normalizedPredicate}));
    assertArrayEquals(new Predicate[] {scalarPredicate}, sqlBuilder.pushed);

    Aggregation aggregation =
        new Aggregation(new AggregateFunc[0], new Expression[] {Expressions.column("event_time")});
    assertFalse(builder.supportCompletePushDown(aggregation));
    assertFalse(builder.pushAggregation(aggregation));

    AggregateFunc aggregateInput = mock(AggregateFunc.class);
    when(aggregateInput.references())
        .thenReturn(
            new org.apache.spark.sql.connector.expressions.NamedReference[] {
              Expressions.column("event_time")
            });
    Aggregation aggregateOnNormalized =
        new Aggregation(new AggregateFunc[] {aggregateInput}, new Expression[0]);
    assertFalse(builder.supportCompletePushDown(aggregateOnNormalized));
    assertFalse(builder.pushAggregation(aggregateOnNormalized));
    assertFalse(
        builder.pushTopN(
            new SortOrder[] {
              Expressions.sort(Expressions.column("event_time"), SortDirection.ASCENDING)
            },
            5));
    assertSame(sqlBuilder.scan, builder.build());
  }

  @Test
  void testPrunedLosslessColumnsInMixedTableUseNativeLane() {
    RecordingNativeBuilder nativeBuilder = new RecordingNativeBuilder();
    RecordingSqlBuilder sqlBuilder = new RecordingSqlBuilder();
    DorisHybridScanBuilder35 builder =
        new DorisHybridScanBuilder35(
            nativeBuilder, sqlBuilder, true, Collections.singleton("event_time"));

    builder.pruneColumns(new StructType().add("id", DataTypes.IntegerType));

    assertSame(nativeBuilder.scan, builder.build());
  }

  @Test
  void testArrowPreferredWrapsOnlyTheSelectedNativeLane() throws Exception {
    RecordingNativeBuilder nativeBuilder = new RecordingNativeBuilder();
    RecordingSqlBuilder sqlBuilder = new RecordingSqlBuilder();
    DorisHybridScanBuilder35 nativeSelected =
        new DorisHybridScanBuilder35(
            nativeBuilder,
            sqlBuilder,
            false,
            Collections.emptySet(),
            arrowConfig(),
            true,
            "endpoint-identity",
            List.of("fe"));

    assertTrue(nativeSelected.build() instanceof DorisArrowFallbackScan35);

    DorisHybridScanBuilder35 sqlSelected =
        new DorisHybridScanBuilder35(
            new RecordingNativeBuilder(),
            sqlBuilder,
            false,
            Collections.emptySet(),
            arrowConfig(),
            true,
            "endpoint-identity",
            List.of("fe"));
    assertTrue(sqlSelected.pushLimit(10));
    assertSame(sqlBuilder.scan, sqlSelected.build());
  }

  private static Predicate predicate(String name, String column, Object value) {
    return new Predicate(
        name, new Expression[] {Expressions.column(column), Expressions.literal(value)});
  }

  private static DorisConfig arrowConfig() throws Exception {
    Map<String, String> options = new HashMap<>();
    options.put("doris.fenodes", "fe:8030");
    options.put("doris.query.port", "9030");
    options.put("doris.user", "reader");
    options.put("doris.password", "test-password");
    options.put("doris.table.identifier", "analytics.events");
    options.put("doris.read.mode", "thrift");
    options.put("doris.read.arrow-flight-sql.port", "8070");
    options.put("doris.fe.auto.fetch", "false");
    return DorisConfig.fromMap(options, false);
  }

  private static class RecordingNativeBuilder
      implements ScanBuilder,
          SupportsPushDownRequiredColumns,
          SupportsPushDownV2Filters,
          SupportsPushDownLimit {

    protected final Scan scan = mock(Scan.class);
    protected StructType requiredSchema;
    protected Predicate[] residual = new Predicate[0];
    protected Predicate[] pushed = new Predicate[0];
    protected int limit;

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
      pushed = subtract(predicates, residual);
      return residual;
    }

    @Override
    public Predicate[] pushedPredicates() {
      return pushed;
    }

    @Override
    public boolean pushLimit(int limit) {
      this.limit = limit;
      return true;
    }
  }

  private static class RecordingSqlBuilder extends RecordingNativeBuilder
      implements SupportsPushDownAggregates, SupportsPushDownTopN, SupportsPushDownOffset {

    private Aggregation aggregation;
    private SortOrder[] orders;
    private int offset;
    private int offsetCalls;

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
      return true;
    }

    @Override
    public boolean isPartiallyPushed() {
      return false;
    }

    @Override
    public boolean pushOffset(int offset) {
      this.offset = offset;
      offsetCalls++;
      return true;
    }
  }

  private static Predicate[] subtract(Predicate[] predicates, Predicate[] removed) {
    java.util.List<Predicate> result = new java.util.ArrayList<>(Arrays.asList(predicates));
    Arrays.stream(removed).forEach(result::remove);
    return result.toArray(new Predicate[0]);
  }
}
