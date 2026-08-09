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

package io.github.jiangxt2.gravitino.doris.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableList;
import io.github.jiangxt2.gravitino.doris.spark.GovernedDorisSparkPlugin;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.logging.log4j.LogManager;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.CatalogManager;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** End-to-end verification against real Gravitino, Spark, and Doris components. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class GovernedDorisConnectorIT {

  private static final Logger LOG = LoggerFactory.getLogger(GovernedDorisConnectorIT.class);
  private static final String METALAKE = "doris_it";
  private static final String CATALOG = "governed_doris";
  private static final String PARTITIONED_CATALOG = "governed_doris_partitioned";
  private static final String JDBC_BASELINE_CATALOG = "direct_doris_jdbc";
  private static final String PROVIDER = "doris-governed";
  private static final String SCHEMA = "connector_it";
  private static final String COMMON_TABLE = "common_types";
  private static final String NORMALIZED_TABLE = "normalized_types";
  private static final String EXTENDED_TABLE = "extended_types";
  private static final String SKETCH_TABLE = "sketch_types";
  private static final String PARTITIONED_TABLE = "partitioned_types";
  private static final String CACHE_TABLE = "cache_table";
  private static final String FAILURE_TABLE = "failure_table";
  private static final String DENIED_TABLE = "denied_table";
  private static final String WIDE_DECIMAL_TABLE = "wide_decimal";
  private static final String READER = "doris_it_reader";
  private static final String READER_ROLE = "doris_it_reader_role";
  private static final String NULL_VALUE = "<null>";

  private DockerTestNetwork network;
  private DorisTestCluster doris;
  private RecordingDorisHttpProxy feProxy;
  private GravitinoTestServer gravitino;
  private GravitinoAdminClient adminClient;
  private GravitinoClient governedClient;
  private SparkSession spark;

  @BeforeAll
  void startInfrastructure() throws Exception {
    Path repositoryRoot =
        Paths.get(requiredSystemProperty("connector.repository.root")).toAbsolutePath();
    Path providerDirectory =
        Paths.get(requiredSystemProperty("connector.provider.directory")).toAbsolutePath();
    String version = System.getProperty("doris.version", "3.0.6.2");
    assertThat(version).isIn("3.0.6.2", "4.0.6");

    try {
      network = DockerTestNetwork.create();
      doris = new DorisTestCluster(network, version, repositoryRoot);
      doris.start();
      feProxy = RecordingDorisHttpProxy.start(doris.hostFeEndpoint());
      createPhysicalTables();

      gravitino = new GravitinoTestServer(network, providerDirectory);
      gravitino.start();
      createGovernedMetadata();
      startSpark();
    } catch (Exception | Error e) {
      closeInfrastructure();
      throw e;
    }
  }

  @AfterAll
  void closeInfrastructure() {
    if (spark != null) {
      spark.close();
      SparkSession.clearActiveSession();
      SparkSession.clearDefaultSession();
      spark = null;
    }
    closeQuietly(governedClient);
    closeQuietly(adminClient);
    closeQuietly(gravitino);
    closeQuietly(feProxy);
    closeQuietly(doris);
    closeQuietly(network);
  }

  @Test
  void readsCommonTypesThroughNativeLaneWithVendedCredentials() throws Exception {
    Catalog catalog = governedClient.loadCatalog(CATALOG);
    assertThat(catalog.properties()).doesNotContainKeys("jdbc-user", "jdbc-password");

    Dataset<Row> dataFrame = spark.table(qualified(CATALOG, COMMON_TABLE));
    StructType schema = dataFrame.schema();
    assertThat(schema.fieldNames())
        .containsExactly(
            "id",
            "bool_col",
            "tiny_col",
            "small_col",
            "big_col",
            "float_col",
            "double_col",
            "amount",
            "event_date",
            "code",
            "label",
            "description");
    assertThat(schema.apply("id").dataType()).isEqualTo(DataTypes.IntegerType);
    assertThat(schema.apply("amount").dataType()).isEqualTo(DataTypes.createDecimalType(18, 3));
    assertThat(schema.apply("event_date").dataType()).isEqualTo(DataTypes.DateType);
    assertThat(schema.apply("code").dataType()).isEqualTo(DataTypes.StringType);

    String sparkSql =
        "SELECT id, bool_col, tiny_col, small_col, big_col, float_col, double_col, "
            + "amount, event_date, code, label, description FROM "
            + qualified(CATALOG, COMMON_TABLE)
            + " ORDER BY id";
    String directSql =
        "SELECT id, bool_col, tiny_col, small_col, big_col, float_col, double_col, "
            + "amount, event_date, code, label, description FROM `"
            + SCHEMA
            + "`.`"
            + COMMON_TABLE
            + "` ORDER BY id";
    assertThat(sparkRows(spark.sql(sparkSql))).containsExactlyElementsOf(jdbcRows(directSql));

    Dataset<Row> pushed =
        spark.sql(
            "SELECT id, label FROM "
                + qualified(CATALOG, COMMON_TABLE)
                + " WHERE id >= 2 AND id <= 3");
    assertThat(sparkRows(pushed)).containsExactly("2,beta", "3,alphabet");
    String plan = pushed.queryExecution().executedPlan().toString();
    assertThat(plan).contains("DorisScanV2").doesNotContain("JDBCRelation");
  }

  @Test
  void pushesAggregateAndTopNWithoutLosingLimitOffsetSemantics() throws Exception {
    Dataset<Row> globalLimit =
        spark.sql("SELECT id FROM " + qualified(CATALOG, COMMON_TABLE) + " LIMIT 2");
    assertThat(globalLimit.collectAsList()).hasSize(2);
    assertThat(globalLimit.queryExecution().executedPlan().toString())
        .contains("JDBCScan")
        .doesNotContain("DorisScanV2");

    String aggregate =
        "SELECT code, COUNT(*), SUM(amount), MIN(amount), MAX(amount), AVG(amount) FROM "
            + qualified(CATALOG, COMMON_TABLE)
            + " GROUP BY code";
    Dataset<Row> aggregateFrame = spark.sql(aggregate);
    List<String> expectedAggregate =
        jdbcRows(
            "SELECT code, COUNT(*), SUM(amount), MIN(amount), MAX(amount), AVG(amount) "
                + "FROM `"
                + SCHEMA
                + "`.`"
                + COMMON_TABLE
                + "` GROUP BY code");
    assertThat(sorted(canonicalAggregateRows(sparkRows(aggregateFrame))))
        .isEqualTo(sorted(canonicalAggregateRows(expectedAggregate)));
    String aggregatePlan = aggregateFrame.queryExecution().executedPlan().toString();
    assertThat(aggregatePlan).contains("PushedAggregates");
    assertThat(aggregatePlan).doesNotContain("HashAggregate").doesNotContain("SortAggregate");

    String topN =
        "SELECT id, amount FROM "
            + qualified(CATALOG, COMMON_TABLE)
            + " ORDER BY id DESC LIMIT 3 OFFSET 2";
    Dataset<Row> topNFrame = spark.sql(topN);
    assertThat(sparkRows(topNFrame))
        .containsExactlyElementsOf(
            jdbcRows(
                "SELECT id, amount FROM `"
                    + SCHEMA
                    + "`.`"
                    + COMMON_TABLE
                    + "` ORDER BY id DESC LIMIT 3 OFFSET 2"));
    String topNPlan = topNFrame.queryExecution().executedPlan().toString();
    assertThat(topNPlan)
        .doesNotContain("TakeOrderedAndProject")
        .doesNotContain("CollectLimit")
        .doesNotContain("GlobalLimit")
        .doesNotContain("LocalLimit");

    Dataset<Row> bounded =
        spark.table(qualified(CATALOG, COMMON_TABLE)).select("id").orderBy("id").limit(3);
    assertThat(bounded.offset(2).collectAsList()).hasSize(1);
    assertThat(bounded.offset(3).collectAsList()).isEmpty();
    assertThat(bounded.offset(4).collectAsList()).isEmpty();
  }

  @Test
  void preservesDatetimeAndComplexValuesAsStrings() throws Exception {
    Dataset<Row> normalized = spark.table(qualified(CATALOG, NORMALIZED_TABLE));
    assertThat(normalized.schema().apply("id").dataType()).isEqualTo(DataTypes.IntegerType);
    Arrays.stream(normalized.schema().fields())
        .filter(field -> !"id".equals(field.name()))
        .forEach(field -> assertThat(field.dataType()).isEqualTo(DataTypes.StringType));

    String query =
        "SELECT id, event_time, array_col, map_col, struct_col, json_col, large_col FROM "
            + qualified(CATALOG, NORMALIZED_TABLE)
            + " ORDER BY id";
    String direct =
        "SELECT id, event_time, array_col, map_col, struct_col, json_col, large_col FROM `"
            + SCHEMA
            + "`.`"
            + NORMALIZED_TABLE
            + "` ORDER BY id";
    spark.conf().set("spark.sql.session.timeZone", "UTC");
    List<String> utc = sparkRows(spark.sql(query));
    spark.conf().set("spark.sql.session.timeZone", "Asia/Shanghai");
    List<String> shanghai = sparkRows(spark.sql(query));
    spark.conf().set("spark.sql.session.timeZone", "UTC");
    assertThat(utc).isEqualTo(shanghai).isEqualTo(jdbcRows(direct));

    Dataset<Row> extended = spark.table(qualified(CATALOG, EXTENDED_TABLE));
    assertThat(extended.schema().apply("id").dataType()).isEqualTo(DataTypes.IntegerType);
    assertThat(extended.schema().apply("variant_col").dataType()).isEqualTo(DataTypes.StringType);
    assertThat(extended.schema().apply("ipv4_col").dataType()).isEqualTo(DataTypes.StringType);
    assertThat(extended.schema().apply("ipv6_col").dataType()).isEqualTo(DataTypes.StringType);
    assertThat(
            sparkRows(
                spark.sql(
                    "SELECT id, variant_col, ipv4_col, ipv6_col FROM "
                        + qualified(CATALOG, EXTENDED_TABLE)
                        + " ORDER BY id")))
        .containsExactlyElementsOf(
            jdbcRows(
                "SELECT id, variant_col, ipv4_col, ipv6_col FROM `"
                    + SCHEMA
                    + "`.`"
                    + EXTENDED_TABLE
                    + "` ORDER BY id"));

    Dataset<Row> ordered =
        spark.sql(
            "SELECT id, event_time FROM "
                + qualified(CATALOG, NORMALIZED_TABLE)
                + " ORDER BY event_time");
    assertThat(ordered.queryExecution().executedPlan().toString()).contains("Sort");

    Dataset<Row> sketch = spark.table(qualified(CATALOG, SKETCH_TABLE));
    assertThat(sketch.schema().apply("bitmap_col").dataType()).isEqualTo(DataTypes.StringType);
    assertThat(sketch.schema().apply("hll_col").dataType()).isEqualTo(DataTypes.StringType);
    assertThat(sparkRows(sketch.orderBy("id")))
        .containsExactlyElementsOf(
            jdbcRows(
                "SELECT id, BITMAP_TO_BASE64(bitmap_col), HLL_TO_BASE64(hll_col) FROM `"
                    + SCHEMA
                    + "`.`"
                    + SKETCH_TABLE
                    + "` ORDER BY id"));

    if (doris.version().startsWith("4.")) {
      StructType wideSchema = spark.table(qualified(CATALOG, WIDE_DECIMAL_TABLE)).schema();
      assertThat(wideSchema.apply("wide_value").dataType()).isEqualTo(DataTypes.StringType);
      assertThat(
              sparkRows(
                  spark.sql(
                      "SELECT id, wide_value FROM "
                          + qualified(CATALOG, WIDE_DECIMAL_TABLE)
                          + " ORDER BY id")))
          .containsExactly("1,1234567890123456789012345678901234567890.123456");
    }
  }

  @Test
  void matchesDorisForStringPredicateBoundaryValues() throws Exception {
    Map<String, List<String>> predicates = new LinkedHashMap<>();
    predicates.put("label LIKE '%a\\\\_b%'", Collections.singletonList("6"));
    predicates.put("label LIKE '%a\\\\%b%'", Collections.singletonList("7"));
    predicates.put("label = 'quo\\'te'", Collections.singletonList("8"));
    predicates.put("HEX(label) = '6261636B5C736C617368'", Collections.singletonList("9"));
    predicates.put("label = '数据'", Collections.singletonList("4"));
    predicates.put("label = ''", Collections.singletonList("10"));
    predicates.put("label IS NULL", Collections.singletonList("5"));

    for (Map.Entry<String, List<String>> entry : predicates.entrySet()) {
      String predicate = entry.getKey();
      List<String> direct =
          jdbcRows(
              "SELECT id FROM `"
                  + SCHEMA
                  + "`.`"
                  + COMMON_TABLE
                  + "` WHERE "
                  + predicate
                  + " ORDER BY id");
      List<String> governed =
          sparkRows(
              spark.sql(
                  "SELECT id FROM "
                      + qualified(CATALOG, COMMON_TABLE)
                      + " WHERE "
                      + predicate
                      + " ORDER BY id"));
      assertThat(direct).as(predicate).isEqualTo(entry.getValue());
      assertThat(governed).as(predicate).isEqualTo(entry.getValue());
    }

    Map<Dataset<Row>, List<String>> pushedStringPredicates = new LinkedHashMap<>();
    Dataset<Row> startsWith =
        spark
            .table(qualified(CATALOG, COMMON_TABLE))
            .filter(org.apache.spark.sql.functions.col("label").startsWith("alp"))
            .select("id");
    Dataset<Row> endsWith =
        spark
            .table(qualified(CATALOG, COMMON_TABLE))
            .filter(org.apache.spark.sql.functions.col("label").endsWith("ta"))
            .select("id");
    Dataset<Row> contains =
        spark
            .table(qualified(CATALOG, COMMON_TABLE))
            .filter(org.apache.spark.sql.functions.col("label").contains("pha"))
            .select("id");
    Dataset<Row> quotedEquals =
        spark
            .table(qualified(CATALOG, COMMON_TABLE))
            .filter(org.apache.spark.sql.functions.col("label").equalTo("quo'te"))
            .select("id");
    pushedStringPredicates.put(startsWith, Arrays.asList("1", "3"));
    pushedStringPredicates.put(endsWith, Collections.singletonList("2"));
    pushedStringPredicates.put(contains, Arrays.asList("1", "3"));
    pushedStringPredicates.put(quotedEquals, Collections.singletonList("8"));
    pushedStringPredicates.forEach(
        (frame, expected) -> {
          assertThat(sorted(sparkRows(frame))).isEqualTo(expected);
          assertThat(frame.queryExecution().executedPlan().toString())
              .contains("DorisScanV2")
              .doesNotContain("JDBCRelation");
        });

    Map<Dataset<Row>, List<String>> residualStringPredicates = new LinkedHashMap<>();
    residualStringPredicates.put(
        spark
            .table(qualified(CATALOG, COMMON_TABLE))
            .filter(org.apache.spark.sql.functions.col("label").startsWith("a_"))
            .select("id"),
        Collections.singletonList("6"));
    residualStringPredicates.put(
        spark
            .table(qualified(CATALOG, COMMON_TABLE))
            .filter(org.apache.spark.sql.functions.col("label").contains("%"))
            .select("id"),
        Collections.singletonList("7"));
    residualStringPredicates.put(
        spark
            .table(qualified(CATALOG, COMMON_TABLE))
            .filter(org.apache.spark.sql.functions.col("label").contains("\\"))
            .select("id"),
        Collections.singletonList("9"));
    residualStringPredicates.put(
        spark
            .table(qualified(CATALOG, COMMON_TABLE))
            .filter(org.apache.spark.sql.functions.col("label").equalTo("back\\slash"))
            .select("id"),
        Collections.singletonList("9"));
    residualStringPredicates.forEach(
        (frame, expected) -> {
          assertThat(sparkRows(frame))
              .as(frame.queryExecution().executedPlan().toString())
              .isEqualTo(expected);
          assertThat(frame.queryExecution().executedPlan().toString()).contains("Filter");
        });
  }

  @Test
  void partitionedSqlLaneMatchesDirectSparkJdbc() throws Exception {
    Dataset<Row> governed =
        spark.table(qualified(PARTITIONED_CATALOG, PARTITIONED_TABLE)).select("id", "event_time");
    Dataset<Row> direct = directPartitionedFrame();

    assertThat(governed.rdd().getNumPartitions()).isEqualTo(direct.rdd().getNumPartitions());
    assertThat(governed.rdd().getNumPartitions()).isEqualTo(4);
    assertThat(governed.schema().apply("event_time").dataType()).isEqualTo(DataTypes.StringType);
    assertThat(direct.schema().apply("event_time").dataType()).isEqualTo(DataTypes.TimestampType);
    assertThat(sorted(sparkRows(governed.select("id"))))
        .isEqualTo(sorted(sparkRows(direct.select("id"))));
    assertThat(sorted(sparkRows(governed)))
        .isEqualTo(
            sorted(
                jdbcRows(
                    "SELECT id, event_time FROM `" + SCHEMA + "`.`" + PARTITIONED_TABLE + "`")));
  }

  @Test
  void comparableSqlLaneHasTheSamePlannerAndNoMaterialTimingRegression() {
    Dataset<Row> governed =
        spark
            .table(qualified(PARTITIONED_CATALOG, PARTITIONED_TABLE))
            .agg(org.apache.spark.sql.functions.sum("id"));
    Dataset<Row> direct = directPartitionedFrame().agg(org.apache.spark.sql.functions.sum("id"));

    assertThat(sparkRows(governed)).isEqualTo(sparkRows(direct));
    assertThat(governed.queryExecution().executedPlan().toString()).contains("PushedAggregates");
    assertThat(direct.queryExecution().executedPlan().toString())
        .contains("PushedAggregates")
        .doesNotContain("JDBCRelation");

    governed.collectAsList();
    direct.collectAsList();
    List<Long> governedMillis = new ArrayList<>();
    List<Long> directMillis = new ArrayList<>();
    for (int run = 0; run < 5; run++) {
      if (run % 2 == 0) {
        governedMillis.add(measureMillis(governed));
        directMillis.add(measureMillis(direct));
      } else {
        directMillis.add(measureMillis(direct));
        governedMillis.add(measureMillis(governed));
      }
    }
    long governedMedian = median(governedMillis);
    long directMedian = median(directMillis);
    LOG.info(
        "Doris {} SQL-lane performance probe: governed={} ms, direct JDBC={} ms",
        doris.version(),
        governedMedian,
        directMedian);
    assertThat(governedMedian)
        .as("governed SQL-lane median must stay within 50%% plus 1000 ms of direct JDBC")
        .isLessThanOrEqualTo((directMedian * 3 / 2) + 1000);
  }

  @Test
  void deniesSelectBeforeAnyDorisRequest() {
    org.apache.spark.sql.connector.catalog.TableCatalog sparkCatalog = sparkCatalog(CATALOG);
    feProxy.reset();
    assertThatThrownBy(
            () -> sparkCatalog.loadTable(Identifier.of(new String[] {SCHEMA}, DENIED_TABLE)))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("not authorized")
        .hasMessageContaining("loadTable");
    assertThat(feProxy.totalRequestCount()).isZero();

    feProxy.reset();
    assertThatThrownBy(
            () -> spark.sql("SELECT * FROM " + qualified(CATALOG, DENIED_TABLE)).collectAsList())
        .hasMessageContaining("not authorized")
        .hasMessageContaining("loadTable");
    assertThat(feProxy.totalRequestCount()).isZero();
  }

  @Test
  @SuppressWarnings("deprecation")
  void cachesOnePhysicalSchemaSnapshotAndRefreshesPrecisely() throws Exception {
    org.apache.spark.sql.connector.catalog.TableCatalog sparkCatalog = sparkCatalog(CATALOG);
    Identifier identifier = Identifier.of(new String[] {SCHEMA}, CACHE_TABLE);
    String path = schemaPath(CACHE_TABLE);
    sparkCatalog.invalidateTable(identifier);
    feProxy.reset();

    Table first = sparkCatalog.loadTable(identifier);
    first.schema();
    sparkCatalog.loadTable(identifier).schema();
    assertThat(feProxy.requestCount("GET", path)).isEqualTo(1);

    sparkCatalog.invalidateTable(identifier);
    sparkCatalog.loadTable(identifier).schema();
    assertThat(feProxy.requestCount("GET", path)).isEqualTo(2);

    String failurePath = schemaPath(FAILURE_TABLE);
    Identifier failureIdentifier = Identifier.of(new String[] {SCHEMA}, FAILURE_TABLE);
    sparkCatalog.invalidateTable(failureIdentifier);
    feProxy.reset();
    feProxy.failNextRequest(failurePath, 503, "credential-bearing third-party failure");
    assertThatThrownBy(() -> sparkCatalog.loadTable(failureIdentifier))
        .hasMessageContaining("authorized table")
        .hasMessageNotContaining("credential-bearing")
        .hasMessageNotContaining(DorisTestCluster.TEST_PASSWORD);
    assertThat(feProxy.requestCount("GET", failurePath)).isEqualTo(1);
  }

  @Test
  void exposesOnlyBatchReadAndKeepsCredentialsOutOfPlansAndLogs() {
    org.apache.spark.sql.connector.catalog.TableCatalog catalog = sparkCatalog(CATALOG);
    Table table;
    try {
      table = catalog.loadTable(Identifier.of(new String[] {SCHEMA}, COMMON_TABLE));
    } catch (Exception e) {
      throw new AssertionError(e);
    }
    assertThat(table.capabilities()).containsExactly(TableCapability.BATCH_READ);
    assertThat(table.properties()).doesNotContainKeys("jdbc-user", "jdbc-password");

    assertThatThrownBy(
            () ->
                spark
                    .sql("INSERT INTO " + qualified(CATALOG, COMMON_TABLE) + " (id) VALUES (999)")
                    .collectAsList())
        .hasMessageContaining("read-only");
    assertThatThrownBy(
            () ->
                spark
                    .sql(
                        "CREATE TABLE "
                            + CATALOG
                            + "."
                            + SCHEMA
                            + ".blocked (id INT) USING parquet")
                    .collectAsList())
        .hasMessageContaining("read-only");

    String logs;
    try (InMemoryLogCapture capture = InMemoryLogCapture.start(LogManager.ROOT_LOGGER_NAME)) {
      LOG.info("governed-doris-driver-sentinel");
      spark
          .sql("SELECT id FROM " + qualified(CATALOG, COMMON_TABLE) + " WHERE id = 1")
          .foreachPartition(
              rows -> {
                LOG.info("governed-doris-executor-sentinel");
                while (rows.hasNext()) {
                  rows.next();
                }
              });
      spark
          .sql("SELECT COUNT(*), SUM(amount) FROM " + qualified(CATALOG, COMMON_TABLE))
          .collectAsList();
      spark.sql("SELECT * FROM " + qualified(CATALOG, NORMALIZED_TABLE)).collectAsList();
      logs = String.join("\n", capture.renderedEvents());
    }
    assertThat(logs)
        .contains("governed-doris-driver-sentinel")
        .contains("governed-doris-executor-sentinel")
        .doesNotContain(DorisTestCluster.TEST_USER)
        .doesNotContain(DorisTestCluster.TEST_PASSWORD)
        .doesNotContain("doris.password")
        .doesNotContain("jdbc-password");

    for (String query :
        Arrays.asList(
            "SELECT id FROM " + qualified(CATALOG, COMMON_TABLE) + " WHERE id = 1",
            "SELECT COUNT(*), SUM(amount) FROM " + qualified(CATALOG, COMMON_TABLE),
            "SELECT * FROM " + qualified(CATALOG, NORMALIZED_TABLE))) {
      String explain =
          sparkRows(spark.sql("EXPLAIN FORMATTED " + query)).stream()
              .collect(Collectors.joining("\n"));
      assertThat(explain)
          .doesNotContain(DorisTestCluster.TEST_USER)
          .doesNotContain(DorisTestCluster.TEST_PASSWORD)
          .doesNotContain("doris.password")
          .doesNotContain("jdbc-password");
    }
  }

  private void createPhysicalTables() throws Exception {
    try (Connection connection =
            DriverManager.getConnection(
                doris.hostJdbcUrl(), DorisTestCluster.TEST_USER, DorisTestCluster.TEST_PASSWORD);
        Statement statement = connection.createStatement()) {
      statement.execute("DROP DATABASE IF EXISTS `" + SCHEMA + "`");
      statement.execute("CREATE DATABASE `" + SCHEMA + "`");
      statement.execute(
          "CREATE TABLE `"
              + SCHEMA
              + "`.`"
              + COMMON_TABLE
              + "` (id INT NOT NULL, bool_col BOOLEAN, tiny_col TINYINT, "
              + "small_col SMALLINT, big_col BIGINT, float_col FLOAT, double_col DOUBLE, "
              + "amount DECIMAL(18,3), event_date DATE, code CHAR(2), label VARCHAR(64), "
              + "description STRING) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 4 "
              + "PROPERTIES ('replication_num'='1')");
      statement.executeUpdate(
          "INSERT INTO `"
              + SCHEMA
              + "`.`"
              + COMMON_TABLE
              + "` VALUES "
              + "(1, TRUE, 7, 300, 10000000000, 1.25, 2.5, 123.456, "
              + "'2026-01-02', 'A1', 'alpha', 'first row'), "
              + "(2, FALSE, -8, -300, -10000000000, -1.25, -2.5, 42.000, "
              + "'2026-02-03', 'B2', 'beta', NULL), "
              + "(3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, "
              + "'C3', 'alphabet', NULL), "
              + "(4, TRUE, 127, 32767, 9223372036854775807, 3.5, 4.5, "
              + "999999999999999.999, '9999-12-31', '', '数据', ''), "
              + "(5, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)");
      try (PreparedStatement insert =
          connection.prepareStatement(
              "INSERT INTO `"
                  + SCHEMA
                  + "`.`"
                  + COMMON_TABLE
                  + "` (id, label, description) VALUES (?, ?, ?)")) {
        insertBoundaryRow(insert, 6, "a_b", "literal underscore");
        insertBoundaryRow(insert, 7, "a%b", "literal percent");
        insertBoundaryRow(insert, 8, "quo'te", "literal quote");
        insertBoundaryRow(insert, 9, "back\\slash", "literal backslash");
        insertBoundaryRow(insert, 10, "", "empty label");
      }

      statement.execute(
          "CREATE TABLE `"
              + SCHEMA
              + "`.`"
              + NORMALIZED_TABLE
              + "` (id INT NOT NULL, event_time DATETIME(6), array_col ARRAY<INT>, "
              + "map_col MAP<STRING, INT>, struct_col STRUCT<name:STRING, score:INT>, "
              + "json_col JSON, large_col LARGEINT) DUPLICATE KEY(id) "
              + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num'='1')");
      statement.executeUpdate(
          "INSERT INTO `"
              + SCHEMA
              + "`.`"
              + NORMALIZED_TABLE
              + "` VALUES (1, '2026-04-05 06:07:08.123456', [1, 2], "
              + "{'a':1, 'b':2}, {'alice', 9}, '{\"k\":1}', "
              + "170141183460469231731687303715884105727), "
              + "(2, NULL, NULL, NULL, NULL, NULL, NULL)");

      statement.execute(
          "CREATE TABLE `"
              + SCHEMA
              + "`.`"
              + EXTENDED_TABLE
              + "` (id INT NOT NULL, variant_col VARIANT, ipv4_col IPV4, ipv6_col IPV6) "
              + "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 "
              + "PROPERTIES ('replication_num'='1')");
      statement.executeUpdate(
          "INSERT INTO `"
              + SCHEMA
              + "`.`"
              + EXTENDED_TABLE
              + "` VALUES (1, '{\"b\":2,\"a\":1}', '192.168.1.7', '2001:db8::7'), "
              + "(2, NULL, NULL, NULL)");

      statement.execute(
          "CREATE TABLE `"
              + SCHEMA
              + "`.`"
              + SKETCH_TABLE
              + "` (id INT NOT NULL, bitmap_col BITMAP BITMAP_UNION, hll_col HLL HLL_UNION) "
              + "AGGREGATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 "
              + "PROPERTIES ('replication_num'='1')");
      statement.executeUpdate(
          "INSERT INTO `"
              + SCHEMA
              + "`.`"
              + SKETCH_TABLE
              + "` SELECT 1, TO_BITMAP(7), HLL_HASH('alpha')");

      createSimpleTable(statement, CACHE_TABLE);
      createSimpleTable(statement, FAILURE_TABLE);
      createSimpleTable(statement, DENIED_TABLE);
      statement.executeUpdate("INSERT INTO `" + SCHEMA + "`.`" + CACHE_TABLE + "` VALUES (1)");
      statement.executeUpdate("INSERT INTO `" + SCHEMA + "`.`" + FAILURE_TABLE + "` VALUES (1)");
      statement.executeUpdate("INSERT INTO `" + SCHEMA + "`.`" + DENIED_TABLE + "` VALUES (1)");

      statement.execute(
          "CREATE TABLE `"
              + SCHEMA
              + "`.`"
              + PARTITIONED_TABLE
              + "` (id INT NOT NULL, event_time DATETIME(6)) DUPLICATE KEY(id) "
              + "DISTRIBUTED BY HASH(id) BUCKETS 4 PROPERTIES ('replication_num'='1')");
      insertPartitionedRows(statement);

      if (doris.version().startsWith("4.")) {
        statement.execute("SET enable_decimal256 = true");
        statement.execute(
            "CREATE TABLE `"
                + SCHEMA
                + "`.`"
                + WIDE_DECIMAL_TABLE
                + "` (id INT NOT NULL, wide_value DECIMAL(76,6)) DUPLICATE KEY(id) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num'='1')");
        statement.executeUpdate(
            "INSERT INTO `"
                + SCHEMA
                + "`.`"
                + WIDE_DECIMAL_TABLE
                + "` VALUES (1, 1234567890123456789012345678901234567890.123456)");
      }
    }
  }

  private void createGovernedMetadata() {
    adminClient =
        GravitinoAdminClient.builder(gravitino.uri())
            .withSimpleAuth(GravitinoTestServer.ADMIN_USER)
            .build();
    GravitinoMetalake metalake =
        adminClient.createMetalake(
            METALAKE, "Governed Doris connector integration tests", Map.of());
    governedClient =
        GravitinoClient.builder(gravitino.uri())
            .withMetalake(METALAKE)
            .withSimpleAuth(GravitinoTestServer.ADMIN_USER)
            .build();

    Catalog catalog =
        metalake.createCatalog(
            CATALOG,
            Catalog.Type.RELATIONAL,
            PROVIDER,
            "Governed Doris integration catalog",
            catalogProperties(false));
    Catalog partitionedCatalog =
        metalake.createCatalog(
            PARTITIONED_CATALOG,
            Catalog.Type.RELATIONAL,
            PROVIDER,
            "Governed Doris partitioned SQL-lane catalog",
            catalogProperties(true));
    catalog.asSchemas().loadSchema(SCHEMA);
    partitionedCatalog.asSchemas().loadSchema(SCHEMA);

    List<String> mainTables =
        new ArrayList<>(
            Arrays.asList(
                COMMON_TABLE,
                NORMALIZED_TABLE,
                EXTENDED_TABLE,
                SKETCH_TABLE,
                CACHE_TABLE,
                FAILURE_TABLE,
                DENIED_TABLE));
    if (doris.version().startsWith("4.")) {
      mainTables.add(WIDE_DECIMAL_TABLE);
    }
    TableCatalog mainTableCatalog = catalog.asTableCatalog();
    mainTables.forEach(name -> mainTableCatalog.loadTable(NameIdentifier.of(SCHEMA, name)));
    partitionedCatalog.asTableCatalog().loadTable(NameIdentifier.of(SCHEMA, PARTITIONED_TABLE));

    metalake.addUser(READER);
    List<SecurableObject> grants = new ArrayList<>();
    addReadGrants(grants, CATALOG, mainTables, DENIED_TABLE);
    addReadGrants(grants, PARTITIONED_CATALOG, Collections.singletonList(PARTITIONED_TABLE), null);
    metalake.createRole(READER_ROLE, new HashMap<>(), grants);
    metalake.grantRolesToUser(ImmutableList.of(READER_ROLE), READER);
  }

  private Map<String, String> catalogProperties(boolean partitioned) {
    Map<String, String> properties = new HashMap<>();
    properties.put("jdbc-url", doris.internalJdbcUrl());
    properties.put("jdbc-driver", "com.mysql.cj.jdbc.Driver");
    properties.put("jdbc-user", DorisTestCluster.TEST_USER);
    properties.put("jdbc-password", DorisTestCluster.TEST_PASSWORD);
    properties.put("doris-fenodes", feProxy.endpoint());
    properties.put("doris-query-port", Integer.toString(doris.hostMysqlPort()));
    properties.put("credential-providers", "jdbc-user-password");
    properties.put("spark.bypass.doris.filter.query.in.max.count", "3");
    properties.put("doris-schema-cache-ttl-ms", "60000");
    properties.put("doris-schema-cache-max-entries", "64");
    if (partitioned) {
      properties.put("doris-jdbc-partition-column", "id");
      properties.put("doris-jdbc-lower-bound", "1");
      properties.put("doris-jdbc-upper-bound", "10001");
      properties.put("doris-jdbc-num-partitions", "4");
      properties.put("doris-jdbc-fetch-size", "32");
    }
    return properties;
  }

  private void startSpark() {
    String configuredReader = System.getenv("SPARK_USER");
    String jdbcCatalogPrefix = "spark.sql.catalog." + JDBC_BASELINE_CATALOG;
    assertThat(configuredReader)
        .as("integrationTest must set SPARK_USER before SparkContext starts")
        .isEqualTo(READER);
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("gravitino-doris-governed-it-" + doris.version())
            .config("spark.ui.enabled", "false")
            .config("spark.plugins", GovernedDorisSparkPlugin.class.getName())
            .config("spark.sql.gravitino.uri", gravitino.uri())
            .config("spark.sql.gravitino.metalake", METALAKE)
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "2")
            .config(
                jdbcCatalogPrefix,
                "org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog")
            .config(jdbcCatalogPrefix + ".url", doris.internalJdbcUrl())
            .config(jdbcCatalogPrefix + ".driver", "com.mysql.cj.jdbc.Driver")
            .config(jdbcCatalogPrefix + ".user", DorisTestCluster.TEST_USER)
            .config(jdbcCatalogPrefix + ".password", DorisTestCluster.TEST_PASSWORD)
            .config(jdbcCatalogPrefix + ".partitionColumn", "id")
            .config(jdbcCatalogPrefix + ".lowerBound", "1")
            .config(jdbcCatalogPrefix + ".upperBound", "10001")
            .config(jdbcCatalogPrefix + ".numPartitions", "4")
            .config(jdbcCatalogPrefix + ".fetchsize", "32")
            .getOrCreate();
    assertThat(spark.sparkContext().sparkUser()).isEqualTo(READER);
  }

  private org.apache.spark.sql.connector.catalog.TableCatalog sparkCatalog(String name) {
    CatalogManager manager = spark.sessionState().catalogManager();
    return (org.apache.spark.sql.connector.catalog.TableCatalog) manager.catalog(name);
  }

  private Dataset<Row> directPartitionedFrame() {
    return spark
        .table(JDBC_BASELINE_CATALOG + "." + SCHEMA + "." + PARTITIONED_TABLE)
        .select("id", "event_time");
  }

  private static void addReadGrants(
      List<SecurableObject> grants, String catalogName, List<String> tables, String excludedTable) {
    SecurableObject catalog =
        SecurableObjects.ofCatalog(catalogName, ImmutableList.of(Privileges.UseCatalog.allow()));
    SecurableObject schema =
        SecurableObjects.ofSchema(catalog, SCHEMA, ImmutableList.of(Privileges.UseSchema.allow()));
    grants.add(catalog);
    grants.add(schema);
    tables.stream()
        .filter(name -> !name.equals(excludedTable))
        .map(
            name ->
                SecurableObjects.ofTable(
                    schema, name, ImmutableList.of(Privileges.SelectTable.allow())))
        .forEach(grants::add);
  }

  private List<String> jdbcRows(String sql) throws Exception {
    List<String> rows = new ArrayList<>();
    try (Connection connection =
            DriverManager.getConnection(
                doris.hostJdbcUrl(), DorisTestCluster.TEST_USER, DorisTestCluster.TEST_PASSWORD);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql)) {
      ResultSetMetaData metadata = resultSet.getMetaData();
      while (resultSet.next()) {
        List<String> values = new ArrayList<>(metadata.getColumnCount());
        for (int index = 1; index <= metadata.getColumnCount(); index++) {
          String value = resultSet.getString(index);
          boolean booleanColumn =
              "BOOLEAN".equalsIgnoreCase(metadata.getColumnTypeName(index))
                  || Boolean.class.getName().equals(metadata.getColumnClassName(index))
                  || "bool_col".equalsIgnoreCase(metadata.getColumnLabel(index));
          if (value != null && booleanColumn) {
            value = Boolean.toString(resultSet.getBoolean(index));
          }
          values.add(value == null ? NULL_VALUE : value);
        }
        rows.add(String.join(",", values));
      }
    }
    return rows;
  }

  private static List<String> sparkRows(Dataset<Row> frame) {
    return frame.collectAsList().stream()
        .map(GovernedDorisConnectorIT::renderRow)
        .collect(Collectors.toList());
  }

  private static String renderRow(Row row) {
    List<String> values = new ArrayList<>(row.size());
    for (int index = 0; index < row.size(); index++) {
      Object value = row.get(index);
      if (value == null) {
        values.add(NULL_VALUE);
      } else if (value instanceof byte[]) {
        values.add(Base64.getEncoder().encodeToString((byte[]) value));
      } else if (value instanceof BigDecimal) {
        values.add(((BigDecimal) value).toPlainString());
      } else if (value instanceof scala.math.BigDecimal) {
        values.add(((scala.math.BigDecimal) value).bigDecimal().toPlainString());
      } else {
        values.add(value.toString());
      }
    }
    return String.join(",", values);
  }

  private static List<String> sorted(List<String> rows) {
    return rows.stream().sorted().collect(Collectors.toList());
  }

  private static List<String> canonicalAggregateRows(List<String> rows) {
    return rows.stream()
        .map(
            row -> {
              String[] values = row.split(",", -1);
              if (values.length != 6) {
                throw new IllegalArgumentException("Unexpected aggregate row shape: " + row);
              }
              for (int index = 2; index < values.length; index++) {
                if (!NULL_VALUE.equals(values[index])) {
                  values[index] =
                      new BigDecimal(values[index]).stripTrailingZeros().toPlainString();
                }
              }
              return String.join(",", values);
            })
        .collect(Collectors.toList());
  }

  private static long measureMillis(Dataset<Row> frame) {
    long start = System.nanoTime();
    frame.collectAsList();
    return Math.max(1, (System.nanoTime() - start) / 1_000_000L);
  }

  private static long median(List<Long> values) {
    List<Long> ordered = values.stream().sorted().collect(Collectors.toList());
    return ordered.get(ordered.size() / 2);
  }

  private static String qualified(String catalog, String table) {
    return String.join(".", catalog, SCHEMA, table);
  }

  private static String schemaPath(String table) {
    return "/api/" + SCHEMA + "/" + table + "/_schema";
  }

  private static String requiredSystemProperty(String name) {
    String value = System.getProperty(name);
    if (value == null || value.isBlank()) {
      throw new IllegalStateException("Missing required system property " + name);
    }
    return value;
  }

  private static void createSimpleTable(Statement statement, String table) throws Exception {
    statement.execute(
        "CREATE TABLE `"
            + SCHEMA
            + "`.`"
            + table
            + "` (id INT NOT NULL) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 "
            + "PROPERTIES ('replication_num'='1')");
  }

  private static void insertPartitionedRows(Statement statement) throws Exception {
    for (int batch = 0; batch < 20; batch++) {
      StringBuilder sql =
          new StringBuilder("INSERT INTO `")
              .append(SCHEMA)
              .append("`.`")
              .append(PARTITIONED_TABLE)
              .append("` VALUES ");
      for (int offset = 1; offset <= 500; offset++) {
        int id = batch * 500 + offset;
        if (offset > 1) {
          sql.append(',');
        }
        sql.append('(')
            .append(id)
            .append(",'2026-01-01 00:00:")
            .append(String.format("%02d", id % 60))
            .append(".000000')");
      }
      statement.executeUpdate(sql.toString());
    }
  }

  private static void insertBoundaryRow(
      PreparedStatement statement, int id, String label, String description) throws Exception {
    statement.setInt(1, id);
    statement.setString(2, label);
    statement.setString(3, description);
    statement.executeUpdate();
  }

  private static void closeQuietly(AutoCloseable closeable) {
    if (closeable == null) {
      return;
    }
    try {
      closeable.close();
    } catch (Exception e) {
      LOG.warn("Failed to close integration-test resource {}", closeable.getClass().getName(), e);
    }
  }
}
