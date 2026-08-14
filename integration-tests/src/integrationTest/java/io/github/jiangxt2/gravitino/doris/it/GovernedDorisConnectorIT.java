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
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;

import com.google.common.collect.ImmutableList;
import io.github.jiangxt2.gravitino.doris.spark.GovernedDorisSparkPlugin;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
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
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.CatalogManager;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.function.Executable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** End-to-end verification against real Gravitino, Spark, and Doris components. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class GovernedDorisConnectorIT {

  private static final Logger LOG = LoggerFactory.getLogger(GovernedDorisConnectorIT.class);
  private static final String METALAKE = "doris_it";
  private static final String CATALOG = "governed_doris";
  private static final String PARTITIONED_CATALOG = "governed_doris_partitioned";
  private static final String RECORDER_CONTROL_CATALOG = "governed_doris_recorder_control";
  private static final String DIRECT_DENIAL_CATALOG = "governed_doris_direct_denial";
  private static final String SQL_DENIAL_CATALOG = "governed_doris_sql_denial";
  private static final String JDBC_BASELINE_CATALOG = "direct_doris_jdbc";
  private static final String STRICT_CATALOG = "governed_doris_strict";
  private static final String STRICT_PARTITIONED_CATALOG = "governed_doris_strict_partitioned";
  private static final String STRICT_RECORDER_CONTROL_CATALOG =
      "governed_doris_strict_recorder_control";
  private static final String STRICT_DENIAL_CATALOG = "governed_doris_strict_denial";
  private static final String ARROW_CATALOG = "governed_doris_arrow";
  private static final String ARROW_FALLBACK_CATALOG = "governed_doris_arrow_fallback";
  private static final String WRITE_CATALOG = "governed_doris_write";
  private static final String TRUNCATE_WRITE_CATALOG = "governed_doris_truncate_write";
  private static final String WRITE_DENIAL_CATALOG = "governed_doris_write_denial";
  private static final String DORIS_WRITE_DENIAL_CATALOG = "governed_doris_load_denial";
  private static final String DORIS_TRUNCATE_DENIAL_CATALOG = "governed_doris_truncate_load_denial";
  private static final String PROVIDER = "doris-governed";
  private static final String SCHEMA = "connector_it";
  private static final String COMMON_TABLE = "common_types";
  private static final String NORMALIZED_TABLE = "normalized_types";
  private static final String EXTENDED_TABLE = "extended_types";
  private static final String SKETCH_TABLE = "sketch_types";
  private static final String PARTITIONED_TABLE = "partitioned_types";
  private static final String CACHE_TABLE = "cache_table";
  private static final String FAILURE_TABLE = "failure_table";
  private static final String DRIFT_TABLE = "drift_table";
  private static final String DENIED_TABLE = "denied_table";
  private static final String WIDE_DECIMAL_TABLE = "wide_decimal";
  private static final String WRITE_APPEND_TABLE = "write_append_types";
  private static final String WRITE_TRUNCATE_TABLE = "write_truncate_types";
  private static final String WRITE_DENIED_TABLE = "write_denied_types";
  private static final String WRITE_DORIS_DENIED_TABLE = "write_doris_denied_types";
  private static final String WRITE_BULK_TABLE = "write_bulk_types";
  private static final String WRITE_FAILURE_TABLE = "write_failure_types";
  private static final String WRITE_DATETIME_TABLE = "write_datetime_types";
  private static final String READER = "doris_it_reader";
  private static final String READER_ROLE = "doris_it_reader_role";
  private static final String NULL_VALUE = "<null>";
  private static final String ARROW_CIRCUIT_PROPERTY_PREFIX =
      "spark.gravitino.doris.arrow.circuit.";
  private static final String ARROW_ATTEMPT_PROPERTY_PREFIX =
      "spark.gravitino.doris.arrow.attempt.";
  private static final List<TypeContractProbe> TYPE_CONTRACT_PROBES =
      Arrays.asList(
          TypeContractProbe.supported(
              "integer", "INTEGER", "17", "INT", Types.IntegerType.get(), DataTypes.IntegerType),
          TypeContractProbe.ddlRejected("decimalv2", "DECIMALV2(18,3)", "123.456"),
          TypeContractProbe.ddlRejected("decimal32", "DECIMAL32(9,2)", "123.45"),
          TypeContractProbe.ddlRejected("decimal64", "DECIMAL64(18,3)", "123.456"),
          TypeContractProbe.ddlRejected("decimal128", "DECIMAL128(38,6)", "123.456789"),
          TypeContractProbe.supported(
              "datev2", "DATEV2", "'2026-01-02'", "DATE", Types.DateType.get(), DataTypes.DateType),
          TypeContractProbe.supported(
              "text", "TEXT", "'evidence'", "TEXT", Types.StringType.get(), DataTypes.StringType),
          TypeContractProbe.normalized(
              "jsonb",
              "JSONB",
              "'{\"k\":1}'",
              "JSON",
              Types.ExternalType.of("json"),
              DataTypes.StringType),
          TypeContractProbe.versionedNormalized(
              "wide_decimal",
              "DECIMAL(76,6)",
              "1234567890123456789012345678901234567890.123456",
              false,
              true,
              "DECIMAL(76,6)",
              Types.ExternalType.of("decimal(76,6)"),
              DataTypes.StringType),
          TypeContractProbe.ddlRejected("binary", "BINARY", "X'4142'"),
          TypeContractProbe.ddlRejected("varbinary", "VARBINARY", "X'4142'"),
          TypeContractProbe.ddlRejected("time", "TIME", "'12:34:56'"),
          TypeContractProbe.ddlRejected("tinyint_unsigned", "TINYINT UNSIGNED", "1"),
          TypeContractProbe.ddlRejected("smallint_unsigned", "SMALLINT UNSIGNED", "1"),
          TypeContractProbe.ddlRejected("int_unsigned", "INT UNSIGNED", "1"),
          TypeContractProbe.ddlRejected("bigint_unsigned", "BIGINT UNSIGNED", "1"));

  private DockerTestNetwork network;
  private DorisTestCluster doris;
  private RecordingDorisHttpProxy feProxy;
  private RecordingDorisTcpProxy tcpProxy;
  private GravitinoTestServer gravitino;
  private TlsTestCertificates tlsCertificates;
  private String originalTrustStore;
  private String originalTrustStoreType;
  private boolean trustStoreConfigured;
  private GravitinoAdminClient adminClient;
  private GravitinoMetalake metalake;
  private GravitinoClient governedClient;
  private SparkSession spark;
  private final Map<String, TypeContractProbeResult> typeContractResults = new LinkedHashMap<>();

  @BeforeAll
  void startInfrastructure() throws Exception {
    Path repositoryRoot =
        Paths.get(requiredSystemProperty("connector.repository.root")).toAbsolutePath();
    Path providerDirectory =
        Paths.get(requiredSystemProperty("connector.provider.directory")).toAbsolutePath();
    Path jdbcDriver =
        Paths.get(requiredSystemProperty("connector.mysql.driver.path")).toAbsolutePath();
    Path externalDriverDirectory =
        Paths.get(requiredSystemProperty("connector.empty.jdbc.driver.directory")).toAbsolutePath();
    Path installedDriverDirectory =
        Paths.get(requiredSystemProperty("connector.installed.jdbc.driver.directory"))
            .toAbsolutePath();
    Path tlsFixtureDirectory =
        Paths.get(requiredSystemProperty("connector.tls.fixture.directory")).toAbsolutePath();
    assertThat(jdbcDriver).isRegularFile();
    assertThat(jdbcDriver.normalize().startsWith(providerDirectory.normalize())).isFalse();
    assertThat(externalDriverDirectory).isDirectory().isEmptyDirectory();
    // The Gravitino 1.3.0 entrypoint recognizes this legacy filename pattern, even though the
    // current Maven artifact is mysql-connector-j.
    assertThat(installedDriverDirectory.resolve("mysql-connector-java-8.0.33.jar")).isRegularFile();
    assertThat(installedDriverDirectory.normalize().startsWith(providerDirectory.normalize()))
        .isFalse();
    String version = System.getProperty("doris.version", "3.0.6.2");
    assertThat(version).isIn("3.0.6.2", "4.0.6");

    try {
      network = DockerTestNetwork.create();
      tlsCertificates = TlsTestCertificates.prepare(tlsFixtureDirectory);
      doris = new DorisTestCluster(network, version, repositoryRoot, tlsCertificates);
      doris.start();
      configureSparkTrustStore(tlsCertificates.clientTrustStore());
      feProxy = RecordingDorisHttpProxy.start(doris.hostFeEndpoint());
      tcpProxy = new RecordingDorisTcpProxy(network);
      tcpProxy.start();
      createPhysicalTables();

      verifyMissingServerDriverFailsBeforeDorisIo(providerDirectory, externalDriverDirectory);
      gravitino =
          new GravitinoTestServer(
              network, providerDirectory, installedDriverDirectory, tlsCertificates);
      gravitino.start();
      createGovernedMetadata();
      startSpark();
      verifyRecorderControls();
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
    closeQuietly(tcpProxy);
    closeQuietly(feProxy);
    closeQuietly(doris);
    closeQuietly(network);
    restoreSparkTrustStore();
  }

  @Test
  @Order(1)
  void recordsTypeContractForCurrentDorisVersion() throws Exception {
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
                + " WHERE id >= 2 AND id <= 3 ORDER BY id");
    assertThat(sparkRows(pushed)).containsExactly("2,beta", "3,alphabet");
    String plan = pushed.queryExecution().executedPlan().toString();
    assertThat(plan).contains("DorisScanV2").doesNotContain("JDBCRelation");

    logTypeContractEvidence();
    assertAll(
        "Doris " + doris.version() + " type contract",
        TYPE_CONTRACT_PROBES.stream()
            .map(probe -> (Executable) () -> assertTypeContractProbe(probe))
            .collect(Collectors.toList()));
  }

  @Test
  @Order(20)
  void usesArrowFlightSqlForTheNativeLane() throws Exception {
    RecordingDorisTcpProxy.State reset = tcpProxy.reset(RecordingDorisTcpProxy.Lane.FLIGHT);
    assertThat(reset.accepted()).isZero();
    assertThat(reset.active()).isZero();

    Dataset<Row> frame =
        spark.sql(
            "SELECT id, label FROM "
                + qualified(ARROW_CATALOG, COMMON_TABLE)
                + " WHERE id <= 3 ORDER BY id");
    assertThat(sparkRows(frame))
        .containsExactlyElementsOf(
            jdbcRows(
                "SELECT id, label FROM `"
                    + SCHEMA
                    + "`.`"
                    + COMMON_TABLE
                    + "` WHERE id <= 3 ORDER BY id"));
    assertThat(frame.queryExecution().executedPlan().toString())
        .contains("DorisScanV2")
        .doesNotContain("JDBCScan");
    await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(
            () -> {
              RecordingDorisTcpProxy.State state =
                  tcpProxy.state(RecordingDorisTcpProxy.Lane.FLIGHT);
              assertThat(state.accepted()).isPositive();
              assertThat(state.active()).isZero();
            });
  }

  @Test
  @Order(21)
  void fallsBackAfterProbeSuccessAndScopesTheCircuitToOneApplication() throws Exception {
    RecordingDorisTcpProxy.State unavailable = tcpProxy.setFlightFailureAvailable(false);
    assertThat(unavailable.active()).isZero();
    RecordingDorisTcpProxy.State reset = tcpProxy.reset(RecordingDorisTcpProxy.Lane.FLIGHT_FAILURE);
    assertThat(reset.accepted()).isZero();
    assertThat(reset.active()).isZero();
    String query =
        "SELECT id, label FROM "
            + qualified(ARROW_FALLBACK_CATALOG, COMMON_TABLE)
            + " WHERE id <= 3 ORDER BY id";
    List<String> expected =
        jdbcRows(
            "SELECT id, label FROM `"
                + SCHEMA
                + "`.`"
                + COMMON_TABLE
                + "` WHERE id <= 3 ORDER BY id");
    long attemptsBeforeFailure = arrowAttemptCount();

    assertThat(sparkRows(spark.sql(query))).containsExactlyElementsOf(expected);
    long firstAcceptedConnections =
        awaitSettledAcceptedConnections(RecordingDorisTcpProxy.Lane.FLIGHT_FAILURE);
    long firstArrowAttempts = arrowAttemptCount();
    assertThat(firstArrowAttempts)
        .as("only the tasks already in flight may enter Arrow before the circuit opens")
        .isGreaterThan(attemptsBeforeFailure)
        .isLessThanOrEqualTo(attemptsBeforeFailure + 2L);
    List<String> firstCircuitKeys = arrowCircuitPropertyKeys();
    assertThat(firstCircuitKeys).hasSize(1);

    assertThat(sparkRows(spark.newSession().sql(query))).containsExactlyElementsOf(expected);
    assertThat(arrowCircuitPropertyKeys()).containsExactlyElementsOf(firstCircuitKeys);
    assertThat(arrowAttemptCount())
        .as("a new session in the same application must bypass the failed Arrow endpoint")
        .isEqualTo(firstArrowAttempts);
    await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(
            () -> {
              RecordingDorisTcpProxy.State state =
                  tcpProxy.state(RecordingDorisTcpProxy.Lane.FLIGHT_FAILURE);
              assertThat(state.active()).isZero();
            });

    RecordingDorisTcpProxy.State recovered = tcpProxy.setFlightFailureAvailable(true);
    assertThat(recovered.active()).isZero();
    assertThat(sparkRows(spark.sql(query))).containsExactlyElementsOf(expected);
    assertThat(arrowAttemptCount())
        .as("endpoint recovery must not reset the fail-sticky application circuit")
        .isEqualTo(firstArrowAttempts);
    long acceptedBeforeNewApplication =
        tcpProxy.state(RecordingDorisTcpProxy.Lane.FLIGHT_FAILURE).accepted();
    assertThat(acceptedBeforeNewApplication).isGreaterThanOrEqualTo(firstAcceptedConnections);

    spark.close();
    SparkSession.clearActiveSession();
    SparkSession.clearDefaultSession();
    spark = null;
    startSpark();

    assertThat(sparkRows(spark.sql(query))).containsExactlyElementsOf(expected);
    assertThat(arrowAttemptCount())
        .as("a new Spark application must get a distinct Arrow attempt key")
        .isGreaterThan(firstArrowAttempts);
    await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(
            () -> {
              RecordingDorisTcpProxy.State state =
                  tcpProxy.state(RecordingDorisTcpProxy.Lane.FLIGHT_FAILURE);
              assertThat(state.accepted())
                  .as("a new Spark application must retry the recovered Arrow endpoint")
                  .isGreaterThan(acceptedBeforeNewApplication);
            });
  }

  @Test
  @Order(22)
  void differentiatesArrowForMultiTabletEmptyAndSqlLimitLanes() throws Exception {
    RecordingDorisTcpProxy.State reset = tcpProxy.reset(RecordingDorisTcpProxy.Lane.FLIGHT);
    assertThat(reset.accepted()).isZero();

    Dataset<Row> multiTablet =
        spark.sql("SELECT id, label FROM " + qualified(ARROW_CATALOG, COMMON_TABLE));
    assertThat(multiTablet.rdd().getNumPartitions()).isGreaterThan(1);
    assertThat(sorted(sparkRows(multiTablet)))
        .isEqualTo(
            sorted(jdbcRows("SELECT id, label FROM `" + SCHEMA + "`.`" + COMMON_TABLE + "`")));
    assertThat(
            sparkRows(
                spark.sql(
                    "SELECT id FROM " + qualified(ARROW_CATALOG, COMMON_TABLE) + " WHERE id < 0")))
        .isEmpty();
    await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(
            () ->
                assertThat(tcpProxy.state(RecordingDorisTcpProxy.Lane.FLIGHT).accepted())
                    .isGreaterThan(1));
    await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(
            () -> assertThat(tcpProxy.state(RecordingDorisTcpProxy.Lane.FLIGHT).active()).isZero());

    tcpProxy.reset(RecordingDorisTcpProxy.Lane.FLIGHT);
    Dataset<Row> limited =
        spark.sql(
            "SELECT id FROM " + qualified(ARROW_CATALOG, COMMON_TABLE) + " ORDER BY id LIMIT 3");
    assertThat(sparkRows(limited))
        .containsExactlyElementsOf(
            jdbcRows("SELECT id FROM `" + SCHEMA + "`.`" + COMMON_TABLE + "` ORDER BY id LIMIT 3"));
    assertThat(limited.queryExecution().executedPlan().toString())
        .contains("JDBCScan")
        .doesNotContain("DorisScanV2");
    await()
        .during(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(3))
        .untilAsserted(
            () ->
                assertThat(tcpProxy.state(RecordingDorisTcpProxy.Lane.FLIGHT).accepted()).isZero());
  }

  @Test
  @Order(30)
  void appendsGovernedRowsAndRoundTripsDatetime() throws Exception {
    spark
        .sql(
            "INSERT INTO "
                + qualified(WRITE_CATALOG, WRITE_APPEND_TABLE)
                + " VALUES (101, 'alpha', '2026-08-14 12:34:56.123456', 12.345), "
                + "(102, NULL, NULL, NULL)")
        .collectAsList();

    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(
            () ->
                assertThat(
                        jdbcRows(
                            "SELECT id, label, event_time, amount FROM `"
                                + SCHEMA
                                + "`.`"
                                + WRITE_APPEND_TABLE
                                + "` ORDER BY id"))
                    .containsExactly(
                        "101,alpha,2026-08-14 12:34:56.123456,12.345", "102,<null>,<null>,<null>"));

    assertThatThrownBy(
            () ->
                spark
                    .sql(
                        "INSERT OVERWRITE "
                            + qualified(WRITE_CATALOG, WRITE_APPEND_TABLE)
                            + " VALUES (103, 'blocked', NULL, NULL)")
                    .collectAsList())
        .hasMessageContaining("write")
        .hasMessageContaining("read-only");
    assertThat(jdbcRows("SELECT id FROM `" + SCHEMA + "`.`" + WRITE_APPEND_TABLE + "` ORDER BY id"))
        .containsExactly("101", "102");
  }

  @Test
  @Order(31)
  void replacesRowsOnlyForExplicitTruncateOverwrite() throws Exception {
    List<String> loadUserGrants = jdbcRows("SHOW GRANTS FOR '" + DorisTestCluster.LOAD_USER + "'");
    assertThat(loadUserGrants)
        .anyMatch(row -> row.toLowerCase(Locale.ROOT).contains("load_priv"))
        .noneMatch(
            row -> {
              String normalized = row.toLowerCase(Locale.ROOT);
              return normalized.contains("alter_priv") || normalized.contains("drop_priv");
            });
    spark
        .sql(
            "INSERT INTO "
                + qualified(TRUNCATE_WRITE_CATALOG, WRITE_TRUNCATE_TABLE)
                + " VALUES (201, 'before', NULL, 1.000)")
        .collectAsList();
    spark
        .sql(
            "INSERT OVERWRITE "
                + qualified(TRUNCATE_WRITE_CATALOG, WRITE_TRUNCATE_TABLE)
                + " VALUES (202, 'after', '2026-08-14 01:02:03.000001', 2.000)")
        .collectAsList();

    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(
            () ->
                assertThat(
                        jdbcRows(
                            "SELECT id, label, event_time, amount FROM `"
                                + SCHEMA
                                + "`.`"
                                + WRITE_TRUNCATE_TABLE
                                + "` ORDER BY id"))
                    .containsExactly("202,after,2026-08-14 01:02:03.000001,2.000"));
  }

  @Test
  @Order(32)
  void deniesModifyBeforeAnyObservedDorisIo() {
    RecordingDorisTcpProxy.State jdbcReset = tcpProxy.reset(RecordingDorisTcpProxy.Lane.DENIAL);
    feProxy.reset();
    SparkSession deniedSession = spark.newSession();
    CatalogManager manager = deniedSession.sessionState().catalogManager();
    try {
      assertThatThrownBy(
              () ->
                  deniedSession
                      .sql(
                          "INSERT INTO "
                              + qualified(WRITE_DENIAL_CATALOG, WRITE_DENIED_TABLE)
                              + " VALUES (301, 'denied', NULL, NULL)")
                      .collectAsList())
          .hasMessageContaining("not authorized")
          .hasMessageContaining("loadTable")
          .hasMessageNotContaining(DorisTestCluster.TEST_PASSWORD);
      assertTcpLaneRemainsZero(RecordingDorisTcpProxy.Lane.DENIAL, jdbcReset.generation());
      assertThat(feProxy.totalRequestCount()).isZero();
    } finally {
      manager.reset();
    }
  }

  @Test
  @Order(33)
  void deniesWriteWhenTheVendedDorisAccountLacksLoadPrivilege() throws Exception {
    assertThatThrownBy(
            () ->
                spark
                    .sql(
                        "INSERT INTO "
                            + qualified(DORIS_WRITE_DENIAL_CATALOG, WRITE_DORIS_DENIED_TABLE)
                            + " VALUES (401, 'denied-by-doris', NULL, NULL)")
                    .collectAsList())
        .hasMessageNotContaining(DorisTestCluster.READ_ONLY_PASSWORD)
        .hasMessageNotContaining(DorisTestCluster.TEST_PASSWORD);
    assertThat(jdbcRows("SELECT id FROM `" + SCHEMA + "`.`" + WRITE_DORIS_DENIED_TABLE + "`"))
        .isEmpty();
  }

  @Test
  @Order(34)
  void writesEmptyAndMultiPartitionBatches() throws Exception {
    Dataset<Row> input =
        spark
            .range(100_000)
            .repartition(4)
            .selectExpr(
                "CAST(id AS INT) AS id",
                "CAST(CONCAT('row-', id) AS STRING) AS label",
                "CAST(NULL AS STRING) AS event_time",
                "CAST(id % 100000 AS DECIMAL(18,3)) AS amount");
    input.limit(0).writeTo(qualified(WRITE_CATALOG, WRITE_BULK_TABLE)).append();
    assertThat(jdbcRows("SELECT COUNT(*) FROM `" + SCHEMA + "`.`" + WRITE_BULK_TABLE + "`"))
        .containsExactly("0");

    input.writeTo(qualified(WRITE_CATALOG, WRITE_BULK_TABLE)).append();
    await()
        .atMost(Duration.ofMinutes(2))
        .untilAsserted(
            () ->
                assertThat(
                        jdbcRows(
                            "SELECT COUNT(*), SUM(id) FROM `"
                                + SCHEMA
                                + "`.`"
                                + WRITE_BULK_TABLE
                                + "`"))
                    .containsExactly("100000,4999950000"));
  }

  @Test
  @Order(35)
  void rejectsInvalidDatetimeWithoutCommittingRows() throws Exception {
    assertThatThrownBy(
            () ->
                spark
                    .sql(
                        "INSERT INTO "
                            + qualified(WRITE_CATALOG, WRITE_FAILURE_TABLE)
                            + " VALUES (501, 'valid', '2026-08-14 00:00:00.000001', 1.000), "
                            + "(502, 'invalid', 'not-a-datetime', 2.000)")
                    .collectAsList())
        .hasMessageNotContaining(DorisTestCluster.LOAD_PASSWORD)
        .hasMessageNotContaining(DorisTestCluster.TEST_PASSWORD);
    assertThat(jdbcRows("SELECT id FROM `" + SCHEMA + "`.`" + WRITE_FAILURE_TABLE + "`")).isEmpty();

    String oversizedLabel = "x".repeat(65);
    assertThatThrownBy(
            () ->
                spark
                    .sql(
                        "INSERT INTO "
                            + qualified(WRITE_CATALOG, WRITE_FAILURE_TABLE)
                            + " VALUES (503, 'valid', '2026-08-14 00:00:00.000001', 3.000), "
                            + "(504, '"
                            + oversizedLabel
                            + "', '2026-08-14 00:00:00.000002', 4.000)")
                    .collectAsList())
        .hasMessageNotContaining(oversizedLabel)
        .hasMessageNotContaining(DorisTestCluster.LOAD_PASSWORD);
    assertThat(jdbcRows("SELECT id FROM `" + SCHEMA + "`.`" + WRITE_FAILURE_TABLE + "`")).isEmpty();
  }

  @Test
  @Order(36)
  void roundTripsCertifiedDatetimeInputsWithExplicitTimezoneSemantics() throws Exception {
    String originalTimezone = spark.conf().get("spark.sql.session.timeZone");
    List<String> timestampExpected = new ArrayList<>();
    try {
      insertDatetimeRows("UTC", 600);
      insertDatetimeRows("America/Los_Angeles", 700);
      timestampExpected.addAll(insertTimestampDatetimeRow("UTC", 801));
      timestampExpected.addAll(insertTimestampDatetimeRow("America/Los_Angeles", 802));
    } finally {
      spark.conf().set("spark.sql.session.timeZone", originalTimezone);
    }

    assertThat(
            jdbcRows(
                "SELECT id, dt0, dt3, dt6 FROM `"
                    + SCHEMA
                    + "`.`"
                    + WRITE_DATETIME_TABLE
                    + "` WHERE id < 800 ORDER BY id"))
        .containsExactly(
            "600,1969-12-31 23:59:59,1969-12-31 23:59:59.001,1969-12-31 23:59:59.000001",
            "601,2024-02-29 12:34:56,2024-02-29 12:34:56.123,2024-02-29 12:34:56.123456",
            "602,2024-03-10 02:30:00,2024-11-03 01:30:00.999,2024-11-03 01:30:00.999999",
            "603,<null>,<null>,<null>",
            "700,1969-12-31 23:59:59,1969-12-31 23:59:59.001,1969-12-31 23:59:59.000001",
            "701,2024-02-29 12:34:56,2024-02-29 12:34:56.123,2024-02-29 12:34:56.123456",
            "702,2024-03-10 02:30:00,2024-11-03 01:30:00.999,2024-11-03 01:30:00.999999",
            "703,<null>,<null>,<null>");

    assertThatThrownBy(
            () ->
                spark
                    .sql(
                        "INSERT INTO "
                            + qualified(WRITE_CATALOG, WRITE_DATETIME_TABLE)
                            + " VALUES (800, '2026-08-14 00:00:00', "
                            + "'2026-08-14 00:00:00.123456', '2026-08-14 00:00:00.123456')")
                    .collectAsList())
        .hasMessageContaining("precision-specific format")
        .hasMessageNotContaining("2026-08-14");
    assertThat(jdbcRows("SELECT COUNT(*) FROM `" + SCHEMA + "`.`" + WRITE_DATETIME_TABLE + "`"))
        .containsExactly("10");
    assertThat(
            jdbcRows(
                "SELECT id, dt0, dt3, dt6 FROM `"
                    + SCHEMA
                    + "`.`"
                    + WRITE_DATETIME_TABLE
                    + "` WHERE id >= 801 ORDER BY id"))
        .containsExactlyElementsOf(timestampExpected);
  }

  @Test
  @Order(37)
  void deniesTruncateWhenTheVendedDorisAccountLacksLoadPrivilege() throws Exception {
    try (Connection connection =
            DriverManager.getConnection(
                doris.hostJdbcUrl(), DorisTestCluster.TEST_USER, DorisTestCluster.TEST_PASSWORD);
        Statement statement = connection.createStatement()) {
      statement.executeUpdate(
          "INSERT INTO `"
              + SCHEMA
              + "`.`"
              + WRITE_DORIS_DENIED_TABLE
              + "` VALUES (450, 'preserved', NULL, 1.000)");
    }

    assertThatThrownBy(
            () ->
                spark
                    .sql(
                        "INSERT OVERWRITE "
                            + qualified(DORIS_TRUNCATE_DENIAL_CATALOG, WRITE_DORIS_DENIED_TABLE)
                            + " VALUES (451, 'blocked', NULL, 2.000)")
                    .collectAsList())
        .hasMessageNotContaining(DorisTestCluster.READ_ONLY_PASSWORD)
        .hasMessageNotContaining(DorisTestCluster.TEST_PASSWORD);
    assertThat(
            jdbcRows(
                "SELECT id FROM `" + SCHEMA + "`.`" + WRITE_DORIS_DENIED_TABLE + "` ORDER BY id"))
        .containsExactly("450");
  }

  @Test
  @Order(38)
  void documentsNonAtomicTruncateWhenTheSubsequentLoadFails() throws Exception {
    String oversizedLabel = "y".repeat(65);
    assertThatThrownBy(
            () ->
                spark
                    .sql(
                        "INSERT OVERWRITE "
                            + qualified(TRUNCATE_WRITE_CATALOG, WRITE_TRUNCATE_TABLE)
                            + " VALUES (203, '"
                            + oversizedLabel
                            + "', NULL, 3.000)")
                    .collectAsList())
        .hasMessageNotContaining(oversizedLabel)
        .hasMessageNotContaining(DorisTestCluster.LOAD_PASSWORD);
    assertThat(jdbcRows("SELECT id FROM `" + SCHEMA + "`.`" + WRITE_TRUNCATE_TABLE + "`"))
        .isEmpty();
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
  void keepsNormalizedColumnOperatorsAsSparkResiduals() throws Exception {
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
    Dataset<Row> extendedFrame =
        spark.sql(
            "SELECT id, variant_col, ipv4_col, ipv6_col FROM "
                + qualified(CATALOG, EXTENDED_TABLE)
                + " ORDER BY id");
    assertThat(sparkRows(extendedFrame))
        .containsExactlyElementsOf(
            jdbcRows(
                "SELECT id, variant_col, ipv4_col, ipv6_col FROM `"
                    + SCHEMA
                    + "`.`"
                    + EXTENDED_TABLE
                    + "` ORDER BY id"));
    assertThat(extendedFrame.queryExecution().executedPlan().toString())
        .contains("JDBCScan")
        .doesNotContain("DorisScanV2");

    Dataset<Row> ordered =
        spark.sql(
            "SELECT id, event_time FROM "
                + qualified(CATALOG, NORMALIZED_TABLE)
                + " ORDER BY event_time");
    assertThat(ordered.queryExecution().executedPlan().toString()).contains("Sort");

    String residualWhere =
        "SELECT id FROM "
            + qualified(CATALOG, NORMALIZED_TABLE)
            + " WHERE event_time >= '2026-04-05 06:07:08.123456' OR event_time IS NULL "
            + "ORDER BY id";
    Dataset<Row> filtered = spark.sql(residualWhere);
    assertThat(sparkRows(filtered)).containsExactly("1", "2");
    assertThat(filtered.queryExecution().executedPlan().toString())
        .contains("Filter")
        .contains("JDBCScan")
        .contains("PushedFilters: []");

    Dataset<Row> grouped =
        spark.sql(
            "SELECT event_time, COUNT(*) FROM "
                + qualified(CATALOG, NORMALIZED_TABLE)
                + " GROUP BY event_time");
    assertThat(sorted(sparkRows(grouped)))
        .isEqualTo(
            sorted(
                jdbcRows(
                    "SELECT event_time, COUNT(*) FROM `"
                        + SCHEMA
                        + "`.`"
                        + NORMALIZED_TABLE
                        + "` GROUP BY event_time")));
    assertThat(grouped.queryExecution().executedPlan().toString())
        .contains("Aggregate")
        .doesNotContain("PushedAggregates: [COUNT");

    Dataset<Row> aggregated =
        spark.sql("SELECT MAX(event_time) FROM " + qualified(CATALOG, NORMALIZED_TABLE));
    assertThat(sparkRows(aggregated))
        .isEqualTo(
            jdbcRows("SELECT MAX(event_time) FROM `" + SCHEMA + "`.`" + NORMALIZED_TABLE + "`"));
    assertThat(aggregated.queryExecution().executedPlan().toString())
        .contains("Aggregate")
        .doesNotContain("PushedAggregates: [MAX");

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
  void matchesDirectJdbcSchemaPartitionsAndPushdown() throws Exception {
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
    assertThat(governed.queryExecution().executedPlan().toString())
        .contains("JDBCScan")
        .doesNotContain("DorisScanV2");
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
  void deniesDirectTableLoadBeforeAnyObservedDorisIo() {
    SparkSession deniedSession = spark.newSession();
    CatalogManager manager = deniedSession.sessionState().catalogManager();
    assertThat(manager).isNotSameAs(spark.sessionState().catalogManager());
    assertThat(isCatalogResolved(manager, DIRECT_DENIAL_CATALOG)).isFalse();

    RecordingDorisTcpProxy.State reset = resetDeniedIoRecorders();
    try {
      org.apache.spark.sql.connector.catalog.TableCatalog deniedCatalog =
          sparkCatalog(deniedSession, DIRECT_DENIAL_CATALOG);
      assertThat(isCatalogResolved(manager, DIRECT_DENIAL_CATALOG)).isTrue();
      assertThatThrownBy(
              () -> deniedCatalog.loadTable(Identifier.of(new String[] {SCHEMA}, DENIED_TABLE)))
          .isInstanceOf(ForbiddenException.class)
          .hasMessageContaining("not authorized")
          .hasMessageContaining("loadTable")
          .hasMessageNotContaining(DorisTestCluster.TEST_PASSWORD);
      assertDeniedIoRemainsZero(reset.generation());
    } finally {
      manager.reset();
    }
  }

  @Test
  void deniesSparkSqlBeforeAnyObservedDorisIo() {
    SparkSession deniedSession = spark.newSession();
    CatalogManager manager = deniedSession.sessionState().catalogManager();
    assertThat(manager).isNotSameAs(spark.sessionState().catalogManager());
    assertThat(isCatalogResolved(manager, SQL_DENIAL_CATALOG)).isFalse();

    RecordingDorisTcpProxy.State reset = resetDeniedIoRecorders();
    try {
      assertThatThrownBy(
              () ->
                  deniedSession
                      .sql("SELECT * FROM " + qualified(SQL_DENIAL_CATALOG, DENIED_TABLE))
                      .collectAsList())
          .hasMessageContaining("not authorized")
          .hasMessageContaining("loadTable")
          .hasMessageNotContaining(DorisTestCluster.TEST_PASSWORD);
      assertThat(isCatalogResolved(manager, SQL_DENIAL_CATALOG)).isTrue();
      assertDeniedIoRemainsZero(reset.generation());
    } finally {
      manager.reset();
    }
  }

  @Test
  @Order(80)
  void rejectsUnsafeCatalogConfigurationWithoutObservedDorisIo() {
    RecordingDorisTcpProxy.State reset = resetDeniedIoRecorders();
    String maliciousSecret = "catalog-security-secret-canary";

    List<Map<String, String>> unsafeProperties = new ArrayList<>();
    Map<String, String> embeddedCredential =
        catalogProperties(false, tcpProxy.jdbcUrl(RecordingDorisTcpProxy.Lane.DENIAL));
    embeddedCredential.put(
        "jdbc-url",
        tcpProxy
            .jdbcUrl(RecordingDorisTcpProxy.Lane.DENIAL)
            .replace("jdbc:mysql://", "jdbc:mysql://reader:" + maliciousSecret + "@"));
    unsafeProperties.add(embeddedCredential);

    Map<String, String> encodedParameter =
        catalogProperties(false, tcpProxy.jdbcUrl(RecordingDorisTcpProxy.Lane.DENIAL));
    encodedParameter.put(
        "jdbc-url",
        tcpProxy.jdbcUrl(RecordingDorisTcpProxy.Lane.DENIAL) + "?auto%2544eserialize=true");
    unsafeProperties.add(encodedParameter);

    Map<String, String> connectionProperties =
        catalogProperties(false, tcpProxy.jdbcUrl(RecordingDorisTcpProxy.Lane.DENIAL));
    connectionProperties.put("gravitino.bypass.connectionProperties", "auto\\u0044eserialize=true");
    unsafeProperties.add(connectionProperties);

    Map<String, String> classLoading =
        catalogProperties(false, tcpProxy.jdbcUrl(RecordingDorisTcpProxy.Lane.DENIAL));
    classLoading.put("gravitino.bypass.connectionFactoryClassName", maliciousSecret);
    unsafeProperties.add(classLoading);

    Map<String, String> connectionInitSqls =
        catalogProperties(false, tcpProxy.jdbcUrl(RecordingDorisTcpProxy.Lane.DENIAL));
    connectionInitSqls.put(
        "gravitino.bypass.connectionInitSqls", "SELECT '" + maliciousSecret + "'");
    unsafeProperties.add(connectionInitSqls);

    Map<String, String> identityOverride =
        catalogProperties(false, tcpProxy.jdbcUrl(RecordingDorisTcpProxy.Lane.DENIAL));
    identityOverride.put("gravitino.bypass.url", "jdbc:mysql://" + maliciousSecret + ":9030/");
    unsafeProperties.add(identityOverride);

    Map<String, String> unknownUrlParameter =
        catalogProperties(false, tcpProxy.jdbcUrl(RecordingDorisTcpProxy.Lane.DENIAL));
    unknownUrlParameter.put(
        "jdbc-url",
        tcpProxy.jdbcUrl(RecordingDorisTcpProxy.Lane.DENIAL)
            + "?unknown-"
            + maliciousSecret
            + "=true");
    unsafeProperties.add(unknownUrlParameter);

    Map<String, String> unknownBypass =
        catalogProperties(false, tcpProxy.jdbcUrl(RecordingDorisTcpProxy.Lane.DENIAL));
    unknownBypass.put("gravitino.bypass.unknown-" + maliciousSecret, "true");
    unsafeProperties.add(unknownBypass);

    Map<String, String> unknownConnectionProperty =
        catalogProperties(false, tcpProxy.jdbcUrl(RecordingDorisTcpProxy.Lane.DENIAL));
    unknownConnectionProperty.put(
        "gravitino.bypass.connectionProperties", "unknown-" + maliciousSecret + "=true");
    unsafeProperties.add(unknownConnectionProperty);

    for (int index = 0; index < unsafeProperties.size(); index++) {
      String catalogName = "unsafe_jdbc_catalog_" + index;
      Map<String, String> properties = unsafeProperties.get(index);
      assertThatThrownBy(
              () ->
                  metalake.createCatalog(
                      catalogName,
                      Catalog.Type.RELATIONAL,
                      PROVIDER,
                      "Unsafe JDBC catalog must be rejected",
                      properties))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageNotContaining(maliciousSecret)
          .hasMessageNotContaining(DorisTestCluster.TEST_PASSWORD)
          .hasMessageNotContaining(properties.get("jdbc-url"));
    }
    assertDeniedIoRemainsZero(reset.generation());
  }

  @Test
  @Order(81)
  void rejectsUnsupportedTimestampMutationBeforeChangingDorisSchema() throws Exception {
    TableCatalog tableCatalog = governedClient.loadCatalog(CATALOG).asTableCatalog();
    NameIdentifier identifier = NameIdentifier.of(SCHEMA, DRIFT_TABLE);
    Map<String, String> before = jdbcColumnTypes(DRIFT_TABLE);

    assertThatThrownBy(
            () ->
                tableCatalog.alterTable(
                    identifier,
                    TableChange.addColumn(
                        new String[] {"unsupported_timestamp"},
                        Types.TimestampType.withTimeZone(),
                        true)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Doris DATETIME requires a timestamp without time zone and precision 0 to 6")
        .hasMessageNotContaining(DorisTestCluster.TEST_PASSWORD);
    assertThat(jdbcColumnTypes(DRIFT_TABLE)).isEqualTo(before);
  }

  @Test
  @Order(90)
  void strictJdbcTlsMatchesVerifiedDirectJdbcAndNeverUsesFeHttp() throws Exception {
    feProxy.reset();
    Dataset<Row> detail =
        spark.sql(
            "SELECT id, label FROM "
                + qualified(STRICT_CATALOG, COMMON_TABLE)
                + " WHERE id >= 2 ORDER BY id LIMIT 3 OFFSET 1");
    assertThat(sparkRows(detail))
        .containsExactlyElementsOf(
            verifiedJdbcRows(
                doris.hostStrictJdbcUrl(),
                "SELECT id, label FROM `"
                    + SCHEMA
                    + "`.`"
                    + COMMON_TABLE
                    + "` WHERE id >= 2 ORDER BY id LIMIT 3 OFFSET 1"));
    assertThat(detail.queryExecution().executedPlan().toString())
        .contains("JDBCScan")
        .doesNotContain("DorisScanV2");

    Dataset<Row> aggregate =
        spark.sql(
            "SELECT code, COUNT(*), SUM(amount) FROM "
                + qualified(STRICT_CATALOG, COMMON_TABLE)
                + " GROUP BY code");
    assertThat(sorted(sparkRows(aggregate)))
        .isEqualTo(
            sorted(
                verifiedJdbcRows(
                    doris.hostStrictJdbcUrl(),
                    "SELECT code, COUNT(*), SUM(amount) FROM `"
                        + SCHEMA
                        + "`.`"
                        + COMMON_TABLE
                        + "` GROUP BY code")));
    assertThat(aggregate.queryExecution().executedPlan().toString())
        .contains("PushedAggregates")
        .doesNotContain("DorisScanV2");

    Dataset<Row> partitioned =
        spark
            .table(qualified(STRICT_PARTITIONED_CATALOG, PARTITIONED_TABLE))
            .select("id", "event_time");
    assertThat(partitioned.rdd().getNumPartitions()).isEqualTo(4);
    assertThat(sorted(sparkRows(partitioned)))
        .isEqualTo(
            sorted(
                verifiedJdbcRows(
                    doris.hostStrictJdbcUrl(),
                    "SELECT id, event_time FROM `" + SCHEMA + "`.`" + PARTITIONED_TABLE + "`")));
    assertThat(feProxy.totalRequestCount()).isZero();
  }

  @Test
  @Order(91)
  void strictAuthorizationDenialPrecedesJdbcAndFeHttpIo() {
    SparkSession deniedSession = spark.newSession();
    CatalogManager manager = deniedSession.sessionState().catalogManager();
    RecordingDorisTcpProxy.State reset = resetDeniedIoRecorders();
    try {
      assertThatThrownBy(
              () ->
                  deniedSession
                      .sql("SELECT * FROM " + qualified(STRICT_DENIAL_CATALOG, DENIED_TABLE))
                      .collectAsList())
          .hasMessageContaining("not authorized")
          .hasMessageNotContaining(DorisTestCluster.TEST_PASSWORD);
      assertDeniedIoRemainsZero(reset.generation());
    } finally {
      manager.reset();
    }
  }

  @Test
  @Order(92)
  void strictTlsRejectsHostnameMismatchWithoutLeakingConnectionMaterial() {
    String mismatchUrl = "jdbc:mysql://doris-fe-mismatch:9030/?sslMode=VERIFY_IDENTITY";
    Map<String, String> mismatchProperties = strictCatalogProperties(false, mismatchUrl);
    assertThatThrownBy(
            () ->
                metalake.testConnection(
                    "strict_hostname_mismatch",
                    Catalog.Type.RELATIONAL,
                    PROVIDER,
                    "Strict hostname mismatch must fail",
                    mismatchProperties))
        .isInstanceOf(RuntimeException.class)
        .hasMessageNotContaining(mismatchUrl)
        .hasMessageNotContaining(DorisTestCluster.TEST_PASSWORD);
  }

  @Test
  @Order(Integer.MAX_VALUE - 2)
  void strictTlsRejectsUnknownCa() {
    doris.installUnknownCaCertificate();
    String unknownCaUrl = doris.hostStrictJdbcUrl();
    assertThatThrownBy(() -> verifiedJdbcRows(unknownCaUrl, "SELECT 1"))
        .isInstanceOf(Exception.class)
        .hasMessageNotContaining(unknownCaUrl)
        .hasMessageNotContaining(DorisTestCluster.TEST_PASSWORD);
  }

  @Test
  @Order(Integer.MAX_VALUE - 1)
  void strictTlsRejectsExpiredServerCertificate() {
    doris.installExpiredCertificate();
    String expiredUrl = doris.hostStrictJdbcUrl();
    assertThatThrownBy(() -> verifiedJdbcRows(expiredUrl, "SELECT 1"))
        .isInstanceOf(Exception.class)
        .hasMessageNotContaining(expiredUrl)
        .hasMessageNotContaining(DorisTestCluster.TEST_PASSWORD);
  }

  @Test
  @Order(Integer.MAX_VALUE)
  void strictTlsRefusesPlaintextDowngrade() {
    doris.disableTls();
    String strictUrl = doris.hostStrictJdbcUrl();
    assertThatThrownBy(() -> verifiedJdbcRows(strictUrl, "SELECT 1"))
        .isInstanceOf(Exception.class)
        .hasMessageNotContaining(strictUrl)
        .hasMessageNotContaining(DorisTestCluster.TEST_PASSWORD);
  }

  private void verifyMissingServerDriverFailsBeforeDorisIo(
      Path providerDirectory, Path externalDriverDirectory) {
    try (GravitinoTestServer serverWithoutDriver =
        new GravitinoTestServer(network, providerDirectory, externalDriverDirectory)) {
      serverWithoutDriver.start();
      try (GravitinoAdminClient client =
          GravitinoAdminClient.builder(serverWithoutDriver.uri())
              .withSimpleAuth(GravitinoTestServer.ADMIN_USER)
              .build()) {
        GravitinoMetalake missingDriverMetalake =
            client.createMetalake("missing_driver", "Missing external JDBC driver smoke", Map.of());
        RecordingDorisTcpProxy.State reset = resetDeniedIoRecorders();

        assertThatThrownBy(
                () ->
                    missingDriverMetalake.createCatalog(
                        "missing_driver_catalog",
                        Catalog.Type.RELATIONAL,
                        PROVIDER,
                        "Catalog creation must fail before Doris I/O",
                        recordingCatalogProperties(RecordingDorisTcpProxy.Lane.DENIAL)))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("com.mysql:mysql-connector-j:8.0.33")
            .hasMessageContaining("$GRAVITINO_HOME/catalogs/doris-governed/libs")
            .hasMessageNotContaining(DorisTestCluster.TEST_PASSWORD)
            .hasMessageNotContaining(tcpProxy.jdbcUrl(RecordingDorisTcpProxy.Lane.DENIAL));
        assertDeniedIoRemainsZero(reset.generation());
      }
    }
  }

  private RecordingDorisTcpProxy.State resetDeniedIoRecorders() {
    feProxy.reset();
    RecordingDorisTcpProxy.State state = tcpProxy.reset(RecordingDorisTcpProxy.Lane.DENIAL);
    assertThat(state.accepted()).isZero();
    assertThat(state.active()).isZero();
    return state;
  }

  private void assertDeniedIoRemainsZero(long generation) {
    await()
        .during(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(3))
        .untilAsserted(
            () -> {
              RecordingDorisTcpProxy.State state =
                  tcpProxy.state(RecordingDorisTcpProxy.Lane.DENIAL);
              assertThat(state.generation()).isEqualTo(generation);
              assertThat(state.accepted()).isZero();
              assertThat(state.active()).isZero();
              assertThat(feProxy.totalRequestCount()).isZero();
            });
  }

  @Test
  @SuppressWarnings("deprecation")
  void usesOnePhysicalSchemaSnapshotPerAuthorizedLoad() throws Exception {
    org.apache.spark.sql.connector.catalog.TableCatalog sparkCatalog = sparkCatalog(CATALOG);
    Identifier identifier = Identifier.of(new String[] {SCHEMA}, CACHE_TABLE);
    String path = schemaPath(CACHE_TABLE);
    sparkCatalog.invalidateTable(identifier);
    feProxy.reset();

    Table first = sparkCatalog.loadTable(identifier);
    assertThat(first.schema().fieldNames()).containsExactly("id");
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
  @SuppressWarnings("deprecation")
  void reloadsStaleSnapshotAndCompletesRefreshTable() throws Throwable {
    org.apache.spark.sql.connector.catalog.TableCatalog sparkCatalog = sparkCatalog(CATALOG);
    org.apache.spark.sql.connector.catalog.TableCatalog strictSparkCatalog =
        sparkCatalog(STRICT_CATALOG);
    Identifier identifier = Identifier.of(new String[] {SCHEMA}, DRIFT_TABLE);
    String qualifiedTable = "`" + SCHEMA + "`.`" + DRIFT_TABLE + "`";
    String path = schemaPath(DRIFT_TABLE);
    sparkCatalog.invalidateTable(identifier);
    strictSparkCatalog.invalidateTable(identifier);
    feProxy.reset();
    assertThat(strictSparkCatalog.loadTable(identifier).schema().fieldNames())
        .containsExactly("id");
    assertThat(feProxy.requestCount("GET", path)).isZero();
    assertThat(sparkCatalog.loadTable(identifier).schema().fieldNames()).containsExactly("id");
    assertThat(feProxy.requestCount("GET", path)).isEqualTo(1);

    Throwable testFailure = null;
    try (Connection connection =
            DriverManager.getConnection(
                doris.hostJdbcUrl(), DorisTestCluster.TEST_USER, DorisTestCluster.TEST_PASSWORD);
        Statement statement = connection.createStatement()) {
      statement.execute("ALTER TABLE " + qualifiedTable + " ADD COLUMN drifted BIGINT NULL");
      await()
          .atMost(Duration.ofSeconds(30))
          .untilAsserted(() -> assertThat(jdbcColumnTypes(DRIFT_TABLE)).containsKey("drifted"));

      // Spark resolves the relation before invoking catalog invalidation. Each transport must
      // therefore replace its exact stale snapshot before REFRESH TABLE can complete. The strict
      // path reloads JDBC metadata without opening the native FE HTTP schema path.
      spark.sql("REFRESH TABLE " + qualified(STRICT_CATALOG, DRIFT_TABLE)).collectAsList();
      assertThat(feProxy.requestCount("GET", path)).isEqualTo(1);
      assertThat(strictSparkCatalog.loadTable(identifier).schema().fieldNames())
          .containsExactly("id", "drifted");
      assertThat(feProxy.requestCount("GET", path)).isEqualTo(1);

      spark.sql("REFRESH TABLE " + qualified(CATALOG, DRIFT_TABLE)).collectAsList();
      assertThat(feProxy.requestCount("GET", path)).isEqualTo(2);
      assertThat(sparkCatalog.loadTable(identifier).schema().fieldNames())
          .containsExactly("id", "drifted");
      assertThat(feProxy.requestCount("GET", path)).isEqualTo(3);
    } catch (Throwable failure) {
      testFailure = failure;
    }

    Throwable cleanupFailure = null;
    try {
      restoreDriftTable(sparkCatalog, identifier, qualifiedTable);
    } catch (Throwable failure) {
      cleanupFailure = failure;
    }
    try {
      strictSparkCatalog.invalidateTable(identifier);
    } catch (Throwable failure) {
      if (cleanupFailure == null) {
        cleanupFailure = failure;
      } else {
        cleanupFailure.addSuppressed(failure);
      }
    }
    if (testFailure != null) {
      if (cleanupFailure != null) {
        testFailure.addSuppressed(cleanupFailure);
      }
      throw testFailure;
    }
    if (cleanupFailure != null) {
      throw cleanupFailure;
    }
  }

  @Test
  void exposesOnlyBatchReadAndKeepsCredentialsOutOfPlansAndLogs() {
    org.apache.spark.sql.connector.catalog.TableCatalog catalog = sparkCatalog(CATALOG);
    org.apache.spark.sql.connector.catalog.TableCatalog strictCatalog =
        sparkCatalog(STRICT_CATALOG);
    Table table;
    Table strictTable;
    try {
      table = catalog.loadTable(Identifier.of(new String[] {SCHEMA}, COMMON_TABLE));
      strictTable = strictCatalog.loadTable(Identifier.of(new String[] {SCHEMA}, COMMON_TABLE));
    } catch (Exception e) {
      throw new AssertionError(e);
    }
    assertThat(table.capabilities()).containsExactly(TableCapability.BATCH_READ);
    assertThat(strictTable.capabilities()).containsExactly(TableCapability.BATCH_READ);
    assertThat(table.properties()).doesNotContainKeys("jdbc-user", "jdbc-password");
    assertThat(strictTable.properties()).doesNotContainKeys("jdbc-user", "jdbc-password");

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
                        "INSERT INTO "
                            + qualified(STRICT_CATALOG, COMMON_TABLE)
                            + " (id) VALUES (999)")
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
      createSimpleTable(statement, DRIFT_TABLE);
      createSimpleTable(statement, DENIED_TABLE);
      createWriteTable(statement, WRITE_APPEND_TABLE);
      createWriteTable(statement, WRITE_TRUNCATE_TABLE);
      createWriteTable(statement, WRITE_DENIED_TABLE);
      createWriteTable(statement, WRITE_DORIS_DENIED_TABLE);
      createWriteTable(statement, WRITE_BULK_TABLE);
      createWriteTable(statement, WRITE_FAILURE_TABLE);
      statement.execute(
          "CREATE TABLE `"
              + SCHEMA
              + "`.`"
              + WRITE_DATETIME_TABLE
              + "` (id INT NOT NULL, dt0 DATETIME, dt3 DATETIME(3), dt6 DATETIME(6)) "
              + "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 2 "
              + "PROPERTIES ('replication_num'='1')");
      statement.execute(
          "GRANT SELECT_PRIV, LOAD_PRIV ON `"
              + SCHEMA
              + "`.* TO '"
              + DorisTestCluster.LOAD_USER
              + "'");
      statement.execute(
          "GRANT SELECT_PRIV ON `" + SCHEMA + "`.* TO '" + DorisTestCluster.READ_ONLY_USER + "'");
      statement.executeUpdate("INSERT INTO `" + SCHEMA + "`.`" + CACHE_TABLE + "` VALUES (1)");
      statement.executeUpdate("INSERT INTO `" + SCHEMA + "`.`" + FAILURE_TABLE + "` VALUES (1)");
      statement.executeUpdate("INSERT INTO `" + SCHEMA + "`.`" + DRIFT_TABLE + "` VALUES (1)");
      statement.executeUpdate("INSERT INTO `" + SCHEMA + "`.`" + DENIED_TABLE + "` VALUES (1)");

      if (doris.version().startsWith("4.")) {
        statement.execute("SET enable_decimal256 = true");
      }
      for (TypeContractProbe probe : TYPE_CONTRACT_PROBES) {
        createTypeContractProbe(statement, probe);
      }

      statement.execute(
          "CREATE TABLE `"
              + SCHEMA
              + "`.`"
              + PARTITIONED_TABLE
              + "` (id INT NOT NULL, event_time DATETIME(6)) DUPLICATE KEY(id) "
              + "DISTRIBUTED BY HASH(id) BUCKETS 4 PROPERTIES ('replication_num'='1')");
      insertPartitionedRows(statement);

      if (doris.version().startsWith("4.")) {
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
    metalake =
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
    Catalog strictCatalog =
        metalake.createCatalog(
            STRICT_CATALOG,
            Catalog.Type.RELATIONAL,
            PROVIDER,
            "Verified JDBC-only Doris integration catalog",
            strictCatalogProperties(false, doris.internalStrictJdbcUrl()));
    testVerifiedServerConnection(
        "strict_server_connection", strictCatalogProperties(false, doris.internalStrictJdbcUrl()));
    Catalog strictPartitionedCatalog =
        metalake.createCatalog(
            STRICT_PARTITIONED_CATALOG,
            Catalog.Type.RELATIONAL,
            PROVIDER,
            "Verified JDBC-only partitioned integration catalog",
            strictCatalogProperties(true, doris.internalStrictJdbcUrl()));
    Catalog recorderControlCatalog =
        metalake.createCatalog(
            RECORDER_CONTROL_CATALOG,
            Catalog.Type.RELATIONAL,
            PROVIDER,
            "Governed Doris TCP recorder control catalog",
            recordingCatalogProperties(RecordingDorisTcpProxy.Lane.CONTROL));
    Catalog strictRecorderControlCatalog =
        metalake.createCatalog(
            STRICT_RECORDER_CONTROL_CATALOG,
            Catalog.Type.RELATIONAL,
            PROVIDER,
            "Verified JDBC-only recorder control catalog",
            strictRecordingCatalogProperties(
                false,
                tcpProxy.jdbcUrl(RecordingDorisTcpProxy.Lane.CONTROL)
                    + "?sslMode=VERIFY_IDENTITY"));
    Catalog arrowCatalog =
        metalake.createCatalog(
            ARROW_CATALOG,
            Catalog.Type.RELATIONAL,
            PROVIDER,
            "Governed Doris Arrow Flight SQL catalog",
            arrowCatalogProperties(RecordingDorisTcpProxy.Lane.FLIGHT));
    Catalog arrowFallbackCatalog =
        metalake.createCatalog(
            ARROW_FALLBACK_CATALOG,
            Catalog.Type.RELATIONAL,
            PROVIDER,
            "Governed Doris Arrow fallback catalog",
            arrowCatalogProperties(RecordingDorisTcpProxy.Lane.FLIGHT_FAILURE));
    Catalog writeCatalog =
        metalake.createCatalog(
            WRITE_CATALOG,
            Catalog.Type.RELATIONAL,
            PROVIDER,
            "Governed Doris batch append catalog",
            writeCatalogProperties(
                false, false, DorisTestCluster.LOAD_USER, DorisTestCluster.LOAD_PASSWORD));
    Catalog truncateWriteCatalog =
        metalake.createCatalog(
            TRUNCATE_WRITE_CATALOG,
            Catalog.Type.RELATIONAL,
            PROVIDER,
            "Governed Doris truncate overwrite catalog",
            writeCatalogProperties(
                true, false, DorisTestCluster.LOAD_USER, DorisTestCluster.LOAD_PASSWORD));
    Catalog writeDenialCatalog =
        metalake.createCatalog(
            WRITE_DENIAL_CATALOG,
            Catalog.Type.RELATIONAL,
            PROVIDER,
            "Governed Doris modify denial catalog",
            writeCatalogProperties(
                false, true, DorisTestCluster.TEST_USER, DorisTestCluster.TEST_PASSWORD));
    Catalog dorisWriteDenialCatalog =
        metalake.createCatalog(
            DORIS_WRITE_DENIAL_CATALOG,
            Catalog.Type.RELATIONAL,
            PROVIDER,
            "Governed Doris LOAD privilege denial catalog",
            writeCatalogProperties(
                false,
                false,
                DorisTestCluster.READ_ONLY_USER,
                DorisTestCluster.READ_ONLY_PASSWORD));
    Catalog dorisTruncateDenialCatalog =
        metalake.createCatalog(
            DORIS_TRUNCATE_DENIAL_CATALOG,
            Catalog.Type.RELATIONAL,
            PROVIDER,
            "Governed Doris truncate LOAD privilege denial catalog",
            writeCatalogProperties(
                true, false, DorisTestCluster.READ_ONLY_USER, DorisTestCluster.READ_ONLY_PASSWORD));
    metalake.createCatalog(
        DIRECT_DENIAL_CATALOG,
        Catalog.Type.RELATIONAL,
        PROVIDER,
        "Fresh direct authorization-denial catalog",
        recordingCatalogProperties(RecordingDorisTcpProxy.Lane.DENIAL));
    metalake.createCatalog(
        SQL_DENIAL_CATALOG,
        Catalog.Type.RELATIONAL,
        PROVIDER,
        "Fresh SQL authorization-denial catalog",
        recordingCatalogProperties(RecordingDorisTcpProxy.Lane.DENIAL));
    metalake.createCatalog(
        STRICT_DENIAL_CATALOG,
        Catalog.Type.RELATIONAL,
        PROVIDER,
        "Fresh strict authorization-denial catalog",
        strictRecordingCatalogProperties(
            false,
            tcpProxy.jdbcUrl(RecordingDorisTcpProxy.Lane.DENIAL) + "?sslMode=VERIFY_IDENTITY"));
    catalog.asSchemas().loadSchema(SCHEMA);
    partitionedCatalog.asSchemas().loadSchema(SCHEMA);
    strictCatalog.asSchemas().loadSchema(SCHEMA);
    strictPartitionedCatalog.asSchemas().loadSchema(SCHEMA);
    recorderControlCatalog.asSchemas().loadSchema(SCHEMA);
    strictRecorderControlCatalog.asSchemas().loadSchema(SCHEMA);
    arrowCatalog.asSchemas().loadSchema(SCHEMA);
    arrowFallbackCatalog.asSchemas().loadSchema(SCHEMA);
    writeCatalog.asSchemas().loadSchema(SCHEMA);
    truncateWriteCatalog.asSchemas().loadSchema(SCHEMA);
    writeDenialCatalog.asSchemas().loadSchema(SCHEMA);
    dorisWriteDenialCatalog.asSchemas().loadSchema(SCHEMA);
    dorisTruncateDenialCatalog.asSchemas().loadSchema(SCHEMA);

    List<String> mainTables =
        new ArrayList<>(
            Arrays.asList(
                COMMON_TABLE,
                NORMALIZED_TABLE,
                EXTENDED_TABLE,
                SKETCH_TABLE,
                CACHE_TABLE,
                FAILURE_TABLE,
                DRIFT_TABLE,
                DENIED_TABLE));
    if (doris.version().startsWith("4.")) {
      mainTables.add(WIDE_DECIMAL_TABLE);
    }
    TableCatalog mainTableCatalog = catalog.asTableCatalog();
    mainTables.forEach(name -> mainTableCatalog.loadTable(NameIdentifier.of(SCHEMA, name)));
    for (TypeContractProbeResult result : typeContractResults.values()) {
      if (!result.ddlSucceeded) {
        continue;
      }
      mainTables.add(result.probe.tableName());
      try {
        org.apache.gravitino.rel.Table table =
            mainTableCatalog.loadTable(NameIdentifier.of(SCHEMA, result.probe.tableName()));
        result.logicalType = table.columns()[1].dataType();
      } catch (RuntimeException e) {
        result.providerFailure = e.getClass().getSimpleName();
      }
    }
    partitionedCatalog.asTableCatalog().loadTable(NameIdentifier.of(SCHEMA, PARTITIONED_TABLE));
    TableCatalog strictMainTableCatalog = strictCatalog.asTableCatalog();
    mainTables.forEach(name -> strictMainTableCatalog.loadTable(NameIdentifier.of(SCHEMA, name)));
    strictPartitionedCatalog
        .asTableCatalog()
        .loadTable(NameIdentifier.of(SCHEMA, PARTITIONED_TABLE));
    recorderControlCatalog.asTableCatalog().loadTable(NameIdentifier.of(SCHEMA, COMMON_TABLE));
    strictRecorderControlCatalog
        .asTableCatalog()
        .loadTable(NameIdentifier.of(SCHEMA, COMMON_TABLE));
    arrowCatalog.asTableCatalog().loadTable(NameIdentifier.of(SCHEMA, COMMON_TABLE));
    arrowFallbackCatalog.asTableCatalog().loadTable(NameIdentifier.of(SCHEMA, COMMON_TABLE));
    writeCatalog.asTableCatalog().loadTable(NameIdentifier.of(SCHEMA, WRITE_APPEND_TABLE));
    writeCatalog.asTableCatalog().loadTable(NameIdentifier.of(SCHEMA, WRITE_BULK_TABLE));
    writeCatalog.asTableCatalog().loadTable(NameIdentifier.of(SCHEMA, WRITE_FAILURE_TABLE));
    writeCatalog.asTableCatalog().loadTable(NameIdentifier.of(SCHEMA, WRITE_DATETIME_TABLE));
    truncateWriteCatalog
        .asTableCatalog()
        .loadTable(NameIdentifier.of(SCHEMA, WRITE_TRUNCATE_TABLE));
    writeDenialCatalog.asTableCatalog().loadTable(NameIdentifier.of(SCHEMA, WRITE_DENIED_TABLE));
    dorisWriteDenialCatalog
        .asTableCatalog()
        .loadTable(NameIdentifier.of(SCHEMA, WRITE_DORIS_DENIED_TABLE));
    dorisTruncateDenialCatalog
        .asTableCatalog()
        .loadTable(NameIdentifier.of(SCHEMA, WRITE_DORIS_DENIED_TABLE));

    metalake.addUser(READER);
    List<SecurableObject> grants = new ArrayList<>();
    addReadGrants(grants, CATALOG, mainTables, DENIED_TABLE);
    addReadGrants(grants, PARTITIONED_CATALOG, Collections.singletonList(PARTITIONED_TABLE), null);
    addReadGrants(grants, STRICT_CATALOG, mainTables, DENIED_TABLE);
    addReadGrants(
        grants, STRICT_PARTITIONED_CATALOG, Collections.singletonList(PARTITIONED_TABLE), null);
    addReadGrants(grants, RECORDER_CONTROL_CATALOG, Collections.singletonList(COMMON_TABLE), null);
    addReadGrants(
        grants, STRICT_RECORDER_CONTROL_CATALOG, Collections.singletonList(COMMON_TABLE), null);
    addReadGrants(grants, DIRECT_DENIAL_CATALOG, Collections.emptyList(), null);
    addReadGrants(grants, SQL_DENIAL_CATALOG, Collections.emptyList(), null);
    addReadGrants(grants, STRICT_DENIAL_CATALOG, Collections.emptyList(), null);
    addReadGrants(grants, ARROW_CATALOG, Collections.singletonList(COMMON_TABLE), null);
    addReadGrants(grants, ARROW_FALLBACK_CATALOG, Collections.singletonList(COMMON_TABLE), null);
    addWriteGrants(
        grants,
        WRITE_CATALOG,
        Arrays.asList(
            WRITE_APPEND_TABLE, WRITE_BULK_TABLE, WRITE_FAILURE_TABLE, WRITE_DATETIME_TABLE));
    addWriteGrants(grants, TRUNCATE_WRITE_CATALOG, Collections.singletonList(WRITE_TRUNCATE_TABLE));
    addReadGrants(
        grants, WRITE_DENIAL_CATALOG, Collections.singletonList(WRITE_DENIED_TABLE), null);
    addWriteGrants(
        grants, DORIS_WRITE_DENIAL_CATALOG, Collections.singletonList(WRITE_DORIS_DENIED_TABLE));
    addWriteGrants(
        grants, DORIS_TRUNCATE_DENIAL_CATALOG, Collections.singletonList(WRITE_DORIS_DENIED_TABLE));
    metalake.createRole(READER_ROLE, new HashMap<>(), grants);
    metalake.grantRolesToUser(ImmutableList.of(READER_ROLE), READER);
  }

  private void testVerifiedServerConnection(String catalogName, Map<String, String> properties) {
    try {
      metalake.testConnection(
          catalogName,
          Catalog.Type.RELATIONAL,
          PROVIDER,
          "Verify Gravitino Server JDBC TLS",
          properties);
    } catch (Exception e) {
      throw new IllegalStateException("Gravitino Server strict JDBC TLS verification failed", e);
    }
  }

  private Map<String, String> catalogProperties(boolean partitioned) {
    return catalogProperties(partitioned, doris.internalJdbcUrl());
  }

  private Map<String, String> catalogProperties(boolean partitioned, String jdbcUrl) {
    Map<String, String> properties = new HashMap<>();
    properties.put("jdbc-url", jdbcUrl);
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

  private Map<String, String> recordingCatalogProperties(RecordingDorisTcpProxy.Lane lane) {
    Map<String, String> properties = catalogProperties(false, tcpProxy.jdbcUrl(lane));
    properties.put("gravitino.bypass.maxIdle", "0");
    return properties;
  }

  private Map<String, String> arrowCatalogProperties(RecordingDorisTcpProxy.Lane flightLane) {
    Map<String, String> properties = catalogProperties(false);
    properties.put("doris-fenodes", tcpProxy.feEndpoint());
    properties.put("doris-arrow-flight-sql-mode", "preferred");
    properties.put(
        "doris-arrow-flight-sql-port", Integer.toString(tcpProxy.flightPort(flightLane)));
    return properties;
  }

  private Map<String, String> writeCatalogProperties(
      boolean truncate, boolean recordIo, String user, String password) {
    Map<String, String> properties =
        recordIo
            ? catalogProperties(false, tcpProxy.jdbcUrl(RecordingDorisTcpProxy.Lane.DENIAL))
            : catalogProperties(false);
    properties.put("jdbc-user", user);
    properties.put("jdbc-password", password);
    if (recordIo) {
      properties.put("gravitino.bypass.maxIdle", "0");
    }
    properties.put("doris-write-mode", "batch");
    properties.put("doris-write-overwrite-mode", truncate ? "truncate" : "reject");
    return properties;
  }

  private Map<String, String> strictCatalogProperties(boolean partitioned, String verifiedJdbcUrl) {
    Map<String, String> properties = new HashMap<>();
    properties.put("jdbc-url", verifiedJdbcUrl);
    properties.put("jdbc-driver", "com.mysql.cj.jdbc.Driver");
    properties.put("jdbc-user", DorisTestCluster.TEST_USER);
    properties.put("jdbc-password", DorisTestCluster.TEST_PASSWORD);
    properties.put("credential-providers", "jdbc-user-password");
    properties.put("doris-read-transport", "strict-jdbc-tls");
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

  private Map<String, String> strictRecordingCatalogProperties(
      boolean partitioned, String verifiedJdbcUrl) {
    Map<String, String> properties = strictCatalogProperties(partitioned, verifiedJdbcUrl);
    properties.put("gravitino.bypass.maxIdle", "0");
    return properties;
  }

  private void verifyRecorderControls() {
    RecordingDorisTcpProxy.State serverControl =
        tcpProxy.state(RecordingDorisTcpProxy.Lane.CONTROL);
    assertThat(serverControl.accepted())
        .as("Gravitino metadata must traverse the control TCP proxy")
        .isPositive();

    SparkSession controlSession = spark.newSession();
    CatalogManager controlManager = controlSession.sessionState().catalogManager();
    assertThat(controlManager).isNotSameAs(spark.sessionState().catalogManager());
    assertThat(isCatalogResolved(controlManager, RECORDER_CONTROL_CATALOG)).isFalse();
    try {
      feProxy.reset();
      controlSession
          .table(qualified(RECORDER_CONTROL_CATALOG, COMMON_TABLE))
          .select("id")
          .collectAsList();
      assertThat(feProxy.totalRequestCount())
          .as("native table loading must traverse the FE HTTP recorder")
          .isPositive();

      long acceptedBeforeSql = tcpProxy.state(RecordingDorisTcpProxy.Lane.CONTROL).accepted();
      controlSession
          .sql("SELECT COUNT(*) FROM " + qualified(RECORDER_CONTROL_CATALOG, COMMON_TABLE))
          .collectAsList();
      await()
          .atMost(Duration.ofSeconds(5))
          .untilAsserted(
              () ->
                  assertThat(tcpProxy.state(RecordingDorisTcpProxy.Lane.CONTROL).accepted())
                      .as("Spark SQL lane must traverse the control TCP proxy")
                      .isGreaterThan(acceptedBeforeSql));
      await()
          .atMost(Duration.ofSeconds(5))
          .untilAsserted(
              () ->
                  assertThat(tcpProxy.state(RecordingDorisTcpProxy.Lane.CONTROL).active())
                      .as("positive-control JDBC connections must close before denial tests")
                      .isZero());

      long strictAcceptedBeforeSpark =
          tcpProxy.state(RecordingDorisTcpProxy.Lane.CONTROL).accepted();
      feProxy.reset();
      controlSession
          .table(qualified(STRICT_RECORDER_CONTROL_CATALOG, COMMON_TABLE))
          .select("id")
          .collectAsList();
      assertThat(feProxy.totalRequestCount())
          .as("strict physical schema and reads must not use FE HTTP")
          .isZero();
      await()
          .atMost(Duration.ofSeconds(5))
          .untilAsserted(
              () ->
                  assertThat(tcpProxy.state(RecordingDorisTcpProxy.Lane.CONTROL).accepted())
                      .as("strict Spark schema and reads must traverse JDBC TCP")
                      .isGreaterThan(strictAcceptedBeforeSpark));
      await()
          .atMost(Duration.ofSeconds(5))
          .untilAsserted(
              () ->
                  assertThat(tcpProxy.state(RecordingDorisTcpProxy.Lane.CONTROL).active())
                      .as("strict JDBC connections must close after the positive control")
                      .isZero());
    } finally {
      controlManager.reset();
    }

    RecordingDorisTcpProxy.State denial = tcpProxy.state(RecordingDorisTcpProxy.Lane.DENIAL);
    assertThat(denial.accepted())
        .as("denial catalogs must prove that the dedicated TCP listener is reachable before reset")
        .isPositive();
    assertThat(denial.active()).isZero();
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
            .config(jdbcCatalogPrefix + ".url", doris.internalStrictJdbcUrl())
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

  private static List<String> arrowCircuitPropertyKeys() {
    return System.getProperties().stringPropertyNames().stream()
        .filter(property -> property.startsWith(ARROW_CIRCUIT_PROPERTY_PREFIX))
        .sorted()
        .collect(Collectors.toList());
  }

  private static long arrowAttemptCount() {
    return System.getProperties().stringPropertyNames().stream()
        .filter(property -> property.startsWith(ARROW_ATTEMPT_PROPERTY_PREFIX))
        .mapToLong(property -> Long.parseLong(System.getProperty(property)))
        .sum();
  }

  private long awaitSettledAcceptedConnections(RecordingDorisTcpProxy.Lane lane) {
    await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(
            () -> {
              RecordingDorisTcpProxy.State state = tcpProxy.state(lane);
              assertThat(state.accepted()).isPositive();
              assertThat(state.active()).isZero();
            });
    return tcpProxy.state(lane).accepted();
  }

  private org.apache.spark.sql.connector.catalog.TableCatalog sparkCatalog(String name) {
    return sparkCatalog(spark, name);
  }

  private org.apache.spark.sql.connector.catalog.TableCatalog sparkCatalog(
      SparkSession session, String name) {
    CatalogManager manager = session.sessionState().catalogManager();
    return (org.apache.spark.sql.connector.catalog.TableCatalog) manager.catalog(name);
  }

  @SuppressWarnings("unchecked")
  private static boolean isCatalogResolved(CatalogManager manager, String catalogName) {
    // Spark 3.5 has no side-effect-free public API for this assertion. Keep this test-only private
    // field access as an explicit compatibility watchpoint; the release-certified 3.5.8 real IT
    // must fail if Spark changes the CatalogManager cache layout.
    try {
      Field catalogsField = CatalogManager.class.getDeclaredField("catalogs");
      catalogsField.setAccessible(true);
      scala.collection.mutable.HashMap<String, Object> catalogs =
          (scala.collection.mutable.HashMap<String, Object>) catalogsField.get(manager);
      return catalogs.contains(catalogName);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Unable to inspect the Spark catalog resolution cache", e);
    }
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

  private static void addWriteGrants(
      List<SecurableObject> grants, String catalogName, List<String> tableNames) {
    SecurableObject catalog =
        SecurableObjects.ofCatalog(catalogName, ImmutableList.of(Privileges.UseCatalog.allow()));
    SecurableObject schema =
        SecurableObjects.ofSchema(catalog, SCHEMA, ImmutableList.of(Privileges.UseSchema.allow()));
    grants.add(catalog);
    grants.add(schema);
    tableNames.stream()
        .map(
            tableName ->
                SecurableObjects.ofTable(
                    schema,
                    tableName,
                    ImmutableList.of(
                        Privileges.SelectTable.allow(), Privileges.ModifyTable.allow())))
        .forEach(grants::add);
  }

  private void insertDatetimeRows(String timezone, int firstId) {
    spark.conf().set("spark.sql.session.timeZone", timezone);
    spark
        .sql(
            "INSERT INTO "
                + qualified(WRITE_CATALOG, WRITE_DATETIME_TABLE)
                + " VALUES "
                + "("
                + firstId
                + ", '1969-12-31 23:59:59', '1969-12-31 23:59:59.001', "
                + "'1969-12-31 23:59:59.000001'), "
                + "("
                + (firstId + 1)
                + ", '2024-02-29 12:34:56', '2024-02-29 12:34:56.123', "
                + "'2024-02-29 12:34:56.123456'), "
                + "("
                + (firstId + 2)
                + ", '2024-03-10 02:30:00', '2024-11-03 01:30:00.999', "
                + "'2024-11-03 01:30:00.999999'), "
                + "("
                + (firstId + 3)
                + ", NULL, NULL, NULL)")
        .collectAsList();
  }

  private List<String> insertTimestampDatetimeRow(String timezone, int id) throws Exception {
    spark.conf().set("spark.sql.session.timeZone", timezone);
    Dataset<Row> input =
        spark.sql(
            "SELECT "
                + id
                + " AS id, CAST('1969-12-31 23:59:59' AS TIMESTAMP) AS dt0, "
                + "CAST('2024-11-03 01:30:00.999' AS TIMESTAMP) AS dt3, "
                + "CAST('2024-03-10 01:59:59.999999' AS TIMESTAMP) AS dt6");
    List<String> expected =
        sparkRows(
            input.selectExpr(
                "id", "CAST(dt0 AS STRING)", "CAST(dt3 AS STRING)", "CAST(dt6 AS STRING)"));
    input.writeTo(qualified(WRITE_CATALOG, WRITE_DATETIME_TABLE)).append();
    return expected;
  }

  private void assertTypeContractProbe(TypeContractProbe probe) throws Exception {
    TypeContractProbeResult result = typeContractResults.get(probe.name);
    assertThat(result).as("missing type probe result for %s", probe.ddlType).isNotNull();
    boolean expectedDdlSupport = probe.supports(doris.version());
    assertThat(result.ddlSucceeded)
        .as(
            "%s DDL support on Doris %s; failure=%s",
            probe.ddlType, doris.version(), result.ddlFailure)
        .isEqualTo(expectedDdlSupport);

    if (!expectedDdlSupport) {
      assertThat(result.insertSucceeded).isFalse();
      assertThat(result.logicalType).isNull();
      assertThat(result.ddlFailure).isEqualTo(probe.expectedDdlFailure);
      return;
    }

    assertThat(result.insertSucceeded)
        .as(
            "%s insert support on Doris %s; failure=%s",
            probe.ddlType, doris.version(), result.insertFailure)
        .isTrue();
    assertThat(result.feTypeName).isEqualToIgnoringCase(probe.expectedFeTypeName);
    assertThat(result.providerFailure).isNull();
    assertThat(result.logicalType)
        .as("%s logical type from FE type %s", probe.ddlType, result.feTypeName)
        .isEqualTo(probe.expectedLogicalType);

    Dataset<Row> frame =
        spark
            .table(qualified(CATALOG, probe.tableName()))
            .select("id", "probe_value")
            .orderBy("id");
    assertThat(frame.schema().apply("probe_value").dataType()).isEqualTo(probe.expectedSparkType);
    assertThat(sparkRows(frame))
        .containsExactlyElementsOf(
            jdbcRows(
                "SELECT id, probe_value FROM `"
                    + SCHEMA
                    + "`.`"
                    + probe.tableName()
                    + "` ORDER BY id"));
    assertThat(frame.rdd().getNumPartitions()).isEqualTo(1);
    String plan = frame.queryExecution().executedPlan().toString();
    if (probe.requiresSqlExecution) {
      assertThat(plan).contains("JDBCScan").doesNotContain("DorisScanV2");
    } else {
      assertThat(plan).contains("DorisScanV2").doesNotContain("JDBCRelation");
    }
  }

  private Map<String, String> jdbcColumnTypes(String table) throws Exception {
    try (Connection connection =
            DriverManager.getConnection(
                doris.hostJdbcUrl(), DorisTestCluster.TEST_USER, DorisTestCluster.TEST_PASSWORD);
        Statement statement = connection.createStatement()) {
      return jdbcColumnTypes(statement, table);
    }
  }

  private void restoreDriftTable(
      org.apache.spark.sql.connector.catalog.TableCatalog sparkCatalog,
      Identifier identifier,
      String qualifiedTable)
      throws Exception {
    try {
      if (jdbcColumnTypes(DRIFT_TABLE).containsKey("drifted")) {
        try (Connection connection =
                DriverManager.getConnection(
                    doris.hostJdbcUrl(),
                    DorisTestCluster.TEST_USER,
                    DorisTestCluster.TEST_PASSWORD);
            Statement statement = connection.createStatement()) {
          statement.execute("ALTER TABLE " + qualifiedTable + " DROP COLUMN drifted");
        }
      }
      await()
          .atMost(Duration.ofSeconds(30))
          .untilAsserted(
              () -> assertThat(jdbcColumnTypes(DRIFT_TABLE)).doesNotContainKey("drifted"));
    } finally {
      sparkCatalog.invalidateTable(identifier);
    }
  }

  private void logTypeContractEvidence() throws Exception {
    List<String> probes =
        typeContractResults.values().stream()
            .map(
                result ->
                    String.format(
                        Locale.ROOT,
                        "%s{ddl=%s,insert=%s,desc=%s,logical=%s,ddlFailure=%s,insertFailure=%s}",
                        result.probe.ddlType,
                        result.ddlSucceeded,
                        result.insertSucceeded,
                        result.feTypeName,
                        result.logicalType == null ? null : result.logicalType.simpleString(),
                        result.ddlFailure,
                        result.insertFailure))
            .collect(Collectors.toList());
    LOG.info(
        "Doris {} type contract evidence: commonDesc={}, commonLogical={}, normalizedDesc={}, "
            + "normalizedLogical={}, extendedDesc={}, extendedLogical={}, sketchDesc={}, "
            + "sketchLogical={}, probes={}",
        doris.version(),
        jdbcColumnTypes(COMMON_TABLE),
        governedColumnTypes(COMMON_TABLE),
        jdbcColumnTypes(NORMALIZED_TABLE),
        governedColumnTypes(NORMALIZED_TABLE),
        jdbcColumnTypes(EXTENDED_TABLE),
        governedColumnTypes(EXTENDED_TABLE),
        jdbcColumnTypes(SKETCH_TABLE),
        governedColumnTypes(SKETCH_TABLE),
        probes);
  }

  private Map<String, String> governedColumnTypes(String table) {
    org.apache.gravitino.rel.Table governedTable =
        governedClient
            .loadCatalog(CATALOG)
            .asTableCatalog()
            .loadTable(NameIdentifier.of(SCHEMA, table));
    Map<String, String> logicalTypes = new LinkedHashMap<>();
    Arrays.stream(governedTable.columns())
        .forEach(column -> logicalTypes.put(column.name(), column.dataType().simpleString()));
    return logicalTypes;
  }

  private static Map<String, String> jdbcColumnTypes(Statement statement, String table)
      throws Exception {
    Map<String, String> columnTypes = new LinkedHashMap<>();
    try (ResultSet columns = statement.executeQuery("DESC `" + SCHEMA + "`.`" + table + "`")) {
      while (columns.next()) {
        columnTypes.put(columns.getString(1).toLowerCase(Locale.ROOT), columns.getString(2));
      }
    }
    return columnTypes;
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

  private List<String> verifiedJdbcRows(String jdbcUrl, String sql) throws Exception {
    List<String> rows = new ArrayList<>();
    try (Connection connection =
            DriverManager.getConnection(
                jdbcUrl, DorisTestCluster.TEST_USER, DorisTestCluster.TEST_PASSWORD);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql)) {
      ResultSetMetaData metadata = resultSet.getMetaData();
      while (resultSet.next()) {
        List<String> values = new ArrayList<>(metadata.getColumnCount());
        for (int index = 1; index <= metadata.getColumnCount(); index++) {
          String value = resultSet.getString(index);
          values.add(value == null ? NULL_VALUE : value);
        }
        rows.add(String.join(",", values));
      }
    }
    return rows;
  }

  private void configureSparkTrustStore(Path trustStore) {
    originalTrustStore = System.getProperty("javax.net.ssl.trustStore");
    originalTrustStoreType = System.getProperty("javax.net.ssl.trustStoreType");
    trustStoreConfigured = true;
    System.setProperty("javax.net.ssl.trustStore", trustStore.toAbsolutePath().toString());
    System.setProperty("javax.net.ssl.trustStoreType", "JKS");
  }

  private void restoreSparkTrustStore() {
    if (!trustStoreConfigured) {
      return;
    }
    setOrClearTrustStoreProperty("javax.net.ssl.trustStore", originalTrustStore);
    setOrClearTrustStoreProperty("javax.net.ssl.trustStoreType", originalTrustStoreType);
    originalTrustStore = null;
    originalTrustStoreType = null;
    trustStoreConfigured = false;
  }

  private static void setOrClearTrustStoreProperty(String name, String value) {
    if (value == null) {
      System.clearProperty(name);
    } else {
      System.setProperty(name, value);
    }
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

  private static void createWriteTable(Statement statement, String table) throws Exception {
    statement.execute(
        "CREATE TABLE `"
            + SCHEMA
            + "`.`"
            + table
            + "` (id INT NOT NULL, label VARCHAR(64), event_time DATETIME(6), "
            + "amount DECIMAL(18,3)) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 2 "
            + "PROPERTIES ('replication_num'='1')");
  }

  private void assertTcpLaneRemainsZero(RecordingDorisTcpProxy.Lane lane, long generation) {
    await()
        .during(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(3))
        .untilAsserted(
            () -> {
              RecordingDorisTcpProxy.State state = tcpProxy.state(lane);
              assertThat(state.generation()).isEqualTo(generation);
              assertThat(state.accepted()).isZero();
              assertThat(state.active()).isZero();
            });
  }

  private void createTypeContractProbe(Statement statement, TypeContractProbe probe) {
    TypeContractProbeResult result = new TypeContractProbeResult(probe);
    typeContractResults.put(probe.name, result);
    try {
      statement.execute(
          "CREATE TABLE `"
              + SCHEMA
              + "`.`"
              + probe.tableName()
              + "` (id INT NOT NULL, probe_value "
              + probe.ddlType
              + ") DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 "
              + "PROPERTIES ('replication_num'='1')");
      result.ddlSucceeded = true;
      result.feTypeName = jdbcColumnTypes(statement, probe.tableName()).get("probe_value");
    } catch (SQLException e) {
      result.ddlFailure = sqlFailureCategory(e);
      return;
    } catch (Exception e) {
      throw new IllegalStateException("Unable to inspect type probe " + probe.ddlType, e);
    }

    try {
      statement.executeUpdate(
          "INSERT INTO `"
              + SCHEMA
              + "`.`"
              + probe.tableName()
              + "` VALUES (1, "
              + probe.insertLiteral
              + "), (2, NULL)");
      result.insertSucceeded = true;
    } catch (SQLException e) {
      result.insertFailure = sqlFailureCategory(e);
    }
  }

  private static String sqlFailureCategory(SQLException exception) {
    return "SQLState=" + exception.getSQLState() + ", errorCode=" + exception.getErrorCode();
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

  private static final class TypeContractProbe {
    private final String name;
    private final String ddlType;
    private final String insertLiteral;
    private final boolean supportedOnDoris3;
    private final boolean supportedOnDoris4;
    private final String expectedFeTypeName;
    private final Type expectedLogicalType;
    private final DataType expectedSparkType;
    private final boolean requiresSqlExecution;
    private final String expectedDdlFailure;

    private TypeContractProbe(
        String name,
        String ddlType,
        String insertLiteral,
        boolean supportedOnDoris3,
        boolean supportedOnDoris4,
        String expectedFeTypeName,
        Type expectedLogicalType,
        DataType expectedSparkType,
        boolean requiresSqlExecution,
        String expectedDdlFailure) {
      this.name = name;
      this.ddlType = ddlType;
      this.insertLiteral = insertLiteral;
      this.supportedOnDoris3 = supportedOnDoris3;
      this.supportedOnDoris4 = supportedOnDoris4;
      this.expectedFeTypeName = expectedFeTypeName;
      this.expectedLogicalType = expectedLogicalType;
      this.expectedSparkType = expectedSparkType;
      this.requiresSqlExecution = requiresSqlExecution;
      this.expectedDdlFailure = expectedDdlFailure;
    }

    private static TypeContractProbe supported(
        String name,
        String ddlType,
        String insertLiteral,
        String expectedFeTypeName,
        Type expectedLogicalType,
        DataType expectedSparkType) {
      return new TypeContractProbe(
          name,
          ddlType,
          insertLiteral,
          true,
          true,
          expectedFeTypeName,
          expectedLogicalType,
          expectedSparkType,
          false,
          null);
    }

    private static TypeContractProbe normalized(
        String name,
        String ddlType,
        String insertLiteral,
        String expectedFeTypeName,
        Type expectedLogicalType,
        DataType expectedSparkType) {
      return new TypeContractProbe(
          name,
          ddlType,
          insertLiteral,
          true,
          true,
          expectedFeTypeName,
          expectedLogicalType,
          expectedSparkType,
          true,
          null);
    }

    private static TypeContractProbe versionedNormalized(
        String name,
        String ddlType,
        String insertLiteral,
        boolean supportedOnDoris3,
        boolean supportedOnDoris4,
        String expectedFeTypeName,
        Type expectedLogicalType,
        DataType expectedSparkType) {
      return new TypeContractProbe(
          name,
          ddlType,
          insertLiteral,
          supportedOnDoris3,
          supportedOnDoris4,
          expectedFeTypeName,
          expectedLogicalType,
          expectedSparkType,
          true,
          "SQLState=42000, errorCode=1235");
    }

    private static TypeContractProbe ddlRejected(
        String name, String ddlType, String insertLiteral) {
      return new TypeContractProbe(
          name,
          ddlType,
          insertLiteral,
          false,
          false,
          null,
          null,
          null,
          false,
          "SQLState=HY000, errorCode=1105");
    }

    private boolean supports(String version) {
      return version.startsWith("4.") ? supportedOnDoris4 : supportedOnDoris3;
    }

    private String tableName() {
      return "type_probe_" + name;
    }
  }

  private static final class TypeContractProbeResult {
    private final TypeContractProbe probe;
    private boolean ddlSucceeded;
    private boolean insertSucceeded;
    private String feTypeName;
    private String ddlFailure;
    private String insertFailure;
    private Type logicalType;
    private String providerFailure;

    private TypeContractProbeResult(TypeContractProbe probe) {
      this.probe = probe;
    }
  }
}
