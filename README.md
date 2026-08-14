# Gravitino Doris Spark Connector

A standalone, governed Apache Doris batch connector for Apache Spark and Apache Gravitino. It
combines Gravitino authorization and credential vending with the official Apache Doris Spark
Connector and Spark's JDBC V2 planner.

The project is independent of the Apache Gravitino source tree. It depends only on published Maven
Central artifacts and does not patch or shadow `org.apache.gravitino` classes.

## Supported matrix

| Component | Supported version |
| --- | --- |
| Apache Gravitino client/server | 1.3.0 |
| Apache Spark | reads on 3.5.x; batch writes on 3.5.3+; release-certified on 3.5.8 |
| Scala | 2.12 |
| Apache Doris Spark Connector | 26.0.0 |
| Apache Doris | 3.0.6.2 and 4.0.6 |
| Java | 17 |

Spark 3.5.8 is the default compile and test version and runs the complete Doris 3.0.6.2 and 4.0.6
integration matrix. Spark 3.5.0 and 3.5.9 run compile, unit, class-loading, integration-test source
compilation, and distribution compatibility smoke tests. Those boundary smokes do not constitute
full Doris integration certification for every Spark 3.5 patch.

The connector supports governed batch reads and opt-in batch append on Spark 3.5.3 or newer.
Spark 3.5.0 through 3.5.2 remain read-only. Batch write, Arrow Flight SQL, and truncate overwrite
are all disabled by default. Gravitino views, streaming writes, predicate/dynamic overwrite, Spark
DDL, CTAS, UPDATE, DELETE, and MERGE are deliberately rejected.

## Why this exists

The generic Spark JDBC path can read many Doris scalar tables, but it does not provide the Doris
Connector's tablet reader and it inherits JDBC metadata limitations for Doris-specific types. This
connector adds:

- a `SELECT_TABLE` authorization check before native table loading or JDBC table materialization;
- fail-closed, channel-specific JDBC URL, `connectionProperties`, and DBCP allow-lists on both the
  Gravitino server and Spark sides;
- an explicit `strict-jdbc-tls` profile that verifies CA and hostname on one JDBC-only transport;
- vended JDBC credentials applied after all user-controlled options;
- native, parallel Doris tablet reads for lossless detail scans;
- an experimental, catalog-managed Arrow Flight SQL mode with safe first-row fallback to the same
  partition's Thrift reader and an application-scoped fail-sticky circuit;
- aggregate, Top-N, limit, and offset pushdown through Spark's own JDBC V2 planner;
- the same JDBC partition tuple and fetch-size controls as a direct Spark JDBC read;
- stable String/base64 semantics for the explicitly verified DATETIME, complex, JSON, VARIANT,
  IP, LARGEINT, sketch, and Doris 4 wide-decimal types;
- bounded physical-schema caching with precise invalidation and one safe stale-hit revalidation;
- a capability facade that exposes only certified batch read/write operations;
- opt-in batch append through the official Doris Stream Load writer with forced 2PC, strict mode,
  zero filter tolerance, schemaless disabled, and automatic redirect disabled.

See [Architecture](docs/architecture.md) for the execution and trust boundaries, and
[Migration from JDBC](docs/migration-from-jdbc.md) for a direct comparison.

## Distribution layout

Run:

```bash
./gradlew installDist
```

The generated directory is
`distribution/build/install/gravitino-doris-spark-connector/`:

```text
spark/jars/                                  Spark-side runtime jars
gravitino/catalogs/doris-governed/libs/      Gravitino provider jars
gravitino/catalogs/doris-governed/conf/      Provider configuration
```

The root of that directory also contains the runtime and license inventory from
[DEPENDENCIES.md](DEPENDENCIES.md). `installDist`, `distTar`, and `distZip` contain identical file
sets and deliberately contain neither a MySQL Connector/J file nor
`com/mysql/cj/jdbc/Driver.class` in any JAR.

Copy the contents of `gravitino/catalogs/doris-governed/` to
`$GRAVITINO_HOME/catalogs/doris-governed/`. MySQL Connector/J is an external deployment
prerequisite, not a redistributed project component. This project tests
`com.mysql:mysql-connector-j:8.0.33`:

- install the Driver JAR in `$GRAVITINO_HOME/catalogs/doris-governed/libs` before starting a
  non-containerized Gravitino Server;
- for the official Gravitino 1.3.0 image, mount a controlled directory containing the selected
  Driver, plus any separately audited JDBC drivers required by other catalogs, at
  `/opt/gravitino/jdbc-drivers`; the image entrypoint recognizes the legacy filename pattern
  `mysql-connector-java-*.jar`;
- add the same Driver JAR and every JAR under `spark/jars/` to the Spark driver and every executor
  classpath with `--jars`, `spark.jars`, or the equivalent cluster-manager mechanism.

The server and Spark adapter fail with fixed, redacted installation guidance when the Driver class
is unavailable before physical JDBC catalog construction. Executor availability is a deployment
responsibility and is exercised when executor-side JDBC/native code loads its dependencies.

## Create a governed catalog

Create a relational Gravitino catalog with provider `doris-governed`. The compatible default
profile is `hybrid`:

```properties
jdbc-url=jdbc:mysql://doris-fe:9030/
jdbc-driver=com.mysql.cj.jdbc.Driver
jdbc-user=gravitino_reader
jdbc-password=${SECRET_FROM_DEPLOYMENT_SYSTEM}
doris-read-transport=hybrid
doris-fenodes=doris-fe:8030
doris-query-port=9030
credential-providers=jdbc-user-password
doris-arrow-flight-sql-mode=disabled
doris-write-mode=disabled
doris-write-overwrite-mode=reject
doris-schema-cache-ttl-ms=30000
doris-schema-cache-max-entries=1000
```

Both `doris-fenodes` and `doris-query-port` are required by `hybrid`. Connector 26.0.0 otherwise defers the
missing query-port failure until native scan planning, so this adapter validates it at catalog
creation and initialization instead.

The `jdbc-url` serves both Gravitino metadata access and Spark's SQL lane, so it must be reachable
from the Gravitino server, Spark driver, and executors.

`jdbc-user` and `jdbc-password` are hidden Gravitino properties. Do not place them in Spark catalog
options or `spark.bypass.*`. The Spark adapter accepts credentials only from Gravitino credential
vending.

## JDBC security boundary

The driver must be exactly `com.mysql.cj.jdbc.Driver`. The URL must use ordinary, single-host
`jdbc:mysql://host:port` syntax with an explicit port and at most one database path. Multi-host,
load-balance, replication, DNS SRV, host-property, embedded-credential, malformed, and
non-converging encoded forms fail closed. Known dangerous Connector/J parameters and DBCP
class-loading, identity-override, and eager-initialization properties are rejected in URL queries,
raw catalog keys, `gravitino.bypass.*`, and parsed `connectionProperties`.

Authorization-denial integration tests use fresh Spark catalog managers and directly observe both
FE HTTP requests and MySQL/JDBC TCP connections remaining at zero. They do not packet-capture Doris
BE/native ports; the absence of later BE work follows from the tested ordering and the fact that no
physical table is loaded and no table delegate, scan, or reader is constructed.

For a verified JDBC-only transport, omit both native endpoint properties and select the strict
profile:

```properties
jdbc-url=jdbc:mysql://doris-fe:9030/?sslMode=VERIFY_IDENTITY
jdbc-driver=com.mysql.cj.jdbc.Driver
jdbc-user=gravitino_reader
jdbc-password=${SECRET_FROM_DEPLOYMENT_SYSTEM}
doris-read-transport=strict-jdbc-tls
credential-providers=jdbc-user-password
```

Deploy the same trusted CA through the JVM truststore of the Gravitino Server, Spark driver, and
every executor. The current strict contract uses Connector/J's system-truststore fallback, so keep
`fallbackToSystemTrustStore` absent or set it exactly to `true`. Do not put a truststore location or
password in the JDBC URL or catalog. This profile structurally excludes the native Doris catalog,
FE HTTP schema requests, and tablet scan; physical schema and all reads use the canonical verified
JDBC URL. The default `hybrid` profile remains non-strict because its native channel is not covered
by this JDBC trust boundary.

## Start Spark

```bash
spark-shell \
  --jars "/path/to/mysql-connector-j-8.0.33.jar,$(find /path/to/distribution/spark/jars -name '*.jar' -print | paste -sd, -)" \
  --conf spark.plugins=io.github.jiangxt2.gravitino.doris.spark.GovernedDorisSparkPlugin \
  --conf spark.sql.gravitino.uri=http://gravitino:8090 \
  --conf spark.sql.gravitino.metalake=production
```

The wrapper plugin first runs the official Gravitino Spark plugin and then registers only catalogs
whose provider is exactly `doris-governed`. Existing Gravitino catalogs and `jdbc-doris` behavior
are unchanged.

Use a three-part identifier or set the current namespace:

```sql
SELECT * FROM governed_doris.analytics.orders;

USE governed_doris.analytics;
SELECT * FROM orders;
```

Two-part access without a current schema fails with an actionable error; the official Doris
catalog has no reliable default namespace.

## Optional Arrow and batch write

Arrow Flight SQL is experimental and remains off unless the server-managed catalog sets both:

```properties
doris-arrow-flight-sql-mode=preferred
doris-arrow-flight-sql-port=8070
```

The port cannot be supplied or overridden through Spark options. The adapter fixes official FE
auto-discovery to `false`, probes the catalog-managed endpoint with a bounded connect timeout, and
falls back to the same partition's Thrift reader only for classified transport failures before any
row is delivered. Once one such failure opens the hashed application/endpoint circuit, later
partitions in that Spark application bypass Arrow. The circuit intentionally does not reset in a
long-lived session; start a new Spark application to retry a recovered endpoint. Arrow uses the
official connector's insecure gRPC location and is therefore incompatible with
`strict-jdbc-tls`.

Enable governed batch append on Spark 3.5.3 or newer with:

```properties
doris-write-mode=batch
doris-write-overwrite-mode=reject
```

The Spark principal needs Gravitino `MODIFY_TABLE`; the Doris technical user needs the data-plane
privileges required by Stream Load. The connector forces the reviewed Stream Load parameters and
does not claim one job-wide atomic transaction across all writer partitions. `append()` is the
default supported operation. Setting `doris-write-overwrite-mode=truncate` additionally permits
only Spark's full-table truncate overwrite. The official connector first issues Doris SQL
`TRUNCATE TABLE` and then starts the load, so a later load failure can leave the table empty or
partially loaded.

## Type behavior

In the compatible `hybrid` profile, lossless scalar values retain Catalyst types. Doris
DATETIME/DATETIMEV2, ARRAY, MAP, STRUCT,
LARGEINT, JSON/JSONB, VARIANT, IP addresses, and the verified Doris 4 wide-decimal form are exposed
as String. BITMAP and HLL use Doris base64 functions. BINARY, VARBINARY, TIME, unsigned integer DDL,
and the explicit legacy DECIMAL family names are probe-only because both certified Doris images
reject their test DDL; they are not part of the released read guarantee.

String normalization is intentionally directional. Column count, order, case-insensitive names,
and nullability safety still fail closed. Lossless scalar types remain validated against Gravitino
metadata. Only the matrix-listed complex, sketch, wide-decimal, JSON, VARIANT, and IP mappings whose
JDBC metadata is proven lossy may use the FE type as the String/base64 execution authority. An
unlisted or future FE type fails closed instead of being silently promoted to String support. See
the [type contract evidence matrix](docs/testing.md#type-contract-evidence).

The strict profile never falls back to FE metadata. Its physical schema comes exclusively from
JDBC `DatabaseMetaData`; the release IT exercises the lossless scalar tables used by strict reads.
The FE-aware Doris-specific normalization matrix above remains a `hybrid` contract until each lossy
family has an independent JDBC-only schema/value fixture.

Writes require exact column count, order, case-sensitive names, safe nullability, and the certified
lossless Catalyst type. Normalized types remain read-only except Doris DATETIME. DATETIME accepts
the exact precision-specific String grammar `yyyy-MM-dd HH:mm:ss[.fraction]` for precision 0..6;
Spark Timestamp inputs follow Spark's standard assignment cast using the write-analysis
`spark.sql.session.timeZone` before the same row validation. Both paths are round-trip tested on
the two certified Doris versions.

## Build and test

Use Java 17:

```bash
./gradlew spotlessCheck test installDist \
  :distribution:resolveDistributionLocks \
  :distribution:verifyDistributionDependencyContract \
  verifySparkDependencyVersions
```

Compatibility checks for the currently verified Spark 3.5 boundaries run in separate Gradle
processes without starting Doris:

```bash
./gradlew test installDist :integration-tests:compileIntegrationTestJava \
  :distribution:verifyDistributionDependencyContract \
  verifySparkDependencyVersions -PsparkVersion=3.5.0
./gradlew test installDist :integration-tests:compileIntegrationTestJava \
  :distribution:verifyDistributionDependencyContract \
  verifySparkDependencyVersions -PsparkVersion=3.5.9
```

Real-infrastructure tests run embedded Spark 3.5.8 plus Docker-managed Gravitino and Doris:

```bash
./gradlew integrationTest -PdorisVersion=3.0.6.2
./gradlew integrationTest -PdorisVersion=4.0.6
```

The opt-in performance harness uses one cluster-mode driver and two executors placed on three
separate Spark Standalone workers. The default matrix is capped at ten million rows:

```bash
./gradlew performanceTest -PdorisVersion=4.0.6
```

It writes an atomic manifest, a redacted Spark-submit log, and Spark event logs under
`integration-tests/build/performance-results/`. Performance evidence is workload-specific; the
project does not claim that Arrow or every governed lane is universally faster than JDBC.

On macOS, start `mac-docker-connector` before the Docker tests. The tests use the routed
`10.20.30.0/28` subnet and apply Docker Engine 28's `nat-unprotected` gateway mode only to that
dedicated test network. They never restart Docker or run a prune operation. See
[Testing](docs/testing.md).

## Documentation

- [Architecture](docs/architecture.md)
- [Configuration and security](docs/configuration.md)
- [Migration from Spark JDBC](docs/migration-from-jdbc.md)
- [Testing](docs/testing.md)

## License

Apache License 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
