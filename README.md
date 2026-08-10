# Gravitino Doris Spark Connector

A standalone, governed Apache Doris reader for Apache Spark and Apache Gravitino. It combines
Gravitino authorization and credential vending with the official Apache Doris Spark Connector and
Spark's JDBC V2 planner.

The project is independent of the Apache Gravitino source tree. It depends only on published Maven
Central artifacts and does not patch or shadow `org.apache.gravitino` classes.

## Supported matrix

| Component | Supported version |
| --- | --- |
| Apache Gravitino client/server | 1.3.0 |
| Apache Spark | 3.5.x; release-certified on 3.5.8 |
| Scala | 2.12 |
| Apache Doris Spark Connector | 26.0.0 |
| Apache Doris | 3.0.6.2 and 4.0.6 |
| Java | 17 |

Spark 3.5.8 is the default compile and test version and runs the complete Doris 3.0.6.2 and 4.0.6
integration matrix. Spark 3.5.0 and 3.5.9 run compile, unit, class-loading, integration-test source
compilation, and distribution compatibility smoke tests. Those boundary smokes do not constitute
full Doris integration certification for every Spark 3.5 patch.

The initial release supports governed batch reads of Doris tables. Gravitino views, Spark writes,
streaming writes, and Spark DDL are deliberately rejected. The facade already contains
policy-gated write and mutation entry points, so later implementations replace delegates and
capabilities without replacing the catalog, table facade, plugin, authorization order, or
credential boundary.

## Why this exists

The generic Spark JDBC path can read many Doris scalar tables, but it does not provide the Doris
Connector's tablet reader and it inherits JDBC metadata limitations for Doris-specific types. This
connector adds:

- a `SELECT_TABLE` authorization check before any Doris request;
- vended JDBC credentials applied after all user-controlled options;
- native, parallel Doris tablet reads for lossless detail scans;
- aggregate, Top-N, limit, and offset pushdown through Spark's own JDBC V2 planner;
- the same JDBC partition tuple and fetch-size controls as a direct Spark JDBC read;
- stable String semantics for DATETIME, complex, unsigned, sketch, binary, wide-decimal, and
  future FE-reported Doris types;
- bounded physical-schema caching with precise `REFRESH TABLE` invalidation;
- a read-only capability facade that prevents write-capability leakage from the native delegate.

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

The root of that directory also contains the pinned runtime and license inventory from
[DEPENDENCIES.md](DEPENDENCIES.md).

Copy the contents of `gravitino/catalogs/doris-governed/` to
`$GRAVITINO_HOME/catalogs/doris-governed/`. Add every jar under `spark/jars/` to both the Spark
driver and executor classpaths.

## Create a governed catalog

Create a relational Gravitino catalog with provider `doris-governed`. A representative property
set is:

```properties
jdbc-url=jdbc:mysql://doris-fe:9030/
jdbc-driver=com.mysql.cj.jdbc.Driver
jdbc-user=gravitino_reader
jdbc-password=${SECRET_FROM_DEPLOYMENT_SYSTEM}
doris-fenodes=doris-fe:8030
doris-query-port=9030
credential-providers=jdbc-user-password
doris-schema-cache-ttl-ms=30000
doris-schema-cache-max-entries=1000
```

Both `doris-fenodes` and `doris-query-port` are required. Connector 26.0.0 otherwise defers the
missing query-port failure until native scan planning, so this adapter validates it at catalog
creation and initialization instead.

The `jdbc-url` serves both Gravitino metadata access and Spark's SQL lane, so it must be reachable
from the Gravitino server, Spark driver, and executors.

`jdbc-user` and `jdbc-password` are hidden Gravitino properties. Do not place them in Spark catalog
options or `spark.bypass.*`. The Spark adapter accepts credentials only from Gravitino credential
vending.

## Start Spark

```bash
spark-shell \
  --jars "$(find /path/to/distribution/spark/jars -name '*.jar' -print | paste -sd, -)" \
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

## Type behavior

Lossless scalar values retain Catalyst types. Doris DATETIME/DATETIMEV2, LARGEINT, unsigned values,
complex values, JSON/JSONB, VARIANT, IP addresses, wide decimals, and unknown FE-reported types are
exposed as String. BINARY and VARBINARY use base64; BITMAP and HLL use Doris base64 functions. This
avoids rejecting an entire table because one readable Doris type has no stable Catalyst
representation.

String normalization is intentionally directional. Column count, order, case-insensitive names,
and nullability safety still fail closed. Lossless scalar types remain validated against Gravitino
metadata. For complex, sketch, wide-decimal, and future types whose JDBC metadata is lossy, the FE
type is authoritative for the String/base64 execution representation.

## Build and test

Use Java 17:

```bash
./gradlew spotlessCheck test installDist verifySparkDependencyVersions
```

Compatibility checks for the currently verified Spark 3.5 boundaries run in separate Gradle
processes without starting Doris:

```bash
./gradlew test installDist :integration-tests:compileIntegrationTestJava \
  verifySparkDependencyVersions -PsparkVersion=3.5.0
./gradlew test installDist :integration-tests:compileIntegrationTestJava \
  verifySparkDependencyVersions -PsparkVersion=3.5.9
```

Real-infrastructure tests run embedded Spark 3.5.8 plus Docker-managed Gravitino and Doris:

```bash
./gradlew integrationTest -PdorisVersion=3.0.6.2
./gradlew integrationTest -PdorisVersion=4.0.6
```

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
