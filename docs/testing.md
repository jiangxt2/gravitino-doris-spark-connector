# Testing

## Local toolchain

The build requires Java 17. On the development machine used for this project:

```bash
export JAVA_HOME="$HOME/Library/Java/JavaVirtualMachines/corretto-17.0.18/Contents/Home"
java -version
./gradlew --version
```

## Static and unit gates

```bash
./gradlew spotlessCheck test installDist rat \
  :distribution:resolveDistributionLocks \
  :distribution:verifyDistributionDependencyContract \
  verifySparkDependencyVersions
```

Java compilation includes Error Prone and `-Xlint:all -Werror`; RAT checks Java, Gradle, and
comment-capable configuration sources with narrow format/generated-file exclusions. These tasks
cover property protection, credential resolution, explicit authorization ordering,
schema/type compatibility, cache behavior, hybrid planner state, capability filtering, plugin
registration, provider loading, distribution assembly, repository container-label configuration,
resolved Spark-module version consistency, immutable workflow action references, and the shared
JDBC URL/driver/DBCP security matrix. They also cover Arrow fallback state/classification/circuit
behavior, patch-aware write capabilities, forced sink options, write-schema validation, and direct
rejection of unsupported write operations. The distribution contract compares `installDist`, tar,
and zip file sets, rejects target-provided libraries, and scans every JAR for the MySQL Driver
class.

`distribution/gradle.lockfile` strictly locks the three production distribution configurations.
`gradle/verification-metadata.xml` enables Gradle SHA-256 verification automatically for plugins,
artifacts, and metadata. The standalone CI lane uses a fresh `GRADLE_USER_HOME`, so a locally cached
artifact cannot bypass that checksum gate.

## Spark compatibility gates

Spark 3.5.8 is the default compile and test version. The lower and current upper compatibility
boundaries run in separate Gradle processes:

```bash
./gradlew test installDist :integration-tests:compileIntegrationTestJava \
  :distribution:verifyDistributionDependencyContract \
  verifySparkDependencyVersions -PsparkVersion=3.5.0
./gradlew test installDist :integration-tests:compileIntegrationTestJava \
  :distribution:verifyDistributionDependencyContract \
  verifySparkDependencyVersions -PsparkVersion=3.5.9
```

These commands compile the integration-test source set but do not start Docker. They verify API
linkage, class loading, the packaged connector distribution, and the complete resolved
`org.apache.spark` module set. They do not certify real Doris behavior on those two boundary
versions. The full real-infrastructure certification remains pinned to Spark 3.5.8.

Spark 3.5.0 through 3.5.2 are read-only. Spark 3.5.3 is the minimum write-aware API boundary and is
validated with a real append/DATETIME smoke in addition to source compilation. Spark 3.5.8 remains
the only full release/runtime matrix, while 3.5.9 is the current upper compile/unit/distribution
compatibility boundary.

## Real-infrastructure gates

```bash
./gradlew integrationTest -PdorisVersion=3.0.6.2
./gradlew integrationTest -PdorisVersion=4.0.6
```

Each invocation starts:

- embedded Spark 3.5.8 using the combined plugin;
- `apache/gravitino:1.3.0` with the built independent provider mounted into its isolated catalog
  directory;
- official split Doris FE and BE images for the selected version;
- an FE HTTP recording proxy that stores only request method and path;
- a digest-pinned, JDK-only transparent TCP proxy with independent control and denial listeners.

The Gravitino image contains its own Connector/J 8.0.27. The harness masks the image's
`/opt/gravitino/jdbc-drivers` directory in both paths: the negative server receives an empty
read-only directory, while the positive server receives a generated test directory containing only
the resolved 8.0.33 Driver. That directory is under `integration-tests/build/tmp`, outside every
production distribution form. The negative Admin API catalog creation must fail with fixed Driver
installation guidance while FE HTTP and MySQL/JDBC TCP counters remain zero.

The tests verify direct-result parity, planner pushdown, four-partition JDBC parity, type
normalization, authorization-before-I/O, cache/refresh, unsupported timestamp mutation before
physical schema change, credential redaction, JDBC configuration rejection, capability
boundaries, strict JDBC TLS transport, Arrow/Thrift fallback, and governed Stream Load writes. The
logical/physical scalar and lossy-placeholder rejection matrix is exercised in focused unit tests
because a real DDL changes both provider metadata and physical schema and cannot independently
manufacture that mismatch.

Arrow cases use real FE/BE Flight ports on both Doris versions. They cover the default no-Arrow
path, successful Arrow reads, an unavailable port, probe success followed by lazy ADBC failure,
same-application fail-sticky behavior, a new-application retry, empty results, and failure after a
row has been delivered. The TCP recorder stores counts only. Because the official ADBC client may
open asynchronous connections after a failure, decision-bound assertions use the connector's
hashed attempt counter while real proxy connections prove that transport was actually exercised.

Write cases cover append, Gravitino `MODIFY_TABLE` denial before observed FE HTTP/JDBC I/O, Doris
`LOAD_PRIV` denial, forced 2PC/strict/filter/schemaless/redirect options, DATETIME precision and
time-zone round trips, explicit truncate success, and the documented empty-table result when a
post-truncate load fails. The truncate account intentionally has `SELECT_PRIV, LOAD_PRIV` without
`ALTER_PRIV` or `DROP_PRIV`: Connector 26.0.0 issues SQL `TRUNCATE TABLE`, and the certified Doris
3.0.6.2 and 4.0.6 servers enforce that statement with `LOAD_PRIV`. Streaming, predicate/dynamic
overwrite, and catalog DDL remain rejection tests.

The harness creates a private CA, valid FE certificate, expired certificate, unrelated self-signed
certificate, and client JVM truststore under `integration-tests/build/tmp`. Nothing containing a
private key is committed. Host fixture directories are `0755` so non-root container entrypoints can
traverse them; files are mounted read-only. The valid certificate SAN covers the dedicated
`10.20.30.0/28` test addresses and Docker hostname. Gravitino and the Spark test JVM receive the
same system truststore; the URL never contains truststore configuration.

Both Doris versions prove strict metadata/schema/read success, Spark JDBC V2 detail/aggregate/
Top-N/limit/offset/partition behavior, zero FE HTTP for the strict path, and JDBC TCP positive
control. Negative cases prove a same-CA hostname mismatch, unknown CA, expired certificate, and a
TLS-disabled server cannot fall back to plaintext. The destructive certificate/TLS swaps run last
inside each suite.

The cache contract counts FE schema requests for `hybrid` and proves that the corresponding
`strict-jdbc-tls` operations make none. Ordinary compatible hits reuse one immutable
Catalyst/type-name pair; explicit invalidation reloads only the target key. The schema-drift test
alters a real Doris table, then proves that Spark's pre-invalidation `REFRESH TABLE` analysis makes
exactly one conditional replacement over each transport before the command invalidates its key.
The next table load fetches a complete snapshot again. Fresh incompatibility and physical-load
errors remain fail-closed and are not retried.

The TCP helper records only accepted/active connection counts and a reset generation. It never
parses or stores MySQL handshakes, SQL, credentials, or other payload bytes. Positive controls first
prove that Gravitino metadata JDBC, Spark SQL JDBC, and native FE HTTP requests traverse the
recorders. Direct `TableCatalog.loadTable` and Spark SQL denial then use different `newSession()`
SessionState/CatalogManager instances and different catalog names. The tests inspect Spark's
resolved-catalog cache without calling `isCatalogRegistered`, because that Spark method resolves the
catalog as a side effect. Each denial starts only after the denial TCP listener has no active
connections and a new generation with zero accepted connections.

During a short post-failure observation window, both denial paths require FE HTTP requests, TCP
accepted connections, and TCP active connections to remain zero. Unit tests separately prove that
physical-catalog `loadTable`, schema loaders, scan builders, and reader factories are not entered.
The catalog object itself is initialized when Spark resolves a catalog name. No test packet-captures
BE/native ports, so the evidence must not be described as direct observation of all Doris network
traffic.

The JDBC security unit matrix covers the exact driver, ordinary URL grammar, malformed and encoded
forms, embedded credentials, the per-channel allow-lists, dangerous Connector/J names, raw and
encoded `gravitino.bypass.*` keys, DBCP class-loading/identity/eager-init/SQL-execution/exposure
properties, and one-level `connectionProperties` parsing with semicolons, newlines, continuation,
Unicode, and malformed escapes. Unknown URL, bypass, and connection-property names fail with fixed
messages that do not echo an injected canary. Deeply nested `connectionProperties` must fail with
a bounded validation error, never `StackOverflowError`. Malicious catalog creation, including
unknown names and `connectionInitSqls`, is also exercised through the real Gravitino Admin API and
must leave both recorders unchanged.

A directly registered Spark JDBC V2 catalog is the partition-count, lossless-column, aggregate-plan,
and timing baseline. It uses the same `JDBCTableCatalog/JDBCTable` implementation as Gravitino's
generic JDBC catalog and receives the same partition tuple and fetch size. Doris DATETIME values use
direct Doris `ResultSet.getString` output as the authoritative value because the standard JDBC
Catalyst timestamp path can apply a JVM time-zone conversion. Boolean lexical forms and
aggregate-decimal trailing scale zeros are compared by value; all other fields, schemas, row
counts, partitions, and plans remain strict.

## Type contract evidence

`GovernedDorisConnectorIT.recordsTypeContractForCurrentDorisVersion` runs on both certified Doris
versions. It executes every probe: supported forms complete DDL, insert, provider loading, Spark
schema, value, partition, and representative-lane assertions; probe-only forms assert the observed
DDL rejection category. No version branch skips a probe.

| Doris DDL form | 3.0.6.2 | 4.0.6 | FE/DESC type | Gravitino 1.3 logical type | Spark representation | Projection and representative lane | Contract |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `BOOLEAN`, `TINYINT`, `SMALLINT`, `INT`, `BIGINT`, `FLOAT`, `DOUBLE`, `DECIMAL(p,s)`, `DATE`, `CHAR(n)`, `VARCHAR(n)`, `STRING`/`TEXT` | DDL, insert, and read | DDL, insert, and read | matching scalar family; `TEXT` stays `text` | matching typed scalar | typed Catalyst scalar | direct, native detail scan | `typed` |
| `DATETIME(6)` | DDL, insert, and read | DDL, insert, and read | `datetime(6)` | timestamp without time zone | String | direct text, SQL scan | `String` |
| `ARRAY<INT>`, `MAP<STRING,INT>`, `STRUCT<...>` | DDL, insert, and read | DDL, insert, and read | matching container family | JDBC-lossy signed-integer placeholder | String | direct text, SQL scan | `String` |
| `LARGEINT` | DDL, insert, and read | DDL, insert, and read | `largeint` | JDBC-lossy integer form | String | direct text, SQL scan | `String` |
| `JSON`; `JSONB` probe | DDL, insert, and read | DDL, insert, and read | `json`; `JSONB` is normalized to `json` | `external(json)` | String | direct text, SQL scan | `String` |
| `VARIANT`, `IPV4`, `IPV6` | DDL, insert, and read | DDL, insert, and read | matching Doris-specific family | external/generic metadata reconciled with the known FE family | String | direct text, SQL scan | `String` |
| `BITMAP`, `HLL` | DDL, insert, and read | DDL, insert, and read | matching sketch family | `external(bit)` / `external(unknown|other)` placeholder | String | `BITMAP_TO_BASE64`/`HLL_TO_BASE64`, SQL scan | `base64` |
| `DECIMAL(76,6)` | DDL rejected (`42000`/`1235`) | DDL, insert, and read after `enable_decimal256=true` | `decimal(76,6)` on 4.0.6 | `external(decimal(76,6))` | String | direct text, SQL scan on 4.0.6 | `probe-only` on 3.0.6.2; `String` on 4.0.6 |
| `DECIMALV2(18,3)`, `DECIMAL32(9,2)`, `DECIMAL64(18,3)`, `DECIMAL128(38,6)` | DDL rejected (`HY000`/`1105`) | DDL rejected (`HY000`/`1105`) | none | none | none | none | `probe-only` |
| `BINARY`, `VARBINARY`, `TIME`, `TINYINT UNSIGNED`, `SMALLINT UNSIGNED`, `INT UNSIGNED`, `BIGINT UNSIGNED` | DDL rejected (`HY000`/`1105`) | DDL rejected (`HY000`/`1105`) | none | none | none | none | `probe-only` |
| Any unlisted or future FE type | no support inference | no support inference | unlisted | generic metadata is insufficient | none | fail closed | `unsupported` |

The lane column describes the complete representative detail scan that contains the type, not an
intrinsic per-column execution property. Projecting only lossless columns from a mixed table keeps
the native lane. If any projected column needs String/base64 normalization, the complete scan uses
the SQL lane and Spark retains authoritative predicates, grouping, aggregate inputs, and ordering
on normalized columns.

The matrix intentionally distinguishes Doris DDL acceptance from converter parsing. Unit tests
keep regression coverage for provider parsing of metadata strings such as legacy decimal names,
but a parser branch does not promote a DDL form into the certified read contract.

This type matrix is certified through the `hybrid` FE-aware schema path. The strict integration
lane independently proves its JDBC-only schema and value path on the lossless scalar, aggregate,
limit/offset, and partitioned tables. It does not reuse FE metadata or inherit certification for
the matrix's JDBC-lossy Doris-specific families.

## macOS Docker routing

Doris scan plans return BE container addresses. On macOS, start `mac-docker-connector` so the host
can route `10.20.30.0/28` before running integration tests.

Docker Engine 28 blocks direct routing to unpublished bridge ports by default. The test harness
sets `com.docker.network.bridge.gateway_mode_ipv4=nat-unprotected` only on its dedicated fixed-subnet
network. If an existing network already owns that exact subnet, the harness reuses it and does not
remove it. It removes only a network that it created after all test containers stop.

The dedicated test BE caps Doris `max_sys_mem_available_low_water_mark_bytes` at 256 MiB on both
supported versions. This retains a low-memory safety floor while avoiding false query cancellation
from Doris's default 5%-of-total reservation in a macOS Docker VM shared with development services.
It does not change the Docker daemon or any non-test container.

Do not repeatedly restart Docker Desktop, change the global firewall, or run `docker network prune`,
`docker system prune`, or any bulk container/volume cleanup.

### Host cannot reach BE container addresses

The symptom is that `mac-docker-connector` reports running but the host still cannot reach the
`10.20.30.x` BE addresses returned in scan plans, failing with `Broken pipe` or a connection
timeout. With Docker Engine 28 or newer, the bridge's default `nat` gateway mode drops direct
routing to unpublished container ports. Verify the dedicated test network's gateway option:

```bash
docker network inspect <test-network> --format '{{json .Options}}'
```

The harness sets `com.docker.network.bridge.gateway_mode_ipv4=nat-unprotected` on the network it
creates. A manually created network missing that option must be recreated with it:

```bash
docker network create --subnet=10.20.30.0/28 --gateway=10.20.30.1 \
  --opt com.docker.network.bridge.gateway_mode_ipv4=nat-unprotected <name>
```

Restarting Docker Desktop or the connector only rebuilds the same network state and does not
remove the Engine 28 restriction.

## Linux CI requirements

The integration lane runs on Ubuntu 24.04 with Docker Engine 28. Its native cgroup v2
controller layout makes the image's JDK 17 container-memory detection crash while the FE
initializes its BDB metadata environment (`NoClassDefFoundError: JVMSystemUtils`,
apache/doris#60536). The compose file therefore disables JVM container support on the test FE
with `JAVA_TOOL_OPTIONS=-XX:-UseContainerSupport`. The runner applies no memory limit, so the
JVM falls back to host detection.

macOS Docker Desktop does not expose this controller layout, so passing integration tests on
macOS does not prove the Linux CI lane. Record OS, Docker server version, kernel, and cgroup
version before treating local results as equivalent to CI.

## Test logs

Gradle XML and HTML reports are under `integration-tests/build/test-results/` and
`integration-tests/build/reports/tests/`. Container output is attached to the Gradle test log.
CI uploads reports when a gate fails. Failure-time Docker inventory and log collection select only
containers labeled `io.github.jiangxt2.gravitino-doris-spark-connector.it=true`; the integration
tests verify that Doris FE, Doris BE, Gravitino, and the TCP proxy carry that label.

## Release evidence boundary

These tests verify dependency resolution, locked production graphs, archive contents, and the
external Driver contract. They do not generate or claim a final-binary SBOM. Release operations
must scan the completed tar and zip with a fixed, checksum-verified binary scanner and publish the
CycloneDX result as a sidecar bound to the archive digest/signature/attestation.
