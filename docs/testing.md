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
./gradlew spotlessCheck test installDist verifySparkDependencyVersions
```

These tasks cover property protection, credential resolution, explicit authorization ordering,
schema/type compatibility, cache behavior, hybrid planner state, capability filtering, plugin
registration, provider loading, distribution assembly, repository container-label configuration,
resolved Spark-module version consistency, and the shared JDBC URL/driver/DBCP security matrix.

## Spark compatibility gates

Spark 3.5.8 is the default compile and test version. The lower and current upper compatibility
boundaries run in separate Gradle processes:

```bash
./gradlew test installDist :integration-tests:compileIntegrationTestJava \
  verifySparkDependencyVersions -PsparkVersion=3.5.0
./gradlew test installDist :integration-tests:compileIntegrationTestJava \
  verifySparkDependencyVersions -PsparkVersion=3.5.9
```

These commands compile the integration-test source set but do not start Docker. They verify API
linkage, class loading, the packaged connector distribution, and the complete resolved
`org.apache.spark` module set. They do not certify real Doris behavior on those two boundary
versions. The full real-infrastructure certification remains pinned to Spark 3.5.8.

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

The tests verify direct-result parity, planner pushdown, four-partition JDBC parity, type
normalization, authorization-before-I/O, cache/refresh, credential redaction, JDBC configuration
rejection, and read-only boundaries.

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
forms, embedded credentials, dangerous Connector/J names, raw and encoded
`gravitino.bypass.*` keys, DBCP class-loading/identity/eager-init/SQL-execution/exposure properties,
and one-level `connectionProperties` parsing with semicolons, newlines, continuation, Unicode, and
malformed escapes. Deeply nested `connectionProperties` must fail with a bounded validation error,
never `StackOverflowError`. Malicious catalog creation, including `connectionInitSqls`, is also
exercised through the real Gravitino Admin API and must leave both recorders unchanged.

A directly registered Spark JDBC V2 catalog is the partition-count, lossless-column, aggregate-plan,
and timing baseline. It uses the same `JDBCTableCatalog/JDBCTable` implementation as Gravitino's
generic JDBC catalog and receives the same partition tuple and fetch size. Doris DATETIME values use
direct Doris `ResultSet.getString` output as the authoritative value because the standard JDBC
Catalyst timestamp path can apply a JVM time-zone conversion. Boolean lexical forms and
aggregate-decimal trailing scale zeros are compared by value; all other fields, schemas, row
counts, partitions, and plans remain strict.

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
