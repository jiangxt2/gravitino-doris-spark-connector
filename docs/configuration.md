# Configuration and security

## Server catalog properties

| Property | Required | Purpose |
| --- | --- | --- |
| `jdbc-url` | yes | Doris FE MySQL protocol URL used by Gravitino metadata and the SQL lane |
| `jdbc-driver` | yes | Must be exactly `com.mysql.cj.jdbc.Driver` |
| `jdbc-user` | yes | Hidden technical-user property used by credential vending |
| `jdbc-password` | yes | Hidden technical-user password; an empty Doris password is valid |
| `doris-fenodes` | yes | Comma-separated `host:httpPort` FE endpoints |
| `doris-query-port` | yes | FE MySQL query port required by Connector 26.0.0 native planning |
| `credential-providers` | no | Explicitly set `jdbc-user-password`; JDBC catalogs also add it automatically |

The FE endpoint parser accepts optional whitespace around commas, validates every `host:port`, and
stores a normalized comma-separated value.

The single `jdbc-url` is used by both the Gravitino server metadata plane and the Spark SQL lane.
Its host and port must therefore be routable from the Gravitino server, Spark driver, and Spark
executors. Use a service address shared by those network domains rather than a process-local
`localhost` address.

## JDBC Driver provisioning

The project does not redistribute MySQL Connector/J. It validates the exact driver class name
`com.mysql.cj.jdbc.Driver` and tests the Maven Central artifact
`com.mysql:mysql-connector-j:8.0.33`.

For a non-containerized Gravitino Server, place the Driver JAR in
`$GRAVITINO_HOME/catalogs/doris-governed/libs` before startup. For the official Gravitino 1.3.0
container, mount a controlled directory containing the chosen Driver and any separately audited
JDBC drivers required by other catalogs at `/opt/gravitino/jdbc-drivers`. The 1.3.0 image entrypoint
links files matching `mysql-connector-java-*.jar` into the server classpath, so the mounted filename
must retain that legacy pattern even when the artifact uses the current `mysql-connector-j`
coordinate.

Pass the Driver together with the project Spark JARs through `--jars`, `spark.jars`, or the
cluster-manager equivalent. Spark must make it visible to the driver and every executor. The
adapter checks driver-side availability before it creates the physical catalog; executor-side
availability remains a deployment responsibility.

Missing Driver failures are fixed and redacted. The server message names the expected catalog
library location; the Spark message names the driver/executor classpath requirement. Neither
message contains the JDBC URL or credentials. These checks run after JDBC configuration validation,
so an unsafe catalog is rejected before class loading is attempted.

## JDBC connection allow-list

The same shared validator runs before Gravitino initializes its JDBC provider and again before
Spark creates a physical Doris catalog or resolves credentials. It accepts only:

- the exact `com.mysql.cj.jdbc.Driver` class;
- ordinary `jdbc:mysql://` URLs with one DNS/IPv4 host or bracketed hexadecimal IPv6 host without
  a zone identifier or dotted-quad tail;
- one explicit port in the range `1..65535`;
- an empty database or one database path;
- structurally complete, non-duplicate `key=value` query parameters.

It rejects load-balance, replication, DNS SRV, multi-host, host sublist and host-property forms,
URL user-info, query credentials, fragments, malformed percent encoding, and values that do not
converge after bounded recursive decoding. JDBC credentials must come only from Gravitino's hidden
`jdbc-user`/`jdbc-password` properties and credential vending.

The following Connector/J parameter names are rejected case-insensitively wherever the driver
could receive them: `maxAllowedPacket`, `autoDeserialize`, `queryInterceptors`,
`statementInterceptors`, `detectCustomCollations`, `allowLoadLocalInfile`,
`allowUrlInLocalInfile`, and `allowLoadLocalInfileInPath`.

Raw and `gravitino.bypass.*` configuration also reject DBCP class-loading properties
`connectionFactoryClassName`, `evictionPolicyClassName`, and `driverClassName`; identity overrides
`url`, `username`, and `password`; eager `initialSize`; SQL-bearing `connectionInitSqls` and
`validationQuery`; and exposure properties `accessToUnderlyingConnectionAllowed`, `jmxName`, and
`registerConnectionMBean`. Canonical `jdbc-url`, `jdbc-driver`, `jdbc-user`, and `jdbc-password`
must not be supplied through `gravitino.bypass.*`.

A `connectionProperties` value is parsed once with the same semicolon/newline and Java
`Properties.load` rules used by DBCP before its effective names are validated. A nested
`connectionProperties` name and unparseable input fail closed. Errors name only a fixed rule or
known denied parameter and do not echo the URL, host, credential, or property value.

## Transport limitation

The current release does not enforce or certify TLS for either the Gravitino metadata connection or
Spark's SQL/native execution lanes. The URL parser permits structurally valid safe query properties
so a future verified transport profile can be added, but current TLS-looking options are not a
supported strict-TLS contract. Do not claim confidentiality, server-identity verification, or
downgrade protection from this release.

## SQL-lane performance properties

The four partition properties are atomic: provide all four or none.

| Gravitino property | Spark JDBC option | Validation |
| --- | --- | --- |
| `doris-jdbc-partition-column` | `partitionColumn` | non-empty |
| `doris-jdbc-lower-bound` | `lowerBound` | non-empty |
| `doris-jdbc-upper-bound` | `upperBound` | non-empty |
| `doris-jdbc-num-partitions` | `numPartitions` | positive integer |
| `doris-jdbc-fetch-size` | `fetchsize` | positive integer |

Partition bounds determine stride; they do not filter rows. Choose the same values used for a
comparable direct Spark JDBC read. Avoid catalog-wide partition settings that prevent Spark from
proving complete aggregate pushdown; use a second governed catalog profile when detail scans and
aggregate workloads need different partition strategies.

The adapter captures the partition tuple and fetch size when the Spark catalog is initialized.
After changing any of these Gravitino catalog properties, recreate the Spark session so the catalog
loads the new values.

## Schema cache properties

| Property | Default | Validation |
| --- | --- | --- |
| `doris-schema-cache-ttl-ms` | `30000` | non-negative; zero disables the catalog cache |
| `doris-schema-cache-max-entries` | `1000` | positive integer |

Each returned table wrapper owns its validated snapshot. The catalog cache reduces repeated FE
requests across wrapper instances. Authorization and logical-schema comparison still run on every
load, including a cache hit. A cache entry contains the Catalyst fields and their corresponding FE
type names as one immutable snapshot. A compatible hit performs no FE request. If current logical
metadata is incompatible with a cached snapshot, the catalog conditionally replaces that exact
entry with one coalesced fresh FE load and validates once more. This is a single stale-hit
revalidation, not a general retry: an initial or refreshed mismatch fails closed, and load errors
are not retried. `REFRESH TABLE` and catalog `invalidateTable` evict only the target table; TTL
expiry reloads the complete snapshot. A zero TTL performs a fresh load every time and does not
retain stale physical schema state.

## Allowed native read tuning

The adapter allows only known read-side `doris.*` options, including connection/request timeouts,
retries, tablet size, batch size, memory limit, IN threshold, and thrift message size. Unknown
options fail closed rather than silently reaching a third-party connector.

The following keys are protected regardless of whether they came from Spark catalog options or a
Gravitino `spark.bypass.*` property:

- `doris.fenodes`, `doris.query.port`, `doris.user`, `doris.password`;
- JDBC URL, driver, user, and password;
- generated `dbtable`/query values.

## Credential lifecycle

Credentials are static Gravitino JDBC credentials. They may be serialized to trusted executors by
the Doris or JDBC reader, but they must not appear in ordinary metadata, logs, errors, plans, or
object rendering. Rotate a technical password in Gravitino and recreate the Spark session so the
catalog fetches a new credential.

Do not grant the technical Doris user broader database permissions than the governed catalogs need.
Gravitino authorization is the control-plane gate; Doris privileges remain a defense-in-depth data
plane boundary.
