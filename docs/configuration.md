# Configuration and security

## Server catalog properties

| Property | Required | Purpose |
| --- | --- | --- |
| `jdbc-url` | yes | Doris FE MySQL protocol URL used by Gravitino metadata and the SQL lane |
| `jdbc-driver` | yes | MySQL JDBC driver class, normally `com.mysql.cj.jdbc.Driver` |
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
load.

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
