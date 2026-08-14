# Configuration and security

## Server catalog properties

| Property | Required | Purpose |
| --- | --- | --- |
| `jdbc-url` | yes | Doris FE MySQL protocol URL used by Gravitino metadata and the SQL lane |
| `jdbc-driver` | yes | Must be exactly `com.mysql.cj.jdbc.Driver` |
| `jdbc-user` | yes | Hidden technical-user property used by credential vending |
| `jdbc-password` | yes | Hidden technical-user password; an empty Doris password is valid |
| `doris-read-transport` | no | `hybrid` (default) or `strict-jdbc-tls` |
| `doris-fenodes` | hybrid only | Comma-separated `host:httpPort` FE endpoints; rejected by strict profile |
| `doris-query-port` | hybrid only | FE MySQL query port; rejected by strict profile |
| `doris-arrow-flight-sql-mode` | no | `disabled` (default) or experimental `preferred`; rejected by strict profile |
| `doris-arrow-flight-sql-port` | preferred only | Catalog-managed FE Arrow Flight SQL port in `1..65535` |
| `doris-write-mode` | no | `disabled` (default) or `batch`; batch requires Spark 3.5.3+ and hybrid transport |
| `doris-write-overwrite-mode` | no | `reject` (default) or explicit non-atomic `truncate`; truncate requires batch write |
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
- structurally complete, non-duplicate `key=value` query parameters from the channel table below.

It rejects load-balance, replication, DNS SRV, multi-host, host sublist and host-property forms,
URL user-info, query credentials, fragments, malformed percent encoding, and values that do not
converge after bounded recursive decoding. JDBC credentials must come only from Gravitino's hidden
`jdbc-user`/`jdbc-password` properties and credential vending.

Driver and pool parameters are allow-listed by their final input channel:

| Channel | Allowed names |
| --- | --- |
| JDBC URL query, `hybrid` | `connectTimeout`, `socketTimeout` |
| JDBC URL query, `strict-jdbc-tls` | `connectTimeout`, `socketTimeout`, required `sslMode`, optional `fallbackToSystemTrustStore` |
| `connectionProperties` | `connectTimeout`, `socketTimeout` |
| `gravitino.bypass.*` | `maxIdle`, `connectTimeout`, `socketTimeout`, `connectionProperties` |
| Spark option / `spark.bypass.*` | only the native read options listed below; all rejected by strict transport |

Names are matched case-insensitively after bounded decoding. Any name not listed for its channel
fails closed, including a future Connector/J or DBCP property. Ordinary Gravitino catalog metadata
is not a driver parameter and remains outside these allow-lists.

The following audited Connector/J families are also explicitly recognized and rejected
case-insensitively, including percent-encoded and camel-case alias variants, before they can reach
the driver. This fixed Connector/J 8.0.33 audit documents the security reasons behind the narrower
allow-lists:

- file/stream and deserialization sinks: `maxAllowedPacket`, `autoDeserialize`,
  `detectCustomCollations`, `allowLoadLocalInfile`, `allowUrlInLocalInfile`,
  `allowLoadLocalInfileInPath`;
- class loading, interceptors, and configuration expansion: `queryInterceptors`,
  `statementInterceptors`, `propertiesTransform`, `socketFactory`, `protocol`,
  `connectionLifecycleInterceptors`, `exceptionInterceptors`, `profilerEventHandler`,
  `clientInfoProvider`, `serverConfigCacheFactory`, `queryInfoCacheFactory`,
  `parseInfoCacheFactory`, `logger`, `useConfigs`, `defaultAuthenticationPlugin`,
  `disabledAuthenticationPlugins`, `authenticationPlugins`, `authenticationFidoCallbackHandler`,
  `loadBalanceExceptionChecker`, `ociConfigFile`, `ociConfigProfile`, `ldapServerHostname`,
  `serverRSAPublicKeyFile`, `allowPublicKeyRetrieval`;
- network path and endpoint routing: `dnsSrv`, `socksProxyHost`, `socksProxyPort`,
  `socksProxyRemoteDns`, `localSocketAddress`,
  `ha.loadBalanceStrategy`/`haLoadBalanceStrategy`,
  `ha.enableJMX`/`haEnableJMX`;
- connection-time side effects outside the read-only boundary: `createDatabaseIfNotExist`,
  `sessionVariables`;
- diagnostic disclosure outside the redaction boundary: `autoGenerateTestcaseScript`,
  `dumpQueriesOnException`, `enablePacketDebug`, `explainSlowQueries`,
  `includeInnodbStatusInDeadlockExceptions`, `includeThreadDumpInDeadlockExceptions`,
  `includeThreadNamesAsStatementComment`, `gatherPerfMetrics`, `logXaCommands`, `logSlowQueries`,
  `profileSQL`, `traceProtocol`, `useUsageAdvisor`;
- client keystore and persistent secrets: `clientCertificateKeyStoreUrl`,
  `clientCertificateKeyStoreType`, `clientCertificateKeyStorePassword`,
  `fallbackToSystemKeyStore`.

JDBC credentials (`user`, `username`, `password`, and the MFA passwords `password1`-`password3`)
must never appear in any connection configuration; they come only from Gravitino's hidden
`jdbc-user`/`jdbc-password` properties and credential vending.

Raw and `gravitino.bypass.*` configuration also reject DBCP class-loading properties
`connectionFactoryClassName`, `evictionPolicyClassName`, and `driverClassName`; identity overrides
`url`, `username`, and `password`; eager `initialSize`; SQL-bearing `connectionInitSqls` and
`validationQuery`; and exposure properties `accessToUnderlyingConnectionAllowed`, `jmxName`, and
`registerConnectionMBean`. Canonical `jdbc-url`, `jdbc-driver`, `jdbc-user`, and `jdbc-password`
must not be supplied through `gravitino.bypass.*`.

A `connectionProperties` value is parsed once with the same semicolon/newline and Java
`Properties.load` rules used by DBCP before its effective names are validated. A nested
`connectionProperties` name and unparseable input fail closed. Errors name only a fixed rule
category and do not echo the URL, host, credential, user-controlled property name, or property
value.

## Read transport profiles

`hybrid` keeps the native tablet lane plus Spark JDBC V2 SQL lane and requires both native endpoint
properties. It is the compatibility default and is not a strict TLS claim: the two endpoint sets
are not identity-bound, and Connector 26.0.0's native HTTPS behavior is outside this verified JDBC
contract.

`strict-jdbc-tls` is JDBC-only. It requires exactly one canonical `sslMode=VERIFY_IDENTITY`, rejects
native endpoints/options, and never initializes the official Doris native catalog or FE HTTP schema
client. Gravitino metadata, Spark physical schema, and Spark reads all use `jdbc-url`.
`fallbackToSystemTrustStore` may be omitted (Connector/J 8.0.33 defaults it to `true`) or supplied
once with the exact value `true`; `false` is rejected.

Arrow `preferred` and batch write are independently opt-in, but both require `hybrid`. They are
rejected rather than silently ignored by `strict-jdbc-tls`.

Strict URL TLS controls are fail closed:

| Parameter family | Contract |
| --- | --- |
| `sslMode` | exactly once, exact value `VERIFY_IDENTITY` |
| `fallbackToSystemTrustStore` | absent or exact value `true` |
| `trustCertificateKeyStoreUrl/Password/Type` | rejected; URL/catalog persistence is not a secret boundary |
| `useSSL`, `requireSSL`, `verifyServerCertificate` | rejected legacy controls |
| `sslTrustStore*` and any other unreviewed TLS/keystore/certificate control | rejected |

Install the private/public CA chain in a read-only JVM truststore on the Gravitino Server, Spark
driver, and every executor. Configure it as JVM startup state, for example with
`-Djavax.net.ssl.trustStore`, `-Djavax.net.ssl.trustStoreType`, and a deployment-secret source for
the password. Never place the truststore password or path in `jdbc-url`, a Gravitino property,
Spark option, log, or plan. The certificate SAN must match the single host in `jdbc-url`; an IP URL
therefore requires an IP SAN.

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

## Arrow Flight SQL

Arrow Flight SQL is disabled by default. To attempt it for native detail scans, set both catalog
properties:

```properties
doris-arrow-flight-sql-mode=preferred
doris-arrow-flight-sql-port=8070
```

This port is declared and validated by the Gravitino server provider and then treated as immutable
catalog state. It cannot come from a Spark option, `spark.bypass.*`, raw `doris.*` property, or
official FE auto-discovery. The Spark adapter forces `doris.fe.auto.fetch=false` and maps only the
managed port to the official connector.

The preferred path applies only after planning selects the native detail lane. It first performs a
TCP connect probe with at most one second per FE and a three-second budget across the complete
catalog-managed FE list. Probe success is not a connection guarantee: lazy ADBC connection or query
initialization can still fail. A classified unavailable-I/O/timeout/not-implemented failure
before the first delivered row closes Arrow and starts the same partition's Thrift reader.
Authentication, authorization, query, schema, conversion, cancellation, interruption, and data
errors fail the task. No fallback is allowed after one Arrow row has been delivered.

One eligible failure opens an executor-JVM circuit identified by a hash of Spark application ID
and endpoint identity. Later partitions in that application skip Arrow. The circuit has no
cooldown or automatic reset in this release, deliberately bounding repeated failure/resource
churn. A long-lived notebook or Spark Thrift Server therefore continues on Thrift until a new
Spark application is started. The current official Arrow implementation uses insecure gRPC and
row-wise `InternalRow` conversion; enabling it is not a strict TLS or native-columnar claim.

## Governed batch write

The default remains read-only. Enable append on Spark 3.5.3 or newer with:

```properties
doris-write-mode=batch
doris-write-overwrite-mode=reject
```

Spark 3.5.0 through 3.5.2 ignore no setting: they retain read capabilities and fail closed for all
writes. The Spark principal must have Gravitino `MODIFY_TABLE` before a physical write delegate is
created. The Doris technical user must have `SELECT_PRIV` for the governed read/schema path and
`LOAD_PRIV` (documented by Doris as the load/INSERT data-plane privilege) for Stream Load. For
truncate overwrite, official Connector 26.0.0 first issues Doris SQL `TRUNCATE TABLE` through
`DorisFrontendClient` and then creates the Stream Load writer. Doris 3.0.6.2 and 4.0.6 both check
that SQL operation with `LOAD_PRIV`; real tests prove a user with `SELECT_PRIV, LOAD_PRIV` and
without `ALTER_PRIV`/`DROP_PRIV` can use it.

Every batch write forces the following effective official connector options after all user input:

| Official option | Forced value |
| --- | --- |
| `doris.sink.mode` | `stream_load` |
| `doris.sink.enable-2pc` | `true` |
| `doris.sink.properties.strict_mode` | `true` |
| `doris.max.filter.ratio` | `0` |
| `doris.write.schemaless` | `false` |
| `doris.sink.auto-redirect` | `false` |

Raw, bypass, or per-write options cannot change this contract. The writer accepts exact column
count/order/names, safe nullability, and certified lossless types. A Doris DATETIME column is the
only normalized write family: its final String must exactly match `yyyy-MM-dd HH:mm:ss` for
precision zero or include exactly the declared 1..6 fractional digits. Spark Timestamp input is
first converted by Spark's assignment rule using the analysis-time `spark.sql.session.timeZone`;
the connector validates the resulting String per row.

`append()` is supported. Streaming writes, predicate overwrite, dynamic overwrite, CTAS, DDL,
UPDATE, DELETE, and MERGE are rejected. Setting `doris-write-overwrite-mode=truncate` adds only
full-table truncate overwrite. It is a non-atomic SQL truncate-then-load compatibility operation: if
writer creation, precommit, or commit later fails, the old rows are already gone and the target can
be empty or partially loaded. 2PC is per official writer transaction and is not a job-wide atomic
commit guarantee.

## Schema cache properties

| Property | Default | Validation |
| --- | --- | --- |
| `doris-schema-cache-ttl-ms` | `30000` | non-negative; zero disables the catalog cache |
| `doris-schema-cache-max-entries` | `1000` | positive integer |

Each returned table wrapper owns its validated snapshot. The catalog cache reduces repeated
physical-metadata requests across wrapper instances: FE HTTP for `hybrid`, JDBC metadata for
`strict-jdbc-tls`. Authorization and logical-schema comparison still run on every load, including a
cache hit. A cache entry contains the Catalyst fields and their corresponding physical type names
as one immutable snapshot. A compatible hit performs no physical-metadata request. If current
logical metadata is incompatible with a cached snapshot, the catalog conditionally replaces that
exact entry with one coalesced fresh transport-specific load and validates once more. This is a
single stale-hit revalidation, not a general retry: an initial or refreshed mismatch fails closed,
and load errors are not retried. `REFRESH TABLE` and catalog `invalidateTable` evict only the target
table; TTL expiry reloads the complete snapshot. A zero TTL performs a fresh load every time and
does not retain stale physical schema state.

## Allowed native read tuning

The hybrid adapter allows only known read-side `doris.*` options, including connection/request
timeouts, retries, tablet size, batch size, memory limit, IN threshold, and thrift message size.
Unknown options fail closed rather than silently reaching a third-party connector.

These options configure only the native Doris reader. The strict profile rejects them instead of
silently ignoring them; strict JDBC partition and fetch-size tuning uses the properties above.

The following keys are protected regardless of whether they came from Spark catalog options or a
Gravitino `spark.bypass.*` property:

- `doris.fenodes`, `doris.query.port`, `doris.user`, `doris.password`;
- `doris.read.mode`, `doris.read.arrow-flight-sql.port`, `doris.fe.auto.fetch`;
- all `doris.sink.*`, `doris.max.filter.ratio`, and `doris.write.schemaless` settings;
- JDBC URL, driver, user, and password;
- generated `dbtable`/query values;
- `doris-read-transport`.

## Credential lifecycle

Credentials are static Gravitino JDBC credentials. They may be serialized to trusted executors by
the Doris or JDBC reader, but they must not appear in ordinary metadata, logs, errors, plans, or
object rendering. Rotate a technical password in Gravitino and recreate the Spark session so the
catalog fetches a new credential.

Do not grant the technical Doris user broader database permissions than the governed catalogs need.
Gravitino authorization is the control-plane gate; Doris privileges remain a defense-in-depth data
plane boundary.
