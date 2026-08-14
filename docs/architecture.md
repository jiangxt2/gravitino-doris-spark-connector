# Architecture

## Design objective

The connector preserves the governance ordering used by Gravitino's existing Spark connectors:
resolve governed metadata and authorization first, then create a storage-specific delegate. It
uses published Gravitino 1.3.0 APIs instead of copying later framework classes into this repository.

## Mapping to Gravitino connector patterns

| Existing Gravitino pattern | Standalone implementation |
| --- | --- |
| `BaseCatalog` template method | `GovernedDorisCatalog` authorizes the logical table before loading its Doris delegate |
| `GravitinoJdbcCatalog` credential vending | `DorisCredentialResolver` requires exactly one vended `JdbcCredential` |
| facade plus storage table delegate | `GovernedDorisTable` exposes the governed schema and policy-filtered batch capabilities |
| `PropertiesConverter` | `DorisPropertiesConverter` validates both Spark options and `spark.bypass.*` through one allow-list |
| version-specific catalog adapter | `DorisCatalogClassResolver` resolves only Spark 3.5 and Scala 2.12 |
| official driver plugin registration | `GovernedDorisDriverPlugin` composes the official plugin and adds only `doris-governed` |
| server `CatalogProvider` service loading | `GovernedDorisCatalogProvider` supplies an isolated short name and type converter |

No project class uses the `org.apache.gravitino` namespace.

The provider's extended type mapping is directional: existing binary, complex, unknown, and
wide-decimal metadata can be loaded, while reverse DDL conversion still rejects binary and
arbitrary external catalog strings. Future DDL support must add an explicit Doris type grammar
allow-list instead of interpolating untrusted type text.

## Source baselines reviewed

The implementation is checked against the concrete source contracts, not only product guides:

- Gravitino 1.3.0 `BaseCatalog`, `GravitinoJdbcCatalog`, `PropertiesConverter`,
  `GravitinoDriverPlugin`, and the JDBC Doris provider/type converter;
- Doris Connector 26.0.0 `DorisTableCatalogBase`, `DorisTableBase`, `DorisScanBuilderBase`,
  `V2ExpressionBuilder`, `SchemaConvertors`, `DorisConfig`, and `DorisOptions`;
- Spark 3.5.8 `JDBCTable`, `JDBCScanBuilder`, `JDBCOptions`, `JdbcDialects`, and V2 pushdown rules,
  with compile and class-loading compatibility boundaries at 3.5.0 and 3.5.9;
- Doris 3.0.6.2 and 4.0.6 FE schema, JDBC metadata, DDL, and value behavior in real containers.

This review is why the project composes the published Gravitino plugin, delegates tablet IO to the
official Doris connector, and delegates SQL generation to Spark JDBC rather than copying any of
those frameworks.

## Spark compatibility boundary

`DorisCatalogClassResolver` accepts Spark 3.5.x only when the Scala binary version is 2.12. The
default version catalog pins Spark 3.5.8 for compilation, unit tests, distribution assembly, and
both real-Doris integration lanes. This is the release-certified combination.

The `sparkVersion` Gradle property exists only for compatibility verification. It accepts numeric
Spark 3.5 patch versions, applies one version to every `org.apache.spark` module in every
subproject, and does not change Scala, Gravitino, or Doris Connector dependencies.
`verifySparkDependencyVersions` resolves the Spark-bearing test classpaths, rejects mixed patch
versions, and requires Spark core, SQL, and Catalyst to be present so an empty resolution cannot
pass. CI runs that task with Spark 3.5.0 and 3.5.9 in separate processes and compiles the
integration-test source set without starting Docker. These boundary smokes are not substitutes for
the Spark 3.5.8 real-infrastructure matrix. Spark 3.5.0 through 3.5.2 retain the same read path but
remain write-disabled. The public write-aware catalog overload is available from Spark 3.5.3;
`DorisCatalogClassResolver` gates it by the parsed patch version without reflection or a second
source set.

## Request flow

```text
Spark SQL
  -> GovernedDorisSparkPlugin
     -> official GravitinoSparkPlugin
     -> register provider=doris-governed
  -> GovernedDorisCatalogSpark35.initialize
     -> shared JDBC URL/driver validation
     -> MySQL Driver availability preflight
     -> immutable transport selection and credential vending
  -> GovernedDorisCatalogSpark35.loadTable
     -> Gravitino TableCatalog.loadTable(identifier, SELECT_TABLE)
     -> authorized physical schema snapshot and compatibility validation
     -> GovernedDorisTable
        -> hybrid: official tablet reader or Spark JDBCTable/JDBCScanBuilder
        -> strict-jdbc-tls: Spark JDBCTable/JDBCScanBuilder only

Spark batch append on 3.5.3+
  -> write-aware loadTable(identifier, MODIFY_TABLE)
  -> authorized logical table and physical schema snapshot
  -> exact write-schema validation
  -> policy-filtered official Doris WriteBuilder
  -> validating DataWriterFactory
  -> official Stream Load writer with forced 2PC options
```

An authorization exception exits before native catalog `loadTable`, FE HTTP, scan construction, or
JDBC connection creation. Integration tests observe FE HTTP method/path counts and MySQL/JDBC TCP
accept/active counts without retaining payloads. They do not directly observe BE/native sockets;
the no-BE-work conclusion is derived from the authorization ordering plus unit tests proving that
physical-catalog `loadTable`, schema loaders, scan builders, and readers were not entered. Spark
catalog resolution does initialize the catalog object before a direct table-load call; it does not
invalidate the table-level ordering or observed zero-I/O result.

## JDBC configuration boundary

`jdbc-security` is a dependency-free shared production module used by both the Gravitino provider
and Spark adapter. The provider validates raw catalog properties in `withCatalogConf` before its
parent lifecycle can create DBCP state. Spark validates the canonical URL and driver before
creating the official Doris catalog or vending credentials, and `DorisJdbcConnectionInfo` repeats
the same check when SQL-lane connection material is formed.

The contract is deliberately narrower than Connector/J's complete URL grammar: exact current
driver class, one ordinary host and explicit port, optional single database, and a separate
immutable allow-list for every parameter channel. URL and `connectionProperties` accept only the
reviewed connection timeouts; DBCP bypass accepts those timeouts, `maxIdle`, and one controlled
`connectionProperties` value. Strict transport adds only its two canonical TLS controls to the
URL. Unknown names fail closed, while ordinary Gravitino catalog metadata is not misclassified as
driver input. The DBCP string is parsed once, matching its runtime consumption; nested
`connectionProperties` is rejected instead of recursively reinterpreted. Failures use fixed
messages and never include user-controlled names, values, or raw connection material.

The strict profile adds a second structural catalog factory: it creates Spark's JDBC-only catalog,
loads the physical snapshot over the verified JDBC connection only after authorization, and seeds
the same `JDBCTable` used for reads. It never instantiates the official Doris catalog. The profile
requires Connector/J `VERIFY_IDENTITY` and the JVM system truststore, so Gravitino metadata, Spark
schema, and Spark reads share one CA/hostname-verified endpoint. The compatible hybrid profile
remains outside that certification.

## Distribution and dependency integrity boundary

The project archive contains the independent Spark adapter, Gravitino provider, shared JDBC
security module, official Doris connector, shaded Gravitino Spark runtime, and required
transitives. Spark core/SQL/Catalyst, server-provided Guava/logging libraries, and MySQL Connector/J
remain outside the archive.

`verifyDistributionDependencyContract` compares `installDist`, tar, and zip file sets, checks
required documentation/configuration, rejects target-provided libraries, and scans every JAR for
the MySQL Driver class. The three production distribution configurations use strict Gradle lock
state. SHA-256 dependency verification covers Gradle-resolved plugin, artifact, and metadata bytes,
while GitHub Actions are fixed to full commit SHAs. These controls establish reproducible reviewed
inputs and archive boundaries; they do not prove publisher identity or provide a final-binary SBOM.

Final release operations must scan the completed tar/zip and publish a sidecar SBOM bound to the
same archive digest, signature, and attestation. Generating a project-resolution SBOM during this
build would not prove the contents of the final binary archive.

## Hybrid scan selection

The native lane handles lossless detail scans and keeps the official Doris Connector's tablet
partitioning, projection, and supported predicates.

When `doris-arrow-flight-sql-mode=preferred`, only the selected native detail lane is wrapped. The
wrapper uses the official `DorisInputPartition`, fixes FE auto-discovery off, and obtains the Flight
port only from the server-managed catalog property. A bounded TCP probe precedes the official
lazy ADBC reader. Probe success does not imply that ADBC connection or query initialization will
succeed; a classified transport failure during those steps can still transition to Thrift.

Fallback is legal only before an Arrow row has been returned to Spark. The state machine
distinguishes not-started, row-ready-but-not-delivered, delivered, exhausted, Thrift fallback, and
closed states. Once `get()` successfully delivers the first Arrow row, all later failures fail the
partition instead of replaying it. Normal empty results enter the exhausted state and never start
Thrift. Unsupported/authentication/query/schema/data failures are not transport fallback signals.

The first eligible failure opens an executor-JVM circuit keyed by SHA-256 of the Spark application
ID and endpoint identity. Subsequent partitions in the same application skip probe and Arrow. The
circuit intentionally has no automatic reset or background retry in this release: this bounds
failure and native-resource churn but means a long-lived Spark Thrift Server or notebook keeps
using Thrift after a transient outage. A new Spark application gets a new key. Arrow continues to
produce `InternalRow` through the official row conversion path; it is not advertised as a native
Spark columnar reader.

The SQL lane is selected when:

- the projected schema contains a String/base64-normalized column;
- Spark pushes a global limit;
- Spark pushes aggregate or Top-N;
- Spark pushes offset;
- a native builder is unavailable but the SQL path is valid.

The SQL lane constructs Spark's `JDBCTable` and therefore reuses Spark 3.5's JDBC V2 aggregate,
Top-N, limit, offset, predicate, and partition planning. It does not contain a second SQL compiler.
Connector 26.0.0 applies native limit per scan partition while reporting complete pushdown, so the
adapter selects the SQL lane for exact global-limit semantics. If a future SQL delegate rejects
limit, native limit is only a pruning hint and Spark retains the authoritative global operator.
After `pushLimit(m)`, `pushOffset(n)` is rejected when `n >= m`; Spark retains the offset and
preserves the correct empty-result semantics.

Predicates and ordering that reference normalized columns remain Spark residuals until equivalent
Doris text semantics are proven. A predicate is reported as pushed only when every selectable lane
accepted it. The verified scalar set includes comparisons, `IN`, null checks, boolean composition,
and Spark V2 `STARTS_WITH`, `ENDS_WITH`, and `CONTAINS` when the literal contains no Doris `LIKE`
metacharacters. Connector 26.0.0 does not escape `%` or `_` in those three translations and does
not escape backslash in string literals generally, so affected predicates remain Spark residuals.
Integration tests compare both paths with direct Doris results.

## Schema and type boundary

The adapter takes one physical schema snapshot, compares it directionally with the authorized
Gravitino table, and returns an executable Spark schema. Hybrid reads obtain the snapshot from FE
HTTP; strict reads obtain column order, name, `TYPE_NAME`, precision, scale, and nullability from
JDBC metadata without FE HTTP. Both retain physical order, names, and nullability while merging
governed comments.

In the `hybrid` profile, directly representable scalar types remain typed. DATETIME avoids JVM and
Spark-session time-zone conversion by using Doris text. The verified ARRAY, MAP, STRUCT, LARGEINT,
JSON/JSONB, VARIANT, IP, and Doris 4 wide-decimal forms use JDBC `getString`; BITMAP and HLL use
explicit base64 projections. The exact two-version contract and probe-only DDL forms are recorded
in [Testing](testing.md#type-contract-evidence).

Gravitino 1.3 obtains Doris logical types through JDBC metadata, which can erase complex
containers, report LARGEINT as INTEGER, and report sketch, VARIANT, or IP types through lossy or
generic metadata. The FE type is authoritative only for evidence-backed pairs: ARRAY, MAP, STRUCT,
and LARGEINT with their observed signed-integer placeholders; BITMAP with its observed external BIT
placeholder; and HLL with its observed external `UNKNOWN`/`OTHER` placeholder. For other FE
families, generic `UNKNOWN`/`OTHER` metadata is accepted only for the separately verified
JSON/JSONB, VARIANT, and IP families. An unlisted pair or future FE type fails closed. DATETIME
consumes the full type string, enforces the 0..6 precision range, and compares an explicitly
governed precision; external decimals require a complete family/precision/scale signature;
unsigned integers require the same base width. Column count, order, case-insensitive name, and
nullability remain strict. Lossless scalars retain directional type checks. The adapter does not
rely on a private or synthetic table marker property.

The strict profile never consults FE metadata. It is certified separately for the JDBC-lossless
scalar schemas in the strict IT matrix; it does not inherit the FE-authoritative normalization
contract for the JDBC-lossy families above.

The compatibility overload without FE type names is a conservative legacy path, not a production
type-identity source. It permits lossless scalars plus an unspecified-precision timestamp and
binary normalization that can be proven from the logical and Catalyst types alone. An explicitly
governed timestamp precision cannot be checked without the FE type name and fails closed. External,
generic, unsigned, complex, sketch, and future types also fail closed on that path and cannot be
promoted into the two-version support matrix.

The per-catalog physical-schema cache is bounded by size and TTL. One successful authorized table
load uses one immutable pair of Catalyst fields and physical type names for compatibility
validation, execution schema, and projection planning; this is not a cross-system transactional
snapshot. Authorization and current logical-schema comparison run on every load, including cache
hits. A compatible cache hit does not access its physical metadata endpoint again. If a cached
snapshot fails compatibility because Doris DDL changed, the adapter conditionally removes only
that exact snapshot, coalesces one fresh transport-specific load, and validates once more. This
bounded revalidation lets Spark resolve `REFRESH TABLE`, whose analysis loads the relation before
calling catalog invalidation. A mismatch from a cache miss or from the one fresh replacement still
fails closed; physical load failures are never retried. `invalidateTable` invalidates only the
precise table key, expiry reloads the complete pair, and a zero TTL retains no cached snapshot.

## Credential and trust boundary

Catalog properties visible to Spark do not contain Gravitino hidden JDBC credentials. The adapter
obtains one `JdbcCredential`, applies endpoint and credentials after user-controlled options, and
keeps connection material out of properties and `toString` methods.

The official Doris reader needs credentials in trusted driver and executor memory. Deployment must
therefore treat both as trusted principals. Tests separately inspect `EXPLAIN`, driver logs, executor
logs, errors, and object rendering. Credential rotation takes effect when the Spark catalog is
reinitialized, normally by recreating the Spark session.

## Batch write and mutation boundary

`DorisCapabilityPolicy` always exposes `BATCH_READ`. It adds `BATCH_WRITE` only when the catalog
selects `doris-write-mode=batch` and the Spark patch is 3.5.3 or newer. It adds `TRUNCATE` only for
the separate `doris-write-overwrite-mode=truncate` opt-in. It never exposes streaming write,
overwrite-by-filter, or dynamic overwrite. Direct calls to disabled entry points fail closed.

The write-aware table load requests Gravitino `MODIFY_TABLE` before it constructs a physical write
delegate or performs Doris I/O. The authorized context retains the original official Doris table,
the logical Gravitino table, the immutable physical snapshot, and the write policy without
rendering credentials. Strict JDBC TLS has no native physical table and rejects all writes.

Write schema validation requires exact column count, order, case-sensitive names, safe nullability,
and the certified lossless Catalyst types. Read-normalized families remain unwritable except Doris
DATETIME. For DATETIME, the writer accepts an exact precision-specific String for precision 0..6;
Spark Timestamp inputs use Spark's normal assignment cast at analysis time, governed by
`spark.sql.session.timeZone`, and the resulting String is checked per row. The wrapper validates
before delegating every `InternalRow` to the official writer.

The forced data-plane configuration is Stream Load, 2PC enabled, strict mode enabled,
`max_filter_ratio=0`, schemaless disabled, and automatic redirect disabled. User-controlled raw or
bypass options cannot replace those values. Driver commit/abort still delegates to the official
connector's per-writer transaction messages; the project does not claim job-wide atomic commit.
Append is supported. Optional full-table truncate uses the official non-atomic SQL
`TRUNCATE TABLE`-then-load path and may leave an empty or partially loaded table after a later
failure. Every Catalog DDL
method continues to route through the rejecting mutation delegate, and streaming write, CTAS,
UPDATE, DELETE, and MERGE remain unsupported.

## Requirement traceability

| Requirement | Implementation | Test evidence |
| --- | --- | --- |
| Independent public dependencies | Gradle modules and version catalog | dependency resolution and distribution smoke |
| Gravitino pattern reuse | plugin, catalog facade, provider | plugin/provider/authorization unit tests and end-to-end IT |
| Aggregate and Top-N | hybrid SQL lane | planner unit tests and differential IT plans/results |
| JDBC performance parity | Spark `JDBCTable`, standard partition tuple, fetch size | four-partition direct-JDBC parity IT |
| No readable type rejects the table | read-schema normalization | type-matrix unit tests and both Doris-version IT lanes |
| Governed batch append | patch-aware capability policy plus official Stream Load delegate | Spark 3.5.3 smoke and both Doris-version IT lanes |
| First-row-safe Arrow fallback | partition-reader state machine and application-scoped circuit | reader unit matrix and real transport-failure IT |
| JDBC configuration fails closed before I/O | shared `jdbc-security` module on server and Spark | exhaustive parser tests and malicious-catalog IT |
| Authorization denial avoids observed Doris entry points | fresh catalog managers plus HTTP/TCP recorders | direct and SQL denial IT on both Doris versions |
| Verified JDBC transport | strict profile, JVM truststore, JDBC-only schema/read catalog | trusted CA plus unknown-CA, hostname, expiry, and plaintext failures on both Doris versions |
| Production dependency boundary is auditable | strict distribution lock, SHA-256 verification, immutable workflow actions, archive scanner | empty-cache build, compatibility matrix, and distribution contract |
| Connector/J remains external | Driver preflight plus zero-class archive scan | missing/present Driver IT and all three distribution forms |
