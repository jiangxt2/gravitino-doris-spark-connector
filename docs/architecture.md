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
| facade plus storage table delegate | `GovernedDorisTable` exposes the governed schema and read-only capability set |
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
the Spark 3.5.8 real-infrastructure matrix.

## Request flow

```text
Spark SQL
  -> GovernedDorisSparkPlugin
     -> official GravitinoSparkPlugin
     -> register provider=doris-governed
  -> GovernedDorisCatalogSpark35.initialize
     -> shared JDBC URL/driver validation
     -> MySQL Driver availability preflight
     -> physical catalog construction and credential vending
  -> GovernedDorisCatalogSpark35.loadTable
     -> Gravitino TableCatalog.loadTable(identifier, SELECT_TABLE)
     -> FE physical schema snapshot and compatibility validation
     -> GovernedDorisTable
        -> native lane: official Doris tablet scan
        -> SQL lane: Spark JDBCTable/JDBCScanBuilder
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
driver class, one ordinary host and explicit port, optional single database, and structured query
parameters. It rejects embedded credentials, complex host forms, known dangerous Connector/J
names, DBCP class-loading/identity/eager-init/SQL-execution/exposure keys, encoded key variants, and
smuggling through `connectionProperties`. The DBCP string is parsed once, matching its runtime
consumption; nested `connectionProperties` is rejected instead of recursively reinterpreted.
Failures use fixed messages and never include raw connection material.

This boundary is configuration hardening, not transport certification. Strict TLS and endpoint
identity/coherence remain separate work; the current native and SQL lanes must not be described as
verified-TLS paths.

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

The adapter takes one FE schema snapshot, compares it directionally with the authorized Gravitino
table, and returns an executable Spark schema. It retains physical order, names, and nullability,
while merging governed comments.

Directly representable scalar types remain typed. DATETIME avoids JVM and Spark-session time-zone
conversion by using Doris text. Complex, JSONB, VARIANT, IP, and future FE types use JDBC
`getString`; binary and sketch families use explicit base64 projections.

Gravitino 1.3 obtains Doris logical types through JDBC metadata, which can erase complex
containers, report LARGEINT as INTEGER, and report sketch, VARIANT, IP, or future types as generic
`UNKNOWN`. The FE type is authoritative only for those proven lossy mappings and generic
placeholders. An ordinary governed scalar cannot silently drift to VARIANT or an unknown future
type. Column count, order, case-insensitive name, and nullability remain strict. Lossless scalars
and normalized families whose JDBC identity is stable (DATETIME, BINARY, and unsigned integers)
retain directional type checks. The adapter does not rely on a private or synthetic table marker
property.

The per-catalog physical-schema cache is bounded by size and TTL. Authorization always runs before
cache lookup. `invalidateTable`, including Spark `REFRESH TABLE`, invalidates the precise table key.

## Credential and trust boundary

Catalog properties visible to Spark do not contain Gravitino hidden JDBC credentials. The adapter
obtains one `JdbcCredential`, applies endpoint and credentials after user-controlled options, and
keeps connection material out of properties and `toString` methods.

The official Doris reader needs credentials in trusted driver and executor memory. Deployment must
therefore treat both as trusted principals. Tests separately inspect `EXPLAIN`, driver logs, executor
logs, errors, and object rendering. Credential rotation takes effect when the Spark catalog is
reinitialized, normally by recreating the Spark session.

## Read-only policy and extension seams

`DorisCapabilityPolicy` exposes only `BATCH_READ`. The table facade structurally implements
`SupportsWrite`, but its write-builder entry point is unreachable through the advertised
capabilities and explicitly rejects direct calls. Every Catalog DDL method already routes through
`DorisCatalogMutationDelegate`, whose initial implementation rejects mutations.
`DorisWriteDelegateFactory` is already invoked after authorization and schema validation; its
`DorisAuthorizedTableContext` retains the original official Doris table and validated read delegate
without exposing credentials through string rendering. The Spark 3.5 write-privilege load path is
also policy-gated and reuses Gravitino's `MODIFY_TABLE` authorization when enabled. A future
implementation replaces the two delegates and capability policy; it does not replace plugin
registration, authorization ordering, table facade, identifier handling, schema validation, or
credential resolution.

## Requirement traceability

| Requirement | Implementation | Test evidence |
| --- | --- | --- |
| Independent public dependencies | Gradle modules and version catalog | dependency resolution and distribution smoke |
| Gravitino pattern reuse | plugin, catalog facade, provider | plugin/provider/authorization unit tests and end-to-end IT |
| Aggregate and Top-N | hybrid SQL lane | planner unit tests and differential IT plans/results |
| JDBC performance parity | Spark `JDBCTable`, standard partition tuple, fetch size | four-partition direct-JDBC parity IT |
| No readable type rejects the table | read-schema normalization | type-matrix unit tests and both Doris-version IT lanes |
| No architecture rewrite for future writes | centralized policy and delegates | capability and explicit-rejection tests |
| JDBC configuration fails closed before I/O | shared `jdbc-security` module on server and Spark | exhaustive parser tests and malicious-catalog IT |
| Authorization denial avoids observed Doris entry points | fresh catalog managers plus HTTP/TCP recorders | direct and SQL denial IT on both Doris versions |
| Production dependency boundary is auditable | strict distribution lock, SHA-256 verification, immutable workflow actions, archive scanner | empty-cache build, compatibility matrix, and distribution contract |
| Connector/J remains external | Driver preflight plus zero-class archive scan | missing/present Driver IT and all three distribution forms |
