# Migration from Spark JDBC

## What changes

| Dimension | Generic Spark JDBC | Governed Doris connector |
| --- | --- | --- |
| Authorization | Spark/JDBC configuration controls access | Gravitino `SELECT_TABLE` must succeed before Doris is contacted |
| Credentials | Usually supplied in Spark options | Vended from hidden Gravitino JDBC properties and protected from option override |
| Detail scan | JDBC partitions only when configured | Native Doris tablet scan for lossless scalar projections |
| Aggregate and Top-N | Spark JDBC V2 pushdown | Same Spark JDBC V2 planner through the SQL lane |
| Optional Arrow | Not used | Experimental native-detail transport; disabled by default with first-row-safe Thrift fallback |
| JDBC partition/fetch controls | Standard Spark options | Same controls exposed as governed catalog properties |
| Doris-specific types | JDBC metadata may be lossy or unsupported | `hybrid` provides FE-aware String/base64 normalization for the explicit two-version matrix; strict remains JDBC-only |
| DATETIME | JDBC/Catalyst conversion may depend on time zone | Doris text representation remains stable across Spark session time zones |
| Schema drift | Physical schema only | Directional comparison with governed Gravitino metadata |
| Batch append | Standard JDBC PreparedStatement batches | Opt-in official Doris Stream Load + forced 2PC on Spark 3.5.3+ |
| Overwrite | JDBC/provider-dependent | Rejected by default; optional non-atomic SQL truncate-then-load only |
| Streaming write and DDL | Provider-dependent | Explicitly rejected |
| Gravitino views | JDBC query text can represent a view | Not exposed by the initial table-only adapter |
| Verified TLS | Connector/J configuration is owned by each Spark job | `strict-jdbc-tls` binds Gravitino metadata, Spark schema, and reads to one `VERIFY_IDENTITY` JDBC URL |

## Performance interpretation

The connector does not claim every query is faster than JDBC. Its enforceable contract is that a
comparable SQL-lane scan uses Spark's same `JDBCTable/JDBCScanBuilder`, partition tuple, fetch size,
and supported pushdowns. Lossless detail scans use the Doris tablet reader instead of falling back
to a single JDBC task.

String normalization necessarily uses the SQL lane. Configure partitioning for large normalized
detail scans. Aggregate catalogs should normally omit JDBC partitioning so Spark can prove complete
aggregate pushdown and Doris returns the reduced result.

Arrow is not an automatic upgrade over the tablet Thrift reader. It uses the same native-detail
planning boundary, remains row-converted by the official connector, and may lose to Thrift because
of workload shape or server/client overhead. Enable `preferred` only after measuring the target
workload. A classified transport failure before the first row switches that partition to Thrift;
after one eligible failure, the same Spark application remains on Thrift until restart.

For writes, migration changes the data plane from JDBC PreparedStatement batches to the official
Doris Stream Load writer. Compare with equal Spark partitions and equal sink controls. The
governed path adds Gravitino `MODIFY_TABLE`, credential vending, and schema validation, then forces
2PC, strict mode, zero filter tolerance, schemaless off, and automatic redirect off. 2PC applies to
the official per-writer transactions and does not make all Spark partitions one atomic transaction.
Keep JDBC or an application-level staging/swap protocol if atomic full-table replacement is a hard
requirement.

## Property migration

| Existing Spark JDBC option | Governed catalog property |
| --- | --- |
| `url` | `jdbc-url` |
| `driver` | `jdbc-driver` |
| `user` | hidden `jdbc-user` |
| `password` | hidden `jdbc-password` |
| `partitionColumn` | `doris-jdbc-partition-column` |
| `lowerBound` | `doris-jdbc-lower-bound` |
| `upperBound` | `doris-jdbc-upper-bound` |
| `numPartitions` | `doris-jdbc-num-partitions` |
| `fetchsize` | `doris-jdbc-fetch-size` |

New, connector-specific opt-ins have no direct JDBC equivalents:

| Governed catalog property | Migration meaning |
| --- | --- |
| `doris-arrow-flight-sql-mode=preferred` | Try Arrow only for native detail scans; default is `disabled` |
| `doris-arrow-flight-sql-port` | Server-managed FE Flight port required by `preferred` |
| `doris-write-mode=batch` | Enable Stream Load append on Spark 3.5.3+; default is `disabled` |
| `doris-write-overwrite-mode=truncate` | Enable only non-atomic full-table truncate overwrite; default is `reject` |

For hybrid/native behavior, configure both `doris-fenodes` and `doris-query-port`. For a direct
JDBC-style security boundary, select `doris-read-transport=strict-jdbc-tls`, add
`sslMode=VERIFY_IDENTITY` to `jdbc-url`, omit both native endpoint properties, and deploy the same
CA truststore to Gravitino, the Spark driver, and every executor.

Choose `hybrid` when the documented FE-aware Doris-specific type matrix is required. Choose strict
for the single verified JDBC trust chain and only after validating that the table's schema is
losslessly represented by JDBC metadata; strict never consults FE HTTP to recover a lossy type.

## Support statement

Apache Gravitino 1.3.0's published documentation may describe Doris as unsupported for the generic
Spark connector. Installing this independent server provider and Spark runtime makes Doris
supported by this project for the exact matrix documented in the README. It does not change the
capability or support statement of an unmodified Gravitino distribution.

Keep existing `jdbc-doris` catalogs unchanged during migration. Create a separate
`doris-governed` catalog, grant a pilot role, compare results and plans, then move workloads. The
wrapper plugin intentionally never intercepts or removes `jdbc-doris`.

Run read and write correctness comparisons before performance measurement. The project performance
harness includes Gravitino JDBC, bare official Connector, and governed lanes on the same Spark
Standalone application, reports governance overhead separately from data-plane differences, and
caps fixtures at ten million rows. A result is evidence only for its recorded workload, row count,
partitioning, Doris/Spark versions, and confidence interval.
