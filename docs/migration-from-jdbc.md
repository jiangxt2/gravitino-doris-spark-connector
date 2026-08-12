# Migration from Spark JDBC

## What changes

| Dimension | Generic Spark JDBC | Governed Doris connector |
| --- | --- | --- |
| Authorization | Spark/JDBC configuration controls access | Gravitino `SELECT_TABLE` must succeed before Doris is contacted |
| Credentials | Usually supplied in Spark options | Vended from hidden Gravitino JDBC properties and protected from option override |
| Detail scan | JDBC partitions only when configured | Native Doris tablet scan for lossless scalar projections |
| Aggregate and Top-N | Spark JDBC V2 pushdown | Same Spark JDBC V2 planner through the SQL lane |
| JDBC partition/fetch controls | Standard Spark options | Same controls exposed as governed catalog properties |
| Doris-specific types | JDBC metadata may be lossy or unsupported | `hybrid` provides FE-aware String/base64 normalization for the explicit two-version matrix; strict remains JDBC-only |
| DATETIME | JDBC/Catalyst conversion may depend on time zone | Doris text representation remains stable across Spark session time zones |
| Schema drift | Physical schema only | Directional comparison with governed Gravitino metadata |
| Writes and DDL | Provider-dependent | Explicitly rejected in the initial release |
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
