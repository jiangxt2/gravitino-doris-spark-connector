# Migration from Spark JDBC

## What changes

| Dimension | Generic Spark JDBC | Governed Doris connector |
| --- | --- | --- |
| Authorization | Spark/JDBC configuration controls access | Gravitino `SELECT_TABLE` must succeed before Doris is contacted |
| Credentials | Usually supplied in Spark options | Vended from hidden Gravitino JDBC properties and protected from option override |
| Detail scan | JDBC partitions only when configured | Native Doris tablet scan for lossless scalar projections |
| Aggregate and Top-N | Spark JDBC V2 pushdown | Same Spark JDBC V2 planner through the SQL lane |
| JDBC partition/fetch controls | Standard Spark options | Same controls exposed as governed catalog properties |
| Doris-specific types | JDBC metadata may be lossy or unsupported | FE-aware String/base64 normalization for the explicit two-version matrix; unlisted types fail closed |
| DATETIME | JDBC/Catalyst conversion may depend on time zone | Doris text representation remains stable across Spark session time zones |
| Schema drift | Physical schema only | Directional comparison with governed Gravitino metadata |
| Writes and DDL | Provider-dependent | Explicitly rejected in the initial release |
| Gravitino views | JDBC query text can represent a view | Not exposed by the initial table-only adapter |

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

Also configure both required native-lane properties: `doris-fenodes` and `doris-query-port`.

## Support statement

Apache Gravitino 1.3.0's published documentation may describe Doris as unsupported for the generic
Spark connector. Installing this independent server provider and Spark runtime makes Doris
supported by this project for the exact matrix documented in the README. It does not change the
capability or support statement of an unmodified Gravitino distribution.

Keep existing `jdbc-doris` catalogs unchanged during migration. Create a separate
`doris-governed` catalog, grant a pilot role, compare results and plans, then move workloads. The
wrapper plugin intentionally never intercepts or removes `jdbc-doris`.
