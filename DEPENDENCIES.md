# Runtime dependencies

The standalone distribution is assembled from published Maven Central artifacts. The following
versions are the compatibility contract of release `0.1.0-SNAPSHOT`:

| Component | Coordinate or module | Version | License |
| --- | --- | --- | --- |
| Apache Gravitino Spark runtime | `org.apache.gravitino:gravitino-spark-connector-runtime-3.5_2.12` | 1.3.0 | Apache-2.0 |
| Apache Gravitino Doris catalog | `org.apache.gravitino:gravitino-catalog-jdbc-doris` | 1.3.0 | Apache-2.0 |
| Apache Doris Spark Connector | `org.apache.doris:spark-doris-connector-spark-3.5` | 26.0.0 | Apache-2.0 |
| Apache Spark | `spark-core_2.12`, `spark-sql_2.12`, `spark-catalyst_2.12` | 3.5.3 | Apache-2.0 |
| Scala library | `org.scala-lang:scala-library` | 2.12.18 | BSD-3-Clause |
| MySQL Connector/J | `com.mysql:mysql-connector-j` (Maven relocation from `mysql:mysql-connector-java`) | 8.0.33 | GPL-2.0 with Universal FOSS Exception |
| Caffeine | `com.github.ben-manes.caffeine:caffeine` | 2.9.3 | Apache-2.0 |

Spark itself is supplied by the target Spark installation; its coordinates are compile-time and
test baselines rather than duplicate Spark binaries in `spark/jars`. Spark-side Gravitino APIs are
provided by the published shaded Gravitino runtime, while server APIs are supplied by the target
Gravitino 1.3.0 installation.

The Spark-side modules deliberately use JDK collections and validation rather than bundling an
unshaded Guava JAR into Spark's shared class path. Guava used to compile the server provider is
supplied by the target Gravitino server and is not copied into the distribution.

The Gravitino Spark runtime is its published shaded JAR; its POM dependencies are deliberately not
expanded into duplicate connector JARs. The server directory follows Gravitino's own JDBC Doris
packaging exclusions for Guava and logging libraries supplied by the server. Other required
transitive runtime JARs retain their packaged `META-INF` license and notice resources. Inspect the
exact resolved graph for a build with:

```bash
./gradlew :distribution:dependencies --configuration sparkRuntime
./gradlew :distribution:dependencies --configuration shadedSparkRuntime
./gradlew :distribution:dependencies --configuration serverRuntime
```
