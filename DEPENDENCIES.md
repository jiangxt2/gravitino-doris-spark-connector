# Dependency inventory

The standalone distribution is assembled from published Maven Central artifacts. The following
versions are the compatibility contract of release `0.1.0-SNAPSHOT`.

## Packaged runtime components

| Component | Coordinate or module | Version | License |
| --- | --- | --- | --- |
| Apache Gravitino Spark runtime | `org.apache.gravitino:gravitino-spark-connector-runtime-3.5_2.12` | 1.3.0 | Apache-2.0 |
| Apache Gravitino Doris catalog | `org.apache.gravitino:gravitino-catalog-jdbc-doris` | 1.3.0 | Apache-2.0 |
| Apache Doris Spark Connector | `org.apache.doris:spark-doris-connector-spark-3.5` | 26.0.0 | Apache-2.0 |
| Caffeine | `com.github.ben-manes.caffeine:caffeine` | 2.9.3 | Apache-2.0 |

Transitive packaged JARs are fixed by `distribution/gradle.lockfile`. The archive contract excludes
Guava, logging APIs/implementations, Error Prone annotations, and Spark core/SQL/Catalyst when they
are supplied by the target Gravitino or Spark installation.

## Target-provided runtime components

| Component | Coordinate or module | Tested version | Role |
| --- | --- | --- | --- |
| Apache Spark | `spark-core_2.12`, `spark-sql_2.12`, `spark-catalyst_2.12` | 3.5.8 | Supplied by the Spark installation; 3.5.0 and 3.5.9 are compatibility boundaries |
| Scala library | `org.scala-lang:scala-library` | 2.12.18 | Supplied by Spark |
| Apache Gravitino Server APIs | Gravitino server distribution | 1.3.0 | Supplied by the Gravitino installation |

Spark-side Gravitino APIs are provided by the published shaded Gravitino runtime, while server APIs
are supplied by the target Gravitino 1.3.0 installation.

The supported Spark series is 3.5.x with Scala 2.12. Version 3.5.8 is the default dependency and
the full Doris integration-test baseline. Versions 3.5.0 and 3.5.9 are compatibility-smoke
boundaries and are not packaged into the distribution.

The Spark-side modules deliberately use JDK collections and validation rather than bundling an
unshaded Guava JAR into Spark's shared class path. Guava used to compile the server provider is
supplied by the target Gravitino server and is not copied into the distribution.

## External deployment prerequisite

| Component | Coordinate or module | Tested version | Role |
| --- | --- | --- | --- |
| MySQL Connector/J | `com.mysql:mysql-connector-j` | 8.0.33 | Installed separately on Gravitino Server, Spark driver, and every executor; also used by tests |

MySQL Connector/J is licensed under GPL-2.0 with the Universal FOSS Exception. It is resolved for
compilation and tests but is not redistributed in `installDist`, tar, or zip. The distribution gate
checks both filenames and every packaged JAR entry for `com/mysql/cj/jdbc/Driver.class`.

## Build and test-only dependencies

| Component | Coordinate or plugin | Version | Role |
| --- | --- | --- | --- |
| JUnit Jupiter and Platform | `org.junit:junit-bom` | 5.11.4 | Unit and integration test engine |
| AssertJ | `org.assertj:assertj-core` | 3.26.3 | Test assertions |
| Mockito | `org.mockito:mockito-core` | 5.14.2 | Unit-test doubles and interaction assertions |
| Testcontainers | `org.testcontainers:testcontainers`, `org.testcontainers:junit-jupiter` | 1.21.4 | Real Gravitino and Doris infrastructure tests |
| Spotless | `com.diffplug.spotless` | 6.25.0 | Build-time formatting gate using google-java-format 1.22.0 |

These components are not copied into the standalone runtime distribution. Connector/J also appears
on compilation and test classpaths to exercise the separately installed runtime prerequisite.

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

The same three production configurations are locked in strict mode. All Gradle-downloaded plugin,
artifact, and metadata inputs used by the default build, Spark boundary builds, and integration
test classpaths are pinned by SHA-256 in `gradle/verification-metadata.xml`. Checksums protect the
integrity of reviewed bytes; they do not establish publisher identity.

This development distribution does not claim an embedded or final-binary SBOM. Release operations
must scan the completed tar and zip with a separately fixed and checksum-verified binary scanner,
publish sidecar CycloneDX JSON, and bind it to the same archive digest/signature/attestation.
