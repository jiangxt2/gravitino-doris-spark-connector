# Contributing

Use Java 17 and keep changes within this standalone repository. Do not copy unpublished Gravitino
framework classes or add `mavenLocal`/snapshot dependencies.

Before proposing a change, run:

```bash
./gradlew spotlessCheck test installDist rat \
  :distribution:resolveDistributionLocks \
  :distribution:verifyDistributionDependencyContract \
  verifySparkDependencyVersions
./gradlew integrationTest -PdorisVersion=3.0.6.2
./gradlew integrationTest -PdorisVersion=4.0.6
```

Every behavior change needs a focused unit test. Database, planner, credential, container, or
integration changes also need the corresponding real-infrastructure test. Never skip a failing test
to obtain a green build.

Java compilation runs Error Prone together with `-Xlint:all -Werror`. The root `rat` task checks
source and comment-capable configuration files for Apache License headers; exclusions are limited
to generated metadata, wrapper artifacts, Markdown, and other fixed non-header file formats.

## Updating dependencies

Production distribution dependency changes must update and review strict lock state explicitly:

```bash
./gradlew :distribution:resolveDistributionLocks --write-locks
```

Review `distribution/gradle.lockfile` for the expected coordinates, versions, and configuration
membership. Do not use lenient locking, dynamic versions, snapshots, or a CI job that rewrites
locks. Spark compatibility test classpaths are intentionally not locked to the default patch; the
three resolved-version gates enforce 3.5.0, 3.5.8, and 3.5.9 independently.

When a plugin or external dependency changes, regenerate SHA-256 verification metadata only after
running the complete default and both Spark-boundary task sets with
`--write-verification-metadata sha256`. Review every changed component, artifact name, version, and
checksum before accepting it. A checksum records reviewed bytes and is not evidence of publisher
identity. Ordinary builds must never rewrite `gradle/verification-metadata.xml`.

All workflow `uses:` references must use a verified full 40-character commit SHA with the release
tag in an inline comment. Do not replace them with a major tag, branch, or short SHA.

Code, comments, commit messages, and public documentation are written in English. Commits must
include a `Signed-off-by` trailer and follow the repository's established message style.
