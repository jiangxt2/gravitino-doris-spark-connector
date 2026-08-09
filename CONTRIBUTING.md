# Contributing

Use Java 17 and keep changes within this standalone repository. Do not copy unpublished Gravitino
framework classes or add `mavenLocal`/snapshot dependencies.

Before proposing a change, run:

```bash
./gradlew spotlessCheck test installDist
./gradlew integrationTest -PdorisVersion=3.0.6.2
./gradlew integrationTest -PdorisVersion=4.0.6
```

Every behavior change needs a focused unit test. Database, planner, credential, container, or
integration changes also need the corresponding real-infrastructure test. Never skip a failing test
to obtain a green build.

Code, comments, commit messages, and public documentation are written in English. Commits must
include a `Signed-off-by` trailer and follow the repository's established message style.
