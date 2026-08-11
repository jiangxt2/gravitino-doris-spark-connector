/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.jar.JarFile
import org.gradle.api.tasks.bundling.Tar
import org.gradle.api.tasks.bundling.Zip

plugins {
  distribution
}

val distributionName = "gravitino-doris-spark-connector"

fun Configuration.configureRuntimeVariant(bundling: String) {
  isCanBeConsumed = false
  isCanBeResolved = true
  attributes {
    attribute(Category.CATEGORY_ATTRIBUTE, objects.named(Category.LIBRARY))
    attribute(Usage.USAGE_ATTRIBUTE, objects.named(Usage.JAVA_RUNTIME))
    attribute(LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE, objects.named(LibraryElements.JAR))
    attribute(Bundling.BUNDLING_ATTRIBUTE, objects.named(bundling))
    attribute(
        TargetJvmEnvironment.TARGET_JVM_ENVIRONMENT_ATTRIBUTE,
        objects.named(TargetJvmEnvironment.STANDARD_JVM))
  }
}

val sparkRuntime by configurations.creating {
  configureRuntimeVariant(Bundling.EXTERNAL)
}
val shadedSparkRuntime by configurations.creating {
  configureRuntimeVariant(Bundling.SHADOWED)
}
val serverRuntime by configurations.creating {
  configureRuntimeVariant(Bundling.EXTERNAL)
}

listOf(sparkRuntime, shadedSparkRuntime, serverRuntime).forEach {
  it.resolutionStrategy.activateDependencyLocking()
}

dependencies {
  sparkRuntime(project(":spark-3.5"))
  sparkRuntime(libs.doris.spark.connector)
  shadedSparkRuntime(libs.gravitino.spark.runtime35) { isTransitive = false }

  serverRuntime(project(":server-provider-1.3"))
  serverRuntime(libs.gravitino.catalog.jdbc.doris)
  serverRuntime(libs.gravitino.catalog.jdbc.common)
}

distributions {
  main {
    distributionBaseName.set(distributionName)
    contents {
      from(rootProject.file("LICENSE"))
      from(rootProject.file("NOTICE"))
      from(rootProject.file("DEPENDENCIES.md"))
      from(rootProject.file("README.md"))
      into("docs") { from(rootProject.file("docs")) }
      into("spark/jars") { from(sparkRuntime, shadedSparkRuntime) }
      into("gravitino/catalogs/doris-governed/libs") {
        from(serverRuntime) {
          exclude("guava-*.jar")
          exclude("log4j-*.jar")
          exclude("slf4j-*.jar")
          exclude("error_prone_annotations-*.jar")
        }
      }
      into("gravitino/catalogs/doris-governed/conf") {
        from(project(":server-provider-1.3").file("src/main/resources/doris-governed.conf"))
      }
    }
  }
}

fun FileTree.archivePaths(rootDirectoryName: String): Set<String> {
  val paths = mutableSetOf<String>()
  visit {
    if (!isDirectory) {
      val path = relativePath.pathString
      paths += path.removePrefix("$rootDirectoryName/")
    }
  }
  return paths
}

fun Iterable<File>.assertNoMysqlDriverClasses(scope: String) {
  forEach { jar ->
    JarFile(jar).use { archive ->
      if (archive.getJarEntry("com/mysql/cj/jdbc/Driver.class") != null) {
        throw GradleException("$scope contains the MySQL Connector/J driver class in ${jar.name}")
      }
    }
  }
}

val verifyDistributionDependencyContract by tasks.registering {
  group = LifecycleBasePlugin.VERIFICATION_GROUP
  description = "Verifies the packaged dependency boundary of every distribution form."
  dependsOn(tasks.installDist, tasks.distTar, tasks.distZip)

  doLast {
    val archiveRoot = "$distributionName-${project.version}"
    val installRoot = layout.buildDirectory.dir("install/$distributionName").get().asFile
    val tarFile = tasks.named<Tar>("distTar").get().archiveFile.get().asFile
    val zipFile = tasks.named<Zip>("distZip").get().archiveFile.get().asFile
    val tarContents = tarTree(tarFile)
    val zipContents = zipTree(zipFile)

    val installPaths =
        installRoot
            .walkTopDown()
            .filter(File::isFile)
            .map { file -> file.relativeTo(installRoot).invariantSeparatorsPath }
            .toSet()
    val tarPaths = tarContents.archivePaths(archiveRoot)
    val zipPaths = zipContents.archivePaths(archiveRoot)
    if (installPaths != tarPaths || installPaths != zipPaths) {
      throw GradleException("installDist, distTar, and distZip contain different file sets")
    }

    val requiredPaths =
        setOf(
            "LICENSE",
            "NOTICE",
            "DEPENDENCIES.md",
            "README.md",
            "gravitino/catalogs/doris-governed/conf/doris-governed.conf")
    val missingPaths = requiredPaths - installPaths
    if (missingPaths.isNotEmpty()) {
      throw GradleException(
          "Distribution is missing required files: ${missingPaths.sorted().joinToString(", ")}")
    }

    val dependencyInventory = rootProject.file("DEPENDENCIES.md").readText()
    val packagedHeading = "## Packaged runtime components"
    val providedHeading = "## Target-provided runtime components"
    val externalHeading = "## External deployment prerequisite"
    val buildTestHeading = "## Build and test-only dependencies"
    val packagedStart = dependencyInventory.indexOf(packagedHeading)
    val providedStart = dependencyInventory.indexOf(providedHeading)
    val externalStart = dependencyInventory.indexOf(externalHeading)
    val buildTestStart = dependencyInventory.indexOf(buildTestHeading)
    if (packagedStart < 0
        || providedStart <= packagedStart
        || externalStart <= providedStart
        || buildTestStart <= externalStart) {
      throw GradleException("DEPENDENCIES.md is missing the required dependency sections")
    }
    val preExternalSections = dependencyInventory.substring(packagedStart, externalStart).lowercase()
    val externalSection = dependencyInventory.substring(externalStart, buildTestStart).lowercase()
    val buildTestSection = dependencyInventory.substring(buildTestStart).lowercase()
    if (preExternalSections.contains("mysql connector/j")
        || preExternalSections.contains("com.mysql:mysql-connector-j")) {
      throw GradleException("DEPENDENCIES.md classifies MySQL Connector/J as packaged")
    }
    if (!externalSection.contains("com.mysql:mysql-connector-j")
        || !externalSection.contains("not redistributed")) {
      throw GradleException(
          "DEPENDENCIES.md must classify MySQL Connector/J as an external prerequisite that is not redistributed")
    }
    if (!buildTestSection.contains("org.junit:junit-bom")
        || !buildTestSection.contains("org.testcontainers:testcontainers")) {
      throw GradleException("DEPENDENCIES.md is missing the build and test-only dependency inventory")
    }

    val mysqlDriverFiles =
        installPaths.filter { path ->
          val normalized = path.lowercase()
          normalized.contains("mysql-connector-java") || normalized.contains("mysql-connector-j")
        }
    if (mysqlDriverFiles.isNotEmpty()) {
      throw GradleException(
          "Distribution contains MySQL Connector/J files: " + mysqlDriverFiles.joinToString(", "))
    }

    val sparkJars = installPaths.filter { it.startsWith("spark/jars/") && it.endsWith(".jar") }
    val serverJars =
        installPaths.filter {
          it.startsWith("gravitino/catalogs/doris-governed/libs/") && it.endsWith(".jar")
        }
    if (sparkJars.none { it.substringAfterLast('/').startsWith("jdbc-security-") }
        || serverJars.none { it.substringAfterLast('/').startsWith("jdbc-security-") }) {
      throw GradleException("Both Spark and Gravitino distributions must contain jdbc-security")
    }
    val duplicatedSparkRuntime =
        sparkJars.filter {
          val name = it.substringAfterLast('/')
          name.startsWith("spark-core_")
              || name.startsWith("spark-sql_")
              || name.startsWith("spark-catalyst_")
        }
    if (duplicatedSparkRuntime.isNotEmpty()) {
      throw GradleException(
          "Spark runtime must be provided by the target installation: "
              + duplicatedSparkRuntime.joinToString(", "))
    }
    val forbiddenServerJars =
        serverJars.filter {
          val name = it.substringAfterLast('/')
          name.startsWith("guava-")
              || name.startsWith("log4j-")
              || name.startsWith("slf4j-")
              || name.startsWith("error_prone_annotations-")
        }
    if (forbiddenServerJars.isNotEmpty()) {
      throw GradleException(
          "Server runtime contains target-provided libraries: "
              + forbiddenServerJars.joinToString(", "))
    }

    installRoot.walkTopDown().filter { it.isFile && it.extension == "jar" }.toList()
        .assertNoMysqlDriverClasses("installDist")
    tarContents.matching { include("**/*.jar") }.files
        .assertNoMysqlDriverClasses("distTar")
    zipContents.matching { include("**/*.jar") }.files
        .assertNoMysqlDriverClasses("distZip")
  }
}

tasks.check { dependsOn(verifyDistributionDependencyContract) }

val resolveDistributionLocks by tasks.registering {
  group = LifecycleBasePlugin.BUILD_GROUP
  description = "Resolves every production distribution configuration for dependency locking."
  doLast {
    listOf(sparkRuntime, shadedSparkRuntime, serverRuntime).forEach(Configuration::resolve)
  }
}

verifyDistributionDependencyContract.configure { dependsOn(resolveDistributionLocks) }
