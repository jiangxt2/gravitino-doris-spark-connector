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

import com.diffplug.gradle.spotless.SpotlessExtension
import org.gradle.api.artifacts.component.ModuleComponentIdentifier
import org.gradle.api.tasks.testing.logging.TestExceptionFormat

plugins {
  base
  alias(libs.plugins.spotless) apply false
}

val dependencyCatalog = libs
val sparkVersionPattern = Regex("""^3\.5\.\d+$""")
val expectedSparkVersion =
    providers.gradleProperty("sparkVersion").orElse(dependencyCatalog.versions.spark)

if (!sparkVersionPattern.matches(expectedSparkVersion.get())) {
  throw GradleException("sparkVersion must match 3.5.<non-negative numeric patch>")
}

val sparkRuntimeConfigurations =
    mapOf(
        ":spark-common" to "testRuntimeClasspath",
        ":spark-3.5" to "testRuntimeClasspath",
        ":integration-tests" to "integrationTestRuntimeClasspath")
val requiredSparkModules =
    setOf("spark-core_2.12", "spark-sql_2.12", "spark-catalyst_2.12")

allprojects {
  group = "io.github.jiangxt2.gravitino.doris"
  version = providers.gradleProperty("connectorVersion").get()
}

subprojects {
  configurations.configureEach {
    resolutionStrategy.eachDependency {
      if (requested.group == "org.apache.spark") {
        useVersion(expectedSparkVersion.get())
        because("all Spark modules must use the requested Spark 3.5 patch")
      }
    }
  }

  if (name != "distribution") {
    apply(plugin = "java-library")
    apply(plugin = "com.diffplug.spotless")

    extensions.configure<JavaPluginExtension> {
      toolchain { languageVersion.set(JavaLanguageVersion.of(17)) }
      withSourcesJar()
      withJavadocJar()
    }

    extensions.configure<SpotlessExtension> {
      java {
        googleJavaFormat(dependencyCatalog.versions.googleJavaFormat.get())
        licenseHeaderFile(rootProject.file("config/license-header.java"))
        removeUnusedImports()
        trimTrailingWhitespace()
        endWithNewline()
      }
      format("misc") {
        target("*.gradle.kts", "*.md", "*.yml", "*.yaml", "*.properties", "*.conf")
        targetExclude("**/build/**", "LICENSE")
        trimTrailingWhitespace()
        endWithNewline()
      }
    }

    dependencies {
      "testImplementation"(platform(dependencyCatalog.junit.bom))
      "testImplementation"(dependencyCatalog.junit.jupiter)
      "testImplementation"(dependencyCatalog.assertj.core)
      "testImplementation"(dependencyCatalog.mockito.core)
      "testRuntimeOnly"(dependencyCatalog.junit.platform.launcher)
    }

    tasks.withType<Test>().configureEach {
      useJUnitPlatform()
      testLogging {
        events("failed", "skipped")
        exceptionFormat = TestExceptionFormat.FULL
      }
    }

    tasks.withType<JavaCompile>().configureEach {
      options.encoding = "UTF-8"
      options.compilerArgs.addAll(listOf("-Xlint:all", "-Werror"))
    }
  }
}

val verifySparkDependencyVersions by tasks.registering {
  group = LifecycleBasePlugin.VERIFICATION_GROUP
  description = "Verifies that Spark-bearing test classpaths use one expected Spark version."
  inputs.property("expectedSparkVersion", expectedSparkVersion)

  doLast {
    val expectedVersion = expectedSparkVersion.get()
    sparkRuntimeConfigurations.forEach { (projectPath, configurationName) ->
      val configuration = project(projectPath).configurations.getByName(configurationName)
      val sparkComponents =
          configuration.incoming.resolutionResult.allComponents
              .mapNotNull { component -> component.id as? ModuleComponentIdentifier }
              .filter { component -> component.group == "org.apache.spark" }

      val resolvedModules = sparkComponents.map { component -> component.module }.toSet()
      val missingModules = requiredSparkModules - resolvedModules
      val mismatchedComponents =
          sparkComponents
              .filter { component -> component.version != expectedVersion }
              .map { component -> "${component.module}:${component.version}" }
              .sorted()

      if (missingModules.isNotEmpty() || mismatchedComponents.isNotEmpty()) {
        throw GradleException(
            buildString {
              append("Spark dependency verification failed for ")
              append("$projectPath:$configurationName")
              if (missingModules.isNotEmpty()) {
                append("; missing modules: ")
                append(missingModules.sorted().joinToString(", "))
              }
              if (mismatchedComponents.isNotEmpty()) {
                append("; expected version $expectedVersion but resolved: ")
                append(mismatchedComponents.joinToString(", "))
              }
            })
      }

      logger.lifecycle(
          "$projectPath:$configurationName resolved ${sparkComponents.size} " +
              "Spark modules at $expectedVersion")
    }
  }
}

tasks.named(LifecycleBasePlugin.CHECK_TASK_NAME) {
  dependsOn(verifySparkDependencyVersions)
}

tasks.register("integrationTest") {
  group = LifecycleBasePlugin.VERIFICATION_GROUP
  description = "Runs the standalone Spark, Gravitino, and Doris integration tests."
  dependsOn(":integration-tests:integrationTest")
}
