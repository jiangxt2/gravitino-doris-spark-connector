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
import org.gradle.api.tasks.testing.logging.TestExceptionFormat

plugins {
  base
  alias(libs.plugins.spotless) apply false
}

val dependencyCatalog = libs

allprojects {
  group = "io.github.jiangxt2.gravitino.doris"
  version = providers.gradleProperty("connectorVersion").get()
}

subprojects {
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

tasks.register("integrationTest") {
  group = LifecycleBasePlugin.VERIFICATION_GROUP
  description = "Runs the standalone Spark, Gravitino, and Doris integration tests."
  dependsOn(":integration-tests:integrationTest")
}
