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

plugins { distribution }

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

dependencies {
  sparkRuntime(project(":spark-3.5"))
  sparkRuntime(libs.doris.spark.connector)
  sparkRuntime(libs.mysql.driver)
  shadedSparkRuntime(libs.gravitino.spark.runtime35) { isTransitive = false }

  serverRuntime(project(":server-provider-1.3"))
  serverRuntime(libs.gravitino.catalog.jdbc.doris)
  serverRuntime(libs.gravitino.catalog.jdbc.common)
  serverRuntime(libs.mysql.driver)
}

distributions {
  main {
    distributionBaseName.set("gravitino-doris-spark-connector")
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

tasks.check { dependsOn(tasks.distTar, tasks.distZip) }
