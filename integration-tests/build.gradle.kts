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

description = "Standalone Gravitino, Spark, and Doris integration tests"

sourceSets {
  create("integrationTest") {
    java.setSrcDirs(listOf("src/integrationTest/java"))
    resources.setSrcDirs(listOf("src/integrationTest/resources"))
    compileClasspath += sourceSets.main.get().output + configurations.testRuntimeClasspath.get()
    runtimeClasspath += output + compileClasspath
  }
}

configurations {
  named("integrationTestImplementation") { extendsFrom(configurations.testImplementation.get()) }
  named("integrationTestRuntimeOnly") { extendsFrom(configurations.testRuntimeOnly.get()) }
}

dependencies {
  testImplementation(project(":spark-3.5"))
  testImplementation(project(":server-provider-1.3"))
  testImplementation(libs.gravitino.spark.runtime35)
  testImplementation(libs.gravitino.client)
  testRuntimeOnly(libs.gravitino.client.runtime)
  testImplementation(libs.doris.spark.connector)
  testImplementation(libs.mysql.driver)
  testImplementation(libs.spark.core)
  testImplementation(libs.spark.sql)
  testImplementation(libs.testcontainers)
  testImplementation(libs.testcontainers.junit)
  // testcontainers and Docker Compose output is otherwise silently swallowed
  // on CI, making any infrastructure startup failure impossible to diagnose.
  // The slf4j binding is log4j-slf4j2-impl (see log4j2-test.xml), so no
  // second slf4j implementation may appear on the classpath.
}

val integrationTest by tasks.registering(Test::class) {
  description = "Runs real-infrastructure integration tests."
  group = LifecycleBasePlugin.VERIFICATION_GROUP
  testClassesDirs = sourceSets["integrationTest"].output.classesDirs
  classpath = sourceSets["integrationTest"].runtimeClasspath
  shouldRunAfter(tasks.test)
  dependsOn(":distribution:installDist")
  systemProperty("doris.version", providers.gradleProperty("dorisVersion").orElse("3.0.6.2").get())
  systemProperty("connector.repository.root", rootProject.projectDir.absolutePath)
  environment("SPARK_USER", "doris_it_reader")
  maxHeapSize = "3g"
  jvmArgs(
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED")
  doFirst {
    systemProperty(
        "connector.provider.directory",
        project(":distribution")
            .layout
            .buildDirectory
            .dir("install/gravitino-doris-spark-connector/gravitino/catalogs/doris-governed")
            .get()
            .asFile
            .absolutePath)
  }
  outputs.upToDateWhen { false }
}

tasks.check { dependsOn(tasks.test) }
