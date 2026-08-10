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

description = "Spark 3.5 and Scala 2.12 Doris delegate implementation"

dependencies {
  api(project(":spark-common"))
  compileOnly(libs.gravitino.spark.common)
  compileOnly(libs.gravitino.spark35)
  compileOnly(libs.doris.spark.connector)
  compileOnly(libs.spark.core)
  compileOnly(libs.spark.sql)
  compileOnly(libs.spark.catalyst)
  compileOnly(libs.scala.library)

  testImplementation(libs.gravitino.spark.common)
  testImplementation(libs.gravitino.spark35)
  testImplementation(libs.gravitino.api)
  testImplementation(libs.doris.spark.connector)
  testImplementation(libs.mysql.driver)
  testImplementation(libs.spark.core)
  testImplementation(libs.spark.sql)
  testImplementation(libs.spark.catalyst)
  testImplementation(libs.scala.library)
  // Required to load GravitinoCatalogManager for the static mock in the write-rejection test.
  testRuntimeOnly(libs.gravitino.client)
}
