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

description = "Version-neutral Gravitino governance facade for Doris Spark reads"

dependencies {
  implementation(project(":jdbc-security"))

  compileOnlyApi(libs.gravitino.api)
  compileOnly(libs.gravitino.spark.common)
  compileOnly(libs.gravitino.client)
  compileOnly(libs.spark.core)
  compileOnly(libs.spark.sql)
  compileOnly(libs.spark.catalyst)

  implementation(libs.caffeine)

  testImplementation(libs.gravitino.spark.common)
  testImplementation(libs.gravitino.client)
  testImplementation(libs.spark.core)
  testImplementation(libs.spark.sql)
  testImplementation(libs.spark.catalyst)
}
