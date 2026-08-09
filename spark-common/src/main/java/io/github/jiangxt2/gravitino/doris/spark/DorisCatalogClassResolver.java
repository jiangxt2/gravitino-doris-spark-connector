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

package io.github.jiangxt2.gravitino.doris.spark;

import org.apache.spark.package$;
import org.apache.spark.util.VersionUtils$;
import scala.util.Properties$;

/** Resolves the version-specific catalog without silently falling back to generic JDBC. */
public final class DorisCatalogClassResolver {

  static final String SPARK_35_CATALOG_CLASS =
      "io.github.jiangxt2.gravitino.doris.spark.GovernedDorisCatalogSpark35";

  private DorisCatalogClassResolver() {}

  /** Returns the supported catalog class or fails with an actionable compatibility message. */
  public static String resolve() {
    return resolve(package$.MODULE$.SPARK_VERSION(), Properties$.MODULE$.versionNumberString());
  }

  static String resolve(String sparkVersion, String scalaVersion) {
    int major = VersionUtils$.MODULE$.majorVersion(sparkVersion);
    int minor = VersionUtils$.MODULE$.minorVersion(sparkVersion);
    if (major == 3 && minor == 5 && scalaVersion.startsWith("2.12.")) {
      return SPARK_35_CATALOG_CLASS;
    }
    throw new IllegalStateException(
        String.format(
            "The governed Doris connector supports Spark 3.5 with Scala 2.12; found Spark %s "
                + "with Scala %s",
            sparkVersion, scalaVersion));
  }
}
