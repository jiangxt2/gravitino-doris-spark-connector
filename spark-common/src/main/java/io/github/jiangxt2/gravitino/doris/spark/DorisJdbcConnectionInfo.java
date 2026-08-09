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

/** Immutable JDBC connection material used by the Doris SQL execution lane. */
public final class DorisJdbcConnectionInfo {

  private final String url;
  private final String driver;
  private final String user;
  private final String password;

  /**
   * Creates JDBC connection material without exposing it through object string rendering.
   *
   * @param url the Doris MySQL-protocol JDBC URL
   * @param driver the JDBC driver class
   * @param user the vended JDBC user
   * @param password the vended JDBC password
   */
  public DorisJdbcConnectionInfo(String url, String driver, String user, String password) {
    this.url = requireNonBlank("jdbc-url", url);
    this.driver = requireNonBlank("jdbc-driver", driver);
    this.user = requireNonBlank("JDBC user", user);
    this.password = requireNonNull("JDBC password", password);
  }

  /** Returns the Doris JDBC URL. */
  public String url() {
    return url;
  }

  /** Returns the JDBC driver class. */
  public String driver() {
    return driver;
  }

  /** Returns the vended JDBC user. */
  public String user() {
    return user;
  }

  /** Returns the vended JDBC password. */
  public String password() {
    return password;
  }

  @Override
  public String toString() {
    return "DorisJdbcConnectionInfo{redacted}";
  }

  private static String requireNonBlank(String name, String value) {
    if (value == null || value.trim().isEmpty()) {
      throw new IllegalArgumentException("Doris Spark adapter requires " + name);
    }
    return value;
  }

  private static String requireNonNull(String name, String value) {
    if (value == null) {
      throw new IllegalArgumentException("Doris Spark adapter requires " + name);
    }
    return value;
  }
}
