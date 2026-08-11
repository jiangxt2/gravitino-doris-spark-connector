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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

public class TestDorisJdbcConnectionInfo {

  private static final String DRIVER = "com.mysql.cj.jdbc.Driver";

  @Test
  void retainsValidatedMaterialButAlwaysRedactsStringRendering() {
    DorisJdbcConnectionInfo connection =
        new DorisJdbcConnectionInfo(
            "jdbc:mysql://fe:9030/analytics", DRIVER, "reader", "connection-secret-canary");

    assertThat(connection.url()).isEqualTo("jdbc:mysql://fe:9030/analytics");
    assertThat(connection.driver()).isEqualTo(DRIVER);
    assertThat(connection.user()).isEqualTo("reader");
    assertThat(connection.password()).isEqualTo("connection-secret-canary");
    assertThat(connection.toString())
        .isEqualTo("DorisJdbcConnectionInfo{redacted}")
        .doesNotContain("reader", "connection-secret-canary", "fe:9030");
  }

  @Test
  void rejectsUnsafeUrlWithoutEchoingConnectionMaterial() {
    String unsafeUrl = "jdbc:mysql://reader:connection-secret-canary@private-fe:9030/";

    assertThatThrownBy(
            () ->
                new DorisJdbcConnectionInfo(
                    unsafeUrl, DRIVER, "reader", "connection-secret-canary"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageNotContaining(unsafeUrl)
        .hasMessageNotContaining("reader")
        .hasMessageNotContaining("connection-secret-canary")
        .hasMessageNotContaining("private-fe");
  }

  @Test
  void rejectsInvalidCredentialsWithoutEchoingTheirValues() {
    assertThatThrownBy(
            () ->
                new DorisJdbcConnectionInfo(
                    "jdbc:mysql://fe:9030/", DRIVER, " ", "connection-secret-canary"))
        .hasMessageContaining("JDBC user")
        .hasMessageNotContaining("connection-secret-canary");
    assertThatThrownBy(
            () -> new DorisJdbcConnectionInfo("jdbc:mysql://fe:9030/", DRIVER, "reader", null))
        .hasMessageContaining("JDBC password")
        .hasMessageNotContaining("reader");
  }
}
