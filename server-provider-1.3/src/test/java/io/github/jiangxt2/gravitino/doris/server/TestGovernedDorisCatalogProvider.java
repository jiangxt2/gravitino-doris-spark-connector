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

package io.github.jiangxt2.gravitino.doris.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.gravitino.CatalogProvider;
import org.junit.jupiter.api.Test;

public class TestGovernedDorisCatalogProvider {

  private static final String DRIVER = "com.mysql.cj.jdbc.Driver";

  @Test
  void hasUniqueServiceLoadedShortNameAndRequiredEndpoint() {
    assertThat(
            ServiceLoader.load(CatalogProvider.class).stream()
                .map(ServiceLoader.Provider::get)
                .filter(provider -> "doris-governed".equals(provider.shortName())))
        .hasSize(1)
        .first()
        .isInstanceOf(GovernedDorisCatalogProvider.class);

    GovernedDorisCatalogProvider provider = new GovernedDorisCatalogProvider();
    assertThat(provider.catalogPropertiesMetadata().getPropertyEntry("doris-fenodes").isRequired())
        .isTrue();
    org.apache.gravitino.connector.PropertyEntry<?> queryPort =
        provider.catalogPropertiesMetadata().getPropertyEntry("doris-query-port");
    assertThat(queryPort.isRequired()).isTrue();
    assertThat(queryPort.decode("9030")).isEqualTo(9030);
    assertThatThrownBy(() -> queryPort.decode("0"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("1 and 65535");
    assertThatThrownBy(() -> queryPort.decode("65536"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("1 and 65535");
    assertThat(provider.catalogPropertiesMetadata().getPropertyEntry("jdbc-password").isHidden())
        .isTrue();
    assertThat(provider.catalogPropertiesMetadata().containsProperty("doris-jdbc-num-partitions"))
        .isTrue();
  }

  @Test
  void validatesRawCatalogConfigurationBeforeJdbcInitialization() {
    GovernedDorisCatalogProvider provider = new GovernedDorisCatalogProvider();
    Map<String, String> unsafe = validProperties();
    unsafe.put("gravitino.bypass.connectionFactoryClassName", "example.SecretCanary");
    assertThatThrownBy(() -> provider.withCatalogConf(unsafe))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("connectionFactoryClassName")
        .hasMessageNotContaining("SecretCanary");
  }

  @Test
  void validCatalogRequiresAnExternalDriverWithoutEchoingConfiguration() {
    // The unit runtime deliberately excludes Connector/J. The driver-present provider path is
    // covered by GovernedDorisConnectorIT with an externally mounted test Driver.
    GovernedDorisCatalogProvider provider = new GovernedDorisCatalogProvider();

    assertThatThrownBy(() -> provider.withCatalogConf(validProperties()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("com.mysql:mysql-connector-j:8.0.33")
        .hasMessageContaining("$GRAVITINO_HOME/catalogs/doris-governed/libs")
        .hasMessageNotContaining("provider-secret-canary")
        .hasMessageNotContaining("jdbc:mysql://fe:9030/");
  }

  @Test
  void rejectsMalformedConnectionPropertiesWithoutEchoingValues() {
    GovernedDorisCatalogProvider provider = new GovernedDorisCatalogProvider();
    Map<String, String> properties = validProperties();
    String malformed = "bad\\" + "u12G4=provider-secret-canary";
    properties.put("gravitino.bypass.connectionProperties", malformed);

    assertThatThrownBy(() -> provider.withCatalogConf(properties))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unable to validate JDBC connectionProperties")
        .hasMessageNotContaining("provider-secret-canary")
        .hasMessageNotContaining(malformed);
  }

  private static Map<String, String> validProperties() {
    Map<String, String> properties = new HashMap<>();
    properties.put("jdbc-url", "jdbc:mysql://fe:9030/");
    properties.put("jdbc-driver", DRIVER);
    properties.put("jdbc-user", "reader");
    properties.put("jdbc-password", "provider-secret-canary");
    return properties;
  }
}
