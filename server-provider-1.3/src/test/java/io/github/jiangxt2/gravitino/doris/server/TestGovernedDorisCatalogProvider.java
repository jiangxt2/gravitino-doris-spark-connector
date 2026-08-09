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

import java.util.ServiceLoader;
import org.apache.gravitino.CatalogProvider;
import org.junit.jupiter.api.Test;

public class TestGovernedDorisCatalogProvider {

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
}
