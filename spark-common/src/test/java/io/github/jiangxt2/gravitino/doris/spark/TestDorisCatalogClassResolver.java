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

public class TestDorisCatalogClassResolver {

  @Test
  void resolvesOnlySpark35Scala212() {
    assertThat(DorisCatalogClassResolver.resolve("3.5.8", "2.12.20"))
        .isEqualTo(DorisCatalogClassResolver.SPARK_35_CATALOG_CLASS);
    assertThatThrownBy(() -> DorisCatalogClassResolver.resolve("3.4.4", "2.12.20"))
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> DorisCatalogClassResolver.resolve("3.5.8", "2.13.16"))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void gatesTheWriteAwareApiAtSpark353WithoutClassProbing() {
    assertThat(DorisCatalogClassResolver.supportsWriteAwareLoad("3.5.0")).isFalse();
    assertThat(DorisCatalogClassResolver.supportsWriteAwareLoad("3.5.2-amzn-1")).isFalse();
    assertThat(DorisCatalogClassResolver.supportsWriteAwareLoad("3.5.3")).isTrue();
    assertThat(DorisCatalogClassResolver.supportsWriteAwareLoad("3.5.9+vendor")).isTrue();
    assertThat(DorisCatalogClassResolver.supportsWriteAwareLoad("3.5")).isFalse();
    assertThat(DorisCatalogClassResolver.supportsWriteAwareLoad("unknown")).isFalse();
  }
}
