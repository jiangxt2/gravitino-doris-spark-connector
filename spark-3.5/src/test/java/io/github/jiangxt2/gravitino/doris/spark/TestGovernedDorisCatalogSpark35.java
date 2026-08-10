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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

import java.util.Set;
import org.apache.gravitino.spark.connector.catalog.GravitinoCatalogManager;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/** Tests Spark 3.5 patch compatibility behavior of the governed Doris catalog. */
public class TestGovernedDorisCatalogSpark35 {

  @Test
  void testWriteAwareLoadIsRejectedBeforeAdapterInitialization() {
    GravitinoCatalogManager manager = mock(GravitinoCatalogManager.class);

    try (MockedStatic<GravitinoCatalogManager> managers =
        mockStatic(GravitinoCatalogManager.class)) {
      managers.when(GravitinoCatalogManager::get).thenReturn(manager);
      GovernedDorisCatalogSpark35 catalog = new GovernedDorisCatalogSpark35();

      assertThatThrownBy(
              () ->
                  catalog.loadTable(Identifier.of(new String[] {"analytics"}, "events"), Set.of()))
          .isInstanceOf(UnsupportedOperationException.class)
          .hasMessageContaining("read-only")
          .hasMessageContaining("table writes");
    }
  }
}
