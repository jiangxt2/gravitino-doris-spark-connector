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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.spark.connector.catalog.GravitinoCatalogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.plugin.DriverPlugin;
import org.apache.spark.api.plugin.PluginContext;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

public class TestGovernedDorisPluginRegistration {

  @Test
  void initializesOfficialPluginBeforeReadingItsSynchronousCatalogSnapshot() {
    DriverPlugin delegate = mock(DriverPlugin.class);
    SparkContext sparkContext = mock(SparkContext.class);
    PluginContext pluginContext = mock(PluginContext.class);
    GravitinoCatalogManager catalogManager = mock(GravitinoCatalogManager.class);
    Catalog governed = mock(Catalog.class);
    SparkConf conf = new SparkConf(false);
    Map<String, String> extraConf = ImmutableMap.of("delegate.setting", "value");
    AtomicBoolean delegateInitialized = new AtomicBoolean();

    when(sparkContext.conf()).thenReturn(conf);
    when(delegate.init(sparkContext, pluginContext))
        .thenAnswer(
            ignored -> {
              delegateInitialized.set(true);
              return extraConf;
            });
    when(governed.provider()).thenReturn(DorisConnectorConstants.PROVIDER);
    when(catalogManager.getCatalogs()).thenReturn(ImmutableMap.of("governed", governed));

    try (MockedStatic<GravitinoCatalogManager> managers =
        mockStatic(GravitinoCatalogManager.class)) {
      managers
          .when(GravitinoCatalogManager::get)
          .thenAnswer(
              ignored -> {
                assertThat(delegateInitialized).isTrue();
                return catalogManager;
              });

      assertThat(new GovernedDorisDriverPlugin(delegate).init(sparkContext, pluginContext))
          .isSameAs(extraConf);
    }

    assertThat(conf.get("spark.sql.catalog.governed"))
        .isEqualTo(DorisCatalogClassResolver.SPARK_35_CATALOG_CLASS);
  }

  @Test
  void registersOnlyTheIndependentProvider() {
    Catalog governed = mock(Catalog.class);
    Catalog jdbc = mock(Catalog.class);
    when(governed.provider()).thenReturn(DorisConnectorConstants.PROVIDER);
    when(jdbc.provider()).thenReturn("jdbc-doris");
    SparkConf conf = new SparkConf(false);

    GovernedDorisDriverPlugin.registerGovernedDorisCatalogs(
        conf, ImmutableMap.of("governed", governed, "legacy", jdbc));

    assertThat(conf.get("spark.sql.catalog.governed"))
        .isEqualTo(DorisCatalogClassResolver.SPARK_35_CATALOG_CLASS);
    assertThat(conf.contains("spark.sql.catalog.legacy")).isFalse();
  }

  @Test
  void refusesToOverwriteAnExistingDifferentCatalog() {
    Catalog governed = mock(Catalog.class);
    when(governed.provider()).thenReturn(DorisConnectorConstants.PROVIDER);
    SparkConf conf =
        new SparkConf(false).set("spark.sql.catalog.governed", "example.DifferentCatalog");

    assertThatThrownBy(
            () ->
                GovernedDorisDriverPlugin.registerGovernedDorisCatalogs(
                    conf, ImmutableMap.of("governed", governed)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("already registered");
  }

  @Test
  void acceptsSupportedSparkPatchesAndRejectsUnsupportedRuntimeWithoutJdbcFallback() {
    assertThat(DorisCatalogClassResolver.resolve("3.5.0", "2.12.18"))
        .isEqualTo(DorisCatalogClassResolver.SPARK_35_CATALOG_CLASS);
    assertThat(DorisCatalogClassResolver.resolve("3.5.8", "2.12.18"))
        .isEqualTo(DorisCatalogClassResolver.SPARK_35_CATALOG_CLASS);
    assertThat(DorisCatalogClassResolver.resolve("3.5.9", "2.12.18"))
        .isEqualTo(DorisCatalogClassResolver.SPARK_35_CATALOG_CLASS);
    assertThatThrownBy(() -> DorisCatalogClassResolver.resolve("3.4.4", "2.12.18"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Spark 3.5");
    assertThatThrownBy(() -> DorisCatalogClassResolver.resolve("4.0.0", "2.12.18"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Spark 3.5");
    assertThatThrownBy(() -> DorisCatalogClassResolver.resolve("3.5.8", "2.13.14"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Scala 2.12");
  }
}
