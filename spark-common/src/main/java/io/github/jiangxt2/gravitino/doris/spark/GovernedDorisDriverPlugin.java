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

import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.spark.connector.catalog.GravitinoCatalogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.plugin.DriverPlugin;
import org.apache.spark.api.plugin.PluginContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Composes the official Gravitino driver plugin and registers this project's provider. */
public final class GovernedDorisDriverPlugin implements DriverPlugin {

  private static final Logger LOG = LoggerFactory.getLogger(GovernedDorisDriverPlugin.class);
  private static final String SPARK_CATALOG_PREFIX = "spark.sql.catalog.";

  private final DriverPlugin delegate;

  /** Creates a wrapper around the official Gravitino driver plugin. */
  public GovernedDorisDriverPlugin(DriverPlugin delegate) {
    this.delegate = delegate;
  }

  @Override
  public Map<String, String> init(SparkContext sparkContext, PluginContext pluginContext) {
    // Gravitino 1.3.0 synchronously loads relational catalogs before its driver-plugin init
    // returns. Read that completed snapshot only after the official plugin finishes.
    Map<String, String> extraConf = delegate.init(sparkContext, pluginContext);
    registerGovernedDorisCatalogs(sparkContext.conf(), GravitinoCatalogManager.get().getCatalogs());
    return extraConf;
  }

  @Override
  public void shutdown() {
    delegate.shutdown();
  }

  static void registerGovernedDorisCatalogs(
      SparkConf sparkConf, Map<String, Catalog> gravitinoCatalogs) {
    String className = null;
    for (Map.Entry<String, Catalog> entry : gravitinoCatalogs.entrySet()) {
      if (!DorisConnectorConstants.PROVIDER.equalsIgnoreCase(entry.getValue().provider())) {
        continue;
      }
      if (className == null) {
        className = DorisCatalogClassResolver.resolve();
      }
      String configKey = SPARK_CATALOG_PREFIX + entry.getKey();
      if (sparkConf.contains(configKey)) {
        String configured = sparkConf.get(configKey);
        if (!className.equals(configured)) {
          throw new IllegalArgumentException(
              String.format(
                  "Spark catalog %s is already registered with %s", entry.getKey(), configured));
        }
      } else {
        sparkConf.set(configKey, className);
      }
      LOG.info("Registered governed Doris catalog {}", entry.getKey());
    }
  }
}
