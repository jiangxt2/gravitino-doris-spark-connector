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

import org.apache.gravitino.spark.connector.plugin.GravitinoSparkPlugin;
import org.apache.spark.api.plugin.DriverPlugin;
import org.apache.spark.api.plugin.ExecutorPlugin;
import org.apache.spark.api.plugin.SparkPlugin;

/** Spark plugin entry point that preserves official Gravitino behavior and adds Doris support. */
public final class GovernedDorisSparkPlugin implements SparkPlugin {

  private final GravitinoSparkPlugin delegate = new GravitinoSparkPlugin();

  @Override
  public DriverPlugin driverPlugin() {
    return new GovernedDorisDriverPlugin(delegate.driverPlugin());
  }

  @Override
  public ExecutorPlugin executorPlugin() {
    return delegate.executorPlugin();
  }
}
