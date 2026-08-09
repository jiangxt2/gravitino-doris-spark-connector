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

import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownOffset;

/** Spark 3.5 Doris scan wrapper that additionally exposes JDBC offset pushdown. */
public class GovernedDorisScanBuilder35 extends GovernedDorisScanBuilder
    implements SupportsPushDownOffset {

  private final SupportsPushDownOffset offsetDelegate;

  /**
   * Creates a governed Spark 3.5 Doris scan builder.
   *
   * @param delegate the version-specific hybrid scan builder
   */
  public GovernedDorisScanBuilder35(ScanBuilder delegate) {
    super(delegate);
    DorisChecks.checkArgument(
        delegate instanceof SupportsPushDownOffset,
        "Doris scan builder does not support offset pushdown");
    this.offsetDelegate = (SupportsPushDownOffset) delegate;
  }

  @Override
  public boolean pushOffset(int offset) {
    return offsetDelegate.pushOffset(offset);
  }
}
