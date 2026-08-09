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

import org.apache.spark.sql.connector.catalog.Table;

/** Stable factory seam for a future governed batch or streaming write delegate. */
@FunctionalInterface
public interface DorisWriteDelegateFactory {

  /**
   * Decorates an authorized read delegate with future governed write behavior.
   *
   * <p>The initial factory returns the read delegate unchanged. A future implementation can use the
   * original official Doris table and credential-vended connection material in the context without
   * changing catalog registration, authorization ordering, or table construction.
   */
  Table create(DorisAuthorizedTableContext context);

  /** Returns the initial factory that deliberately exposes no write implementation. */
  static DorisWriteDelegateFactory readOnly() {
    return DorisAuthorizedTableContext::readDelegate;
  }
}
