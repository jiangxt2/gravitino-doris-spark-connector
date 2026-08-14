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

import java.util.Set;
import org.apache.spark.sql.connector.catalog.TableCapability;

/** Central capability gate that prevents delegate write capabilities from leaking to Spark. */
public final class DorisCapabilityPolicy {

  private static final Set<TableCapability> READ_CAPABILITIES = Set.of(TableCapability.BATCH_READ);
  private static final DorisCapabilityPolicy READ_ONLY = new DorisCapabilityPolicy();

  private final Set<TableCapability> tableCapabilities;

  private DorisCapabilityPolicy() {
    this(READ_CAPABILITIES);
  }

  private DorisCapabilityPolicy(Set<TableCapability> tableCapabilities) {
    this.tableCapabilities = Set.copyOf(tableCapabilities);
  }

  /** Returns the initial read-only policy. */
  public static DorisCapabilityPolicy readOnly() {
    return READ_ONLY;
  }

  /** Returns a policy for a future facade after its read/write interfaces are implemented. */
  public static DorisCapabilityPolicy of(Set<TableCapability> tableCapabilities) {
    if (!tableCapabilities.contains(TableCapability.BATCH_READ)) {
      throw new IllegalArgumentException("A governed Doris table must retain BATCH_READ");
    }
    return new DorisCapabilityPolicy(tableCapabilities);
  }

  /** Returns the capabilities certified by the supplied write policy and Spark patch. */
  public static DorisCapabilityPolicy from(DorisWritePolicy writePolicy) {
    return from(writePolicy, DorisCatalogClassResolver.supportsWriteAwareLoad());
  }

  static DorisCapabilityPolicy from(DorisWritePolicy writePolicy, boolean writeAwareLoadSupported) {
    if (!writePolicy.enabled() || !writeAwareLoadSupported) {
      return readOnly();
    }
    java.util.EnumSet<TableCapability> capabilities =
        java.util.EnumSet.of(TableCapability.BATCH_READ, TableCapability.BATCH_WRITE);
    if (writePolicy.allowsTruncate()) {
      capabilities.add(TableCapability.TRUNCATE);
    }
    return of(capabilities);
  }

  /** Returns capabilities that may be exposed by the governed facade. */
  public Set<TableCapability> tableCapabilities() {
    return tableCapabilities;
  }

  /** Returns whether the policy enables construction of a batch or streaming write builder. */
  public boolean allowsTableWrites() {
    return tableCapabilities.contains(TableCapability.BATCH_WRITE)
        || tableCapabilities.contains(TableCapability.STREAMING_WRITE);
  }

  /** Creates a consistent unsupported-operation error for future mutation seams. */
  public UnsupportedOperationException reject(String operation) {
    return new UnsupportedOperationException(
        "The governed Doris connector policy does not support "
            + operation
            + "; the requested operation remains read-only");
  }
}
