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

import io.github.jiangxt2.gravitino.doris.security.DorisJdbcSecurity;
import java.util.Map;

/** Immutable, fail-closed policy for the governed Doris batch-write surface. */
public final class DorisWritePolicy {

  private static final DorisWritePolicy DISABLED =
      new DorisWritePolicy(
          DorisConnectorConstants.WRITE_DISABLED, DorisConnectorConstants.WRITE_OVERWRITE_REJECT);

  private final String mode;
  private final String overwriteMode;

  private DorisWritePolicy(String mode, String overwriteMode) {
    this.mode = mode;
    this.overwriteMode = overwriteMode;
  }

  /** Parses an already server-validated catalog property map and revalidates its write contract. */
  public static DorisWritePolicy from(Map<String, String> properties) {
    String mode = DorisJdbcSecurity.writeMode(properties);
    String overwriteMode = DorisJdbcSecurity.writeOverwriteMode(properties);
    if (DorisConnectorConstants.WRITE_DISABLED.equals(mode)) {
      if (!DorisConnectorConstants.WRITE_OVERWRITE_REJECT.equals(overwriteMode)) {
        throw new IllegalArgumentException(
            "doris-write-overwrite-mode=truncate requires doris-write-mode=batch");
      }
      return DISABLED;
    }
    if (DorisConnectorConstants.STRICT_JDBC_TLS_TRANSPORT.equals(
        DorisJdbcSecurity.readTransport(properties))) {
      throw new IllegalArgumentException(
          "Strict JDBC TLS transport does not support governed Doris writes");
    }
    return new DorisWritePolicy(mode, overwriteMode);
  }

  /** Returns the stable disabled policy. */
  public static DorisWritePolicy disabled() {
    return DISABLED;
  }

  /** Returns whether governed batch writes are requested. */
  public boolean enabled() {
    return DorisConnectorConstants.WRITE_BATCH.equals(mode);
  }

  /** Returns whether non-atomic truncate-then-load compatibility is explicitly enabled. */
  public boolean allowsTruncate() {
    return enabled() && DorisConnectorConstants.WRITE_OVERWRITE_TRUNCATE.equals(overwriteMode);
  }

  /** Returns connector options that must override every user-controlled source. */
  public Map<String, String> forcedConnectorOptions() {
    if (!enabled()) {
      return Map.of();
    }
    return Map.of(
        DorisConnectorConstants.DORIS_SINK_MODE,
        "stream_load",
        DorisConnectorConstants.DORIS_SINK_AUTO_REDIRECT,
        "false",
        DorisConnectorConstants.DORIS_SINK_ENABLE_2PC,
        "true",
        DorisConnectorConstants.DORIS_SINK_STRICT_MODE,
        "true",
        DorisConnectorConstants.DORIS_MAX_FILTER_RATIO,
        "0",
        DorisConnectorConstants.DORIS_WRITE_SCHEMALESS,
        "false");
  }
}
