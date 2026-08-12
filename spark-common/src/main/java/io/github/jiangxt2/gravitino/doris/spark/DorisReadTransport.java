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

/** Immutable execution and trust-boundary profile for governed Doris reads. */
public enum DorisReadTransport {
  HYBRID,
  STRICT_JDBC_TLS;

  /** Parses the catalog-managed transport property using the compatible default. */
  public static DorisReadTransport from(Map<String, String> properties) {
    String value = DorisJdbcSecurity.readTransport(properties);
    return DorisConnectorConstants.STRICT_JDBC_TLS_TRANSPORT.equals(value)
        ? STRICT_JDBC_TLS
        : HYBRID;
  }

  /** Returns whether this profile can construct the official Doris native lane. */
  public boolean allowsNativeLane() {
    return this == HYBRID;
  }
}
