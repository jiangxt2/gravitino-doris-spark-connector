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

package io.github.jiangxt2.gravitino.doris.server;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.github.jiangxt2.gravitino.doris.security.DorisJdbcSecurity;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.catalog.jdbc.JdbcCatalogPropertiesMetadata;
import org.apache.gravitino.connector.PropertyEntry;

/** Catalog property metadata for native and JDBC Doris read planes. */
public final class GovernedDorisCatalogPropertiesMetadata extends JdbcCatalogPropertiesMetadata {

  private static final Map<String, PropertyEntry<?>> PROPERTIES;

  static {
    List<PropertyEntry<?>> entries =
        ImmutableList.of(
            PropertyEntry.stringImmutablePropertyEntry(
                DorisJdbcSecurity.READ_TRANSPORT,
                "Governed Doris read transport: hybrid or strict-jdbc-tls",
                false,
                DorisJdbcSecurity.HYBRID_TRANSPORT,
                false,
                false),
            optionalString(
                DorisJdbcSecurity.DORIS_FE_NODES,
                "Doris FE HTTP endpoints required only by hybrid native planning"),
            optionalPort(
                DorisJdbcSecurity.DORIS_QUERY_PORT,
                "Doris FE MySQL query port required only by hybrid native planning"),
            optionalString(
                "doris-jdbc-partition-column",
                "Spark JDBC partition column for String-normalized detail scans"),
            optionalString("doris-jdbc-lower-bound", "Spark JDBC partition lower bound"),
            optionalString("doris-jdbc-upper-bound", "Spark JDBC partition upper bound"),
            PropertyEntry.integerOptionalPropertyEntry(
                "doris-jdbc-num-partitions", "Spark JDBC partition count", false, null, false),
            PropertyEntry.integerOptionalPropertyEntry(
                "doris-jdbc-fetch-size", "Spark JDBC fetch size", false, null, false),
            optionalString(
                "doris-schema-cache-ttl-ms",
                "Bounded physical-schema cache TTL in milliseconds; zero disables caching"),
            PropertyEntry.integerOptionalPropertyEntry(
                "doris-schema-cache-max-entries",
                "Maximum number of physical schemas cached per Spark catalog",
                false,
                null,
                false));
    PROPERTIES = Maps.uniqueIndex(entries, PropertyEntry::getName);
  }

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return ImmutableMap.<String, PropertyEntry<?>>builder()
        .putAll(super.specificPropertyEntries())
        .putAll(PROPERTIES)
        .build();
  }

  private static PropertyEntry<String> optionalString(String name, String description) {
    return PropertyEntry.stringOptionalPropertyEntry(name, description, false, null, false);
  }

  private static PropertyEntry<Integer> optionalPort(String name, String description) {
    return new PropertyEntry.Builder<Integer>()
        .withName(name)
        .withDescription(description)
        .withRequired(false)
        .withImmutable(false)
        .withJavaType(Integer.class)
        .withDecoder(value -> decodePort(name, value))
        .withEncoder(String::valueOf)
        .withHidden(false)
        .withReserved(false)
        .build();
  }

  private static int decodePort(String name, String value) {
    try {
      int port = Integer.parseInt(value);
      if (port < 1 || port > 65535) {
        throw new NumberFormatException("port out of range");
      }
      return port;
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(name + " must be between 1 and 65535");
    }
  }
}
