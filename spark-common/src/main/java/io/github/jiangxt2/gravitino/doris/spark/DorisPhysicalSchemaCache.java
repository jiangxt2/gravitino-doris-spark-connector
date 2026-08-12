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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.spark.sql.connector.catalog.Identifier;

/** Bounded per-catalog cache for physical Doris schema snapshots. */
public final class DorisPhysicalSchemaCache {

  private final Cache<String, DorisPhysicalSchema> cache;
  private final boolean enabled;

  /** Creates a cache from visible catalog properties. */
  public static DorisPhysicalSchemaCache from(Map<String, String> properties) {
    long ttl =
        parseNonNegativeLong(
            properties,
            DorisConnectorConstants.SCHEMA_CACHE_TTL_MS,
            DorisConnectorConstants.DEFAULT_SCHEMA_CACHE_TTL_MS);
    int maxEntries =
        parsePositiveInt(
            properties,
            DorisConnectorConstants.SCHEMA_CACHE_MAX_ENTRIES,
            DorisConnectorConstants.DEFAULT_SCHEMA_CACHE_MAX_ENTRIES);
    return new DorisPhysicalSchemaCache(ttl, maxEntries);
  }

  /** Creates a cache with the supplied TTL and capacity. */
  public DorisPhysicalSchemaCache(long ttlMs, int maxEntries) {
    if (ttlMs < 0) {
      throw new IllegalArgumentException("Schema cache TTL must not be negative");
    }
    if (maxEntries < 1) {
      throw new IllegalArgumentException("Schema cache capacity must be positive");
    }
    enabled = ttlMs > 0;
    Caffeine<Object, Object> builder = Caffeine.newBuilder().maximumSize(maxEntries);
    if (enabled) {
      builder.expireAfterWrite(Duration.ofMillis(ttlMs));
    }
    cache = builder.build();
  }

  /** Returns one schema, coalescing concurrent loads for the same identifier. */
  public DorisPhysicalSchema get(Identifier identifier, Supplier<DorisPhysicalSchema> loader) {
    return getWithStatus(identifier, loader).schema();
  }

  /** Returns one schema and whether a retained entry existed before this lookup. */
  Lookup getWithStatus(Identifier identifier, Supplier<DorisPhysicalSchema> loader) {
    Objects.requireNonNull(identifier, "identifier");
    Objects.requireNonNull(loader, "loader");
    if (!enabled) {
      return new Lookup(load(loader), false);
    }
    String key = key(identifier);
    DorisPhysicalSchema retained = cache.getIfPresent(key);
    if (retained != null) {
      return new Lookup(retained, true);
    }
    // A concurrent caller may complete or join this load. Either outcome still belongs to the
    // current miss generation and must not be eligible for stale-entry revalidation.
    return new Lookup(cache.get(key, ignored -> load(loader)), false);
  }

  /**
   * Removes the expected snapshot, if it is still current, and returns one coalesced replacement.
   */
  DorisPhysicalSchema reloadIfSame(
      Identifier identifier, DorisPhysicalSchema expected, Supplier<DorisPhysicalSchema> loader) {
    Objects.requireNonNull(identifier, "identifier");
    Objects.requireNonNull(expected, "expected");
    Objects.requireNonNull(loader, "loader");
    if (!enabled) {
      return load(loader);
    }
    String key = key(identifier);
    cache.asMap().remove(key, expected);
    return cache.get(key, ignored -> load(loader));
  }

  /** Invalidates one table cache entry. */
  public void invalidate(Identifier identifier) {
    cache.invalidate(key(identifier));
  }

  /** Returns the number of live cache entries for diagnostics and tests. */
  public long estimatedSize() {
    return cache.estimatedSize();
  }

  private static String key(Identifier identifier) {
    return String.join("\u0000", identifier.namespace()) + "\u0000" + identifier.name();
  }

  private static DorisPhysicalSchema load(Supplier<DorisPhysicalSchema> loader) {
    return Objects.requireNonNull(loader.get(), "physical schema loader returned null");
  }

  static final class Lookup {
    private final DorisPhysicalSchema schema;
    private final boolean cacheHit;

    private Lookup(DorisPhysicalSchema schema, boolean cacheHit) {
      this.schema = schema;
      this.cacheHit = cacheHit;
    }

    DorisPhysicalSchema schema() {
      return schema;
    }

    boolean cacheHit() {
      return cacheHit;
    }
  }

  private static long parseNonNegativeLong(
      Map<String, String> properties, String key, long defaultValue) {
    String value = properties == null ? null : properties.get(key);
    if (value == null) {
      return defaultValue;
    }
    try {
      long parsed = Long.parseLong(value.trim());
      if (parsed < 0) {
        throw new NumberFormatException("negative");
      }
      return parsed;
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(key + " must be a non-negative integer");
    }
  }

  private static int parsePositiveInt(
      Map<String, String> properties, String key, int defaultValue) {
    String value = properties == null ? null : properties.get(key);
    if (value == null) {
      return defaultValue;
    }
    try {
      int parsed = Integer.parseInt(value.trim());
      if (parsed < 1) {
        throw new NumberFormatException("not positive");
      }
      return parsed;
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(key + " must be a positive integer");
    }
  }
}
