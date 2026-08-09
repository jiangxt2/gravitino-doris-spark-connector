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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

public class TestDorisPhysicalSchemaCache {

  private static final Identifier TABLE = Identifier.of(new String[] {"analytics"}, "events");

  @Test
  void cachesOnceAndRefreshInvalidatesExactlyOneTable() {
    DorisPhysicalSchemaCache cache = new DorisPhysicalSchemaCache(60_000, 10);
    AtomicInteger loads = new AtomicInteger();

    DorisPhysicalSchema first = cache.get(TABLE, () -> schema(loads.incrementAndGet()));
    DorisPhysicalSchema second = cache.get(TABLE, () -> schema(loads.incrementAndGet()));
    assertThat(second).isSameAs(first);
    assertThat(loads).hasValue(1);

    cache.invalidate(TABLE);
    assertThat(cache.get(TABLE, () -> schema(loads.incrementAndGet()))).isNotSameAs(first);
    assertThat(loads).hasValue(2);
  }

  @Test
  void coalescesConcurrentLoads() throws Exception {
    DorisPhysicalSchemaCache cache = new DorisPhysicalSchemaCache(60_000, 10);
    AtomicInteger loads = new AtomicInteger();
    ExecutorService executor = Executors.newFixedThreadPool(8);
    try {
      Callable<DorisPhysicalSchema> task =
          () -> cache.get(TABLE, () -> schema(loads.incrementAndGet()));
      List<Future<DorisPhysicalSchema>> futures = new ArrayList<>();
      for (int index = 0; index < 8; index++) {
        futures.add(executor.submit(task));
      }
      DorisPhysicalSchema expected = futures.get(0).get();
      for (Future<DorisPhysicalSchema> future : futures) {
        assertThat(future.get()).isSameAs(expected);
      }
      assertThat(loads).hasValue(1);
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void disabledCacheAlwaysLoadsAndConfigurationFailsClosed() {
    DorisPhysicalSchemaCache cache = new DorisPhysicalSchemaCache(0, 1);
    AtomicInteger loads = new AtomicInteger();
    cache.get(TABLE, () -> schema(loads.incrementAndGet()));
    cache.get(TABLE, () -> schema(loads.incrementAndGet()));
    assertThat(loads).hasValue(2);

    assertThatThrownBy(() -> new DorisPhysicalSchemaCache(-1, 1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new DorisPhysicalSchemaCache(1, 0))
        .isInstanceOf(IllegalArgumentException.class);
  }

  private static DorisPhysicalSchema schema(int ignoredVersion) {
    return DorisPhysicalSchema.withoutTypeNames(new StructType());
  }
}
