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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

public class TestDorisPhysicalSchemaCache {

  private static final Identifier TABLE = Identifier.of(new String[] {"analytics"}, "events");
  private static final Identifier OTHER_TABLE =
      Identifier.of(new String[] {"analytics"}, "other_events");

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
  void reportsHitsAndConditionallyReloadsOnlyTheExpectedSnapshot() {
    DorisPhysicalSchemaCache cache = new DorisPhysicalSchemaCache(60_000, 10);
    AtomicInteger loads = new AtomicInteger();

    DorisPhysicalSchemaCache.Lookup first =
        cache.getWithStatus(TABLE, () -> schema(loads.incrementAndGet()));
    DorisPhysicalSchemaCache.Lookup second =
        cache.getWithStatus(TABLE, () -> schema(loads.incrementAndGet()));
    assertThat(first.cacheHit()).isFalse();
    assertThat(second.cacheHit()).isTrue();
    assertThat(second.schema()).isSameAs(first.schema());
    assertThat(loads).hasValue(1);

    DorisPhysicalSchema unrelatedSnapshot = schema(99);
    assertThat(cache.reloadIfSame(TABLE, unrelatedSnapshot, () -> schema(loads.incrementAndGet())))
        .isSameAs(first.schema());
    assertThat(loads).hasValue(1);

    DorisPhysicalSchema replacement =
        cache.reloadIfSame(TABLE, first.schema(), () -> schema(loads.incrementAndGet()));
    assertThat(replacement).isNotSameAs(first.schema());
    assertThat(replacement.schema().fieldNames()).containsExactly("version_2");
    assertThat(loads).hasValue(2);
    assertThat(cache.getWithStatus(TABLE, () -> schema(loads.incrementAndGet())).cacheHit())
        .isTrue();
    assertThat(loads).hasValue(2);
  }

  @Test
  void coalescedFreshLoadIsNotReportedAsRetainedHit() throws Exception {
    DorisPhysicalSchemaCache cache = new DorisPhysicalSchemaCache(60_000, 10);
    AtomicInteger loads = new AtomicInteger();
    CountDownLatch loaderStarted = new CountDownLatch(1);
    CountDownLatch allowLoaderToFinish = new CountDownLatch(1);
    AtomicReference<Thread> followerThread = new AtomicReference<>();
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<DorisPhysicalSchemaCache.Lookup> leader =
          executor.submit(
              () ->
                  cache.getWithStatus(
                      TABLE,
                      () -> {
                        loads.incrementAndGet();
                        loaderStarted.countDown();
                        await(allowLoaderToFinish);
                        return schema(1);
                      }));
      assertThat(loaderStarted.await(5, TimeUnit.SECONDS)).isTrue();

      Future<DorisPhysicalSchemaCache.Lookup> follower =
          executor.submit(
              () -> {
                followerThread.set(Thread.currentThread());
                return cache.getWithStatus(
                    TABLE,
                    () -> {
                      throw new AssertionError("coalesced follower must not invoke the loader");
                    });
              });
      waitUntilBlocked(followerThread);
      allowLoaderToFinish.countDown();

      assertThat(leader.get().cacheHit()).isFalse();
      assertThat(follower.get().cacheHit()).isFalse();
      assertThat(loads).hasValue(1);
      assertThat(cache.getWithStatus(TABLE, () -> schema(loads.incrementAndGet())).cacheHit())
          .isTrue();
      assertThat(loads).hasValue(1);
    } finally {
      allowLoaderToFinish.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  void testCoalescesAndInvalidatesOnlyTargetSnapshot() throws Exception {
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

      DorisPhysicalSchema other = cache.get(OTHER_TABLE, () -> schema(loads.incrementAndGet()));
      assertThat(loads).hasValue(2);
      cache.invalidate(TABLE);
      assertThat(cache.get(OTHER_TABLE, () -> schema(loads.incrementAndGet()))).isSameAs(other);
      assertThat(loads).hasValue(2);
      assertThat(cache.get(TABLE, () -> schema(loads.incrementAndGet()))).isNotSameAs(expected);
      assertThat(loads).hasValue(3);
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void testReloadsCompleteSnapshotAfterExpiry() throws Exception {
    DorisPhysicalSchemaCache cache = new DorisPhysicalSchemaCache(5, 10);
    AtomicInteger loads = new AtomicInteger();

    DorisPhysicalSchema first = cache.get(TABLE, () -> schema(loads.incrementAndGet()));
    Thread.sleep(25);
    DorisPhysicalSchema second = cache.get(TABLE, () -> schema(loads.incrementAndGet()));

    assertThat(second).isNotSameAs(first);
    assertThat(loads).hasValue(2);
    assertThat(first.schema().fieldNames()).containsExactly("version_1");
    assertThat(first.dorisTypeName(0)).isEqualTo("INT");
    assertThat(second.schema().fieldNames()).containsExactly("version_2");
    assertThat(second.dorisTypeName(0)).isEqualTo("BIGINT");
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

  private static DorisPhysicalSchema schema(int version) {
    return new DorisPhysicalSchema(
        new StructType().add("version_" + version, DataTypes.IntegerType),
        Arrays.asList(version == 1 ? "INT" : "BIGINT"));
  }

  private static void await(CountDownLatch latch) {
    try {
      if (!latch.await(5, TimeUnit.SECONDS)) {
        throw new AssertionError("timed out waiting for the cache test latch");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError("cache test was interrupted", e);
    }
  }

  private static void waitUntilBlocked(AtomicReference<Thread> threadReference) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (System.nanoTime() < deadline) {
      Thread thread = threadReference.get();
      if (thread != null
          && (thread.getState() == Thread.State.BLOCKED
              || thread.getState() == Thread.State.WAITING
              || thread.getState() == Thread.State.TIMED_WAITING)) {
        return;
      }
      Thread.sleep(1);
    }
    assertThat(threadReference.get()).as("coalesced cache follower").isNotNull();
    assertThat(threadReference.get().getState())
        .as("coalesced cache follower state")
        .isIn(Thread.State.BLOCKED, Thread.State.WAITING, Thread.State.TIMED_WAITING);
  }
}
