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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.doris.spark.client.entity.Backend;
import org.apache.doris.spark.client.entity.DorisReaderPartition;
import org.apache.doris.spark.client.read.DorisReader;
import org.apache.doris.spark.config.DorisConfig;
import org.apache.doris.spark.exception.DorisException;
import org.apache.doris.spark.read.DorisInputPartition;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class TestDorisArrowFallbackPartitionReader35 {

  private static final String ENDPOINT_IDENTITY = "endpoint-identity";

  @AfterEach
  void clearCircuit() {
    DorisArrowFallbackCircuitBreaker35.clearForTests();
  }

  @Test
  void deliversArrowRowsThenExhaustsWithoutFallback() throws Exception {
    FakeReader arrow = new FakeReader(readerPartition(), new Object[][] {{"arrow-row"}});
    AtomicInteger thriftCreations = new AtomicInteger();
    DorisArrowFallbackPartitionReader35 reader =
        reader(
            ignored -> arrow,
            ignored -> {
              thriftCreations.incrementAndGet();
              return new FakeReader(readerPartition(), new Object[0][]);
            },
            (hosts, port) -> {});

    assertThat(reader.next()).isTrue();
    assertThat(reader.stateForTests())
        .isEqualTo(DorisArrowFallbackPartitionReader35.State.ARROW_ROW_READY_NOT_DELIVERED);
    InternalRow row = reader.get();
    assertThat(row.getUTF8String(0).toString()).isEqualTo("arrow-row");
    assertThat(reader.stateForTests())
        .isEqualTo(DorisArrowFallbackPartitionReader35.State.ARROW_DELIVERED);
    assertThat(reader.next()).isFalse();
    assertThat(reader.next()).isFalse();
    assertThat(reader.stateForTests())
        .isEqualTo(DorisArrowFallbackPartitionReader35.State.ARROW_EXHAUSTED);
    assertThat(thriftCreations).hasValue(0);

    reader.close();
    reader.close();
    assertThat(arrow.closeCalls).isEqualTo(1);
    assertThat(reader.stateForTests()).isEqualTo(DorisArrowFallbackPartitionReader35.State.CLOSED);
  }

  @Test
  void successfulEmptyArrowResultIsExhaustedAndNeverFallsBack() throws Exception {
    FakeReader arrow = new FakeReader(readerPartition(), new Object[0][]);
    AtomicInteger thriftCreations = new AtomicInteger();
    DorisArrowFallbackPartitionReader35 reader =
        reader(
            ignored -> arrow,
            ignored -> {
              thriftCreations.incrementAndGet();
              return new FakeReader(readerPartition(), new Object[0][]);
            },
            (hosts, port) -> {});

    assertThat(reader.next()).isFalse();
    assertThat(reader.next()).isFalse();
    assertThat(reader.stateForTests())
        .isEqualTo(DorisArrowFallbackPartitionReader35.State.ARROW_EXHAUSTED);
    assertThat(thriftCreations).hasValue(0);
  }

  @Test
  void probeOrConstructorTransportFailureOpensCircuitAndUsesThrift() throws Exception {
    AtomicInteger arrowCreations = new AtomicInteger();
    FakeReader thrift = new FakeReader(readerPartition(), new Object[][] {{"thrift-row"}});
    DorisArrowFallbackPartitionReader35 probeFailure =
        reader(
            ignored -> {
              arrowCreations.incrementAndGet();
              return new FakeReader(readerPartition(), new Object[0][]);
            },
            ignored -> thrift,
            (hosts, port) -> {
              throw new ConnectException("redacted");
            });

    assertThat(probeFailure.next()).isTrue();
    assertThat(probeFailure.get().getUTF8String(0).toString()).isEqualTo("thrift-row");
    assertThat(arrowCreations).hasValue(0);
    assertThat(probeFailure.stateForTests())
        .isEqualTo(DorisArrowFallbackPartitionReader35.State.THRIFT_FALLBACK);

    DorisArrowFallbackCircuitBreaker35.clearForTests();
    AtomicInteger probes = new AtomicInteger();
    AtomicInteger constructors = new AtomicInteger();
    DorisArrowFallbackPartitionReader35 constructorFailure =
        reader(
            ignored -> {
              constructors.incrementAndGet();
              throw new DorisException(new ConnectException("redacted"));
            },
            ignored -> new FakeReader(readerPartition(), new Object[][] {{"fallback"}}),
            (hosts, port) -> probes.incrementAndGet());
    assertThat(constructorFailure.next()).isTrue();
    assertThat(constructorFailure.get().getUTF8String(0).toString()).isEqualTo("fallback");
    assertThat(probes).hasValue(1);
    assertThat(constructors).hasValue(1);

    DorisArrowFallbackPartitionReader35 sameApplicationPartition =
        reader(
            ignored -> {
              constructors.incrementAndGet();
              return new FakeReader(readerPartition(), new Object[0][]);
            },
            ignored -> new FakeReader(readerPartition(), new Object[][] {{"circuit"}}),
            (hosts, port) -> probes.incrementAndGet());
    assertThat(sameApplicationPartition.next()).isTrue();
    assertThat(sameApplicationPartition.get().getUTF8String(0).toString()).isEqualTo("circuit");
    assertThat(probes).hasValue(1);
    assertThat(constructors).hasValue(1);
  }

  @Test
  void firstGetTransportFailureMayFallbackButConversionFailureMayNot() throws Exception {
    FakeReader failingArrow =
        new FakeReader(readerPartition(), new Object[][] {{"unused-1"}, {"unused-2"}});
    failingArrow.failNextAfterAdvance = true;
    AtomicInteger thriftCreations = new AtomicInteger();
    DorisArrowFallbackPartitionReader35 reader =
        reader(
            ignored -> failingArrow,
            ignored -> {
              thriftCreations.incrementAndGet();
              return new FakeReader(
                  readerPartition(), new Object[][] {{"replacement-1"}, {"replacement-2"}});
            },
            (hosts, port) -> {});
    assertThat(reader.next()).isTrue();
    assertThat(reader.get().getUTF8String(0).toString()).isEqualTo("replacement-1");
    assertThat(reader.next()).isTrue();
    assertThat(reader.get().getUTF8String(0).toString()).isEqualTo("replacement-2");
    assertThat(reader.next()).isFalse();
    assertThat(thriftCreations).hasValue(1);
    assertThat(failingArrow.position).isEqualTo(1);

    DorisArrowFallbackCircuitBreaker35.clearForTests();
    AtomicInteger conversionThriftCreations = new AtomicInteger();
    DorisArrowFallbackPartitionReader35 invalidRow =
        reader(
            ignored -> new FakeReader(readerPartition(), new Object[][] {new Object[0]}),
            ignored -> {
              conversionThriftCreations.incrementAndGet();
              return new FakeReader(readerPartition(), new Object[][] {{"wrong"}});
            },
            (hosts, port) -> {});
    assertThat(invalidRow.next()).isTrue();
    assertThatThrownBy(invalidRow::get)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("column counts");
    assertThat(conversionThriftCreations).hasValue(0);
    assertThat(DorisArrowFallbackCircuitBreaker35.isOpen(ENDPOINT_IDENTITY)).isFalse();
  }

  @Test
  void boundsTheCatalogManagedEndpointProbeByPerHostAndTotalTimeouts() {
    List<String> attempts = new ArrayList<>();
    List<Integer> timeouts = new ArrayList<>();
    AtomicLong elapsedNanos = new AtomicLong();

    assertThatThrownBy(
            () ->
                DorisArrowFallbackPartitionReader35.probeEndpoints(
                    List.of("fe-1", "fe-2", "fe-3", "fe-4", "fe-5"),
                    8070,
                    (host, port, timeoutMillis) -> {
                      attempts.add(host);
                      timeouts.add(timeoutMillis);
                      elapsedNanos.addAndGet(
                          TimeUnit.MILLISECONDS.toNanos(Math.min(750, timeoutMillis)));
                      throw new SocketTimeoutException("redacted");
                    },
                    elapsedNanos::get))
        .isInstanceOf(ConnectException.class)
        .hasMessage("Doris Arrow Flight SQL endpoint is unavailable");

    assertThat(attempts).containsExactly("fe-1", "fe-2", "fe-3", "fe-4");
    assertThat(timeouts).containsExactly(1_000, 1_000, 1_000, 750);
    assertThat(elapsedNanos).hasValue(TimeUnit.SECONDS.toNanos(3));
  }

  @Test
  void serializesTheFactoryWithThePinnedDorisConfigContract() throws Exception {
    DorisArrowFallbackPartitionReaderFactory35 factory =
        new DorisArrowFallbackPartitionReaderFactory35(
            DataTypes.createStructType(
                new org.apache.spark.sql.types.StructField[] {
                  DataTypes.createStructField("value", DataTypes.StringType, true)
                }),
            config(),
            ENDPOINT_IDENTITY,
            List.of("fe"));

    byte[] serialized;
    try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        ObjectOutputStream output = new ObjectOutputStream(bytes)) {
      output.writeObject(factory);
      serialized = bytes.toByteArray();
    }

    Object restored;
    try (ObjectInputStream input = new ObjectInputStream(new ByteArrayInputStream(serialized))) {
      restored = input.readObject();
    }
    assertThat(restored).isInstanceOf(DorisArrowFallbackPartitionReaderFactory35.class);
    org.apache.spark.sql.connector.read.PartitionReaderFactory restoredFactory =
        (org.apache.spark.sql.connector.read.PartitionReaderFactory) restored;
    try (org.apache.spark.sql.connector.read.PartitionReader<InternalRow> restoredReader =
        restoredFactory.createReader(inputPartition())) {
      assertThat(restoredReader).isInstanceOf(DorisArrowFallbackPartitionReader35.class);
    }
  }

  @Test
  void failureAfterDeliveredRowNeverFallsBackOrOpensCircuit() throws Exception {
    FakeReader arrow = new FakeReader(readerPartition(), new Object[][] {{"delivered"}});
    arrow.failHasNextAfterRows = true;
    AtomicInteger thriftCreations = new AtomicInteger();
    DorisArrowFallbackPartitionReader35 reader =
        reader(
            ignored -> arrow,
            ignored -> {
              thriftCreations.incrementAndGet();
              return new FakeReader(readerPartition(), new Object[0][]);
            },
            (hosts, port) -> {});

    assertThat(reader.next()).isTrue();
    assertThat(reader.get().getUTF8String(0).toString()).isEqualTo("delivered");
    assertThatThrownBy(reader::next)
        .isInstanceOf(IOException.class)
        .hasMessage("Doris partition read failed");
    assertThat(thriftCreations).hasValue(0);
    assertThat(DorisArrowFallbackCircuitBreaker35.isOpen(ENDPOINT_IDENTITY)).isFalse();
  }

  @Test
  void nonTransportConstructorFailureIsSanitizedAndDoesNotFallback() throws Exception {
    AtomicInteger thriftCreations = new AtomicInteger();
    DorisArrowFallbackPartitionReader35 reader =
        reader(
            ignored -> {
              throw new SecurityException("credential-secret-canary");
            },
            ignored -> {
              thriftCreations.incrementAndGet();
              return new FakeReader(readerPartition(), new Object[0][]);
            },
            (hosts, port) -> {});

    assertThatThrownBy(reader::next)
        .isInstanceOf(IOException.class)
        .hasMessage("Doris partition read failed")
        .hasMessageNotContaining("secret-canary");
    assertThat(thriftCreations).hasValue(0);
    assertThat(DorisArrowFallbackCircuitBreaker35.isOpen(ENDPOINT_IDENTITY)).isFalse();
  }

  @Test
  void fallbackCreationFailureRetainsOnlyRedactedFailureTypes() throws Exception {
    DorisArrowFallbackPartitionReader35 reader =
        reader(
            ignored -> {
              throw new DorisException(new ConnectException("arrow-secret-canary"));
            },
            ignored -> {
              throw new SecurityException("thrift-secret-canary");
            },
            (hosts, port) -> {});

    assertThatThrownBy(reader::next)
        .isInstanceOf(IOException.class)
        .hasMessage("Doris partition read failed")
        .hasMessageNotContaining("secret-canary")
        .satisfies(
            failure -> {
              assertThat(failure.getSuppressed()).hasSize(2);
              assertThat(java.util.Arrays.toString(failure.getSuppressed()))
                  .contains(ConnectException.class.getName())
                  .contains(SecurityException.class.getName())
                  .doesNotContain("secret-canary");
            });
    assertThat(DorisArrowFallbackCircuitBreaker35.isOpen(ENDPOINT_IDENTITY)).isTrue();
  }

  private static DorisArrowFallbackPartitionReader35 reader(
      DorisArrowFallbackPartitionReader35.ReaderCreator arrowCreator,
      DorisArrowFallbackPartitionReader35.ReaderCreator thriftCreator,
      DorisArrowFallbackPartitionReader35.EndpointProbe probe)
      throws Exception {
    return new DorisArrowFallbackPartitionReader35(
        inputPartition(),
        DataTypes.createStructType(
            new org.apache.spark.sql.types.StructField[] {
              DataTypes.createStructField("value", DataTypes.StringType, true)
            }),
        config(),
        ENDPOINT_IDENTITY,
        List.of("fe"),
        8070,
        arrowCreator,
        thriftCreator,
        probe);
  }

  private static DorisInputPartition inputPartition() {
    return new DorisInputPartition(
        "analytics",
        "events",
        new Backend("be", 8040, 9060),
        new long[] {1L},
        "opaque-plan",
        new String[] {"value"},
        new String[0],
        -1,
        false);
  }

  private static DorisConfig config() throws Exception {
    Map<String, String> options = new HashMap<>();
    options.put("doris.fenodes", "fe:8030");
    options.put("doris.query.port", "9030");
    options.put("doris.user", "reader");
    options.put("doris.password", "test-password");
    options.put("doris.table.identifier", "analytics.events");
    options.put("doris.read.mode", "thrift");
    options.put("doris.read.arrow-flight-sql.port", "8070");
    options.put("doris.fe.auto.fetch", "false");
    return DorisConfig.fromMap(options, false);
  }

  private static DorisReaderPartition readerPartition() throws Exception {
    return new DorisReaderPartition(
        "analytics",
        "events",
        new Backend("be", 8040, 9060),
        new Long[] {1L},
        "opaque-plan",
        new String[] {"value"},
        new String[0],
        -1,
        config(),
        false);
  }

  private static final class FakeReader extends DorisReader {

    private final Object[][] rows;
    private int position;
    private int closeCalls;
    private boolean failNextAfterAdvance;
    private boolean failHasNextAfterRows;

    private FakeReader(DorisReaderPartition partition, Object[][] rows) {
      super(partition);
      this.rows = rows;
    }

    @Override
    public boolean hasNext() throws DorisException {
      if (failHasNextAfterRows && position >= rows.length) {
        throw new DorisException(new ConnectException("redacted"));
      }
      return position < rows.length;
    }

    @Override
    public Object next() throws DorisException {
      if (failNextAfterAdvance) {
        position++;
        throw new DorisException(new ConnectException("redacted"));
      }
      return rows[position++];
    }

    @Override
    public void close() {
      closeCalls++;
    }
  }
}
