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

import java.io.IOException;
import java.io.Serializable;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import org.apache.doris.spark.client.entity.DorisReaderPartition;
import org.apache.doris.spark.client.read.DorisFlightSqlReader;
import org.apache.doris.spark.client.read.DorisReader;
import org.apache.doris.spark.client.read.DorisThriftReader;
import org.apache.doris.spark.config.DorisConfig;
import org.apache.doris.spark.read.DorisInputPartition;
import org.apache.doris.spark.util.RowConvertors;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructType;

/** Arrow-first partition reader with a hard fallback boundary before the first delivered row. */
final class DorisArrowFallbackPartitionReader35 implements PartitionReader<InternalRow> {

  private static final int PROBE_CONNECT_TIMEOUT_MILLIS = 1_000;
  private static final int PROBE_TOTAL_TIMEOUT_MILLIS = 3_000;

  enum State {
    ARROW_NOT_STARTED,
    ARROW_ROW_READY_NOT_DELIVERED,
    ARROW_DELIVERED,
    ARROW_EXHAUSTED,
    THRIFT_FALLBACK,
    CLOSED
  }

  @FunctionalInterface
  interface ReaderCreator extends Serializable {
    DorisReader create(DorisReaderPartition partition) throws Exception;
  }

  @FunctionalInterface
  interface EndpointProbe extends Serializable {
    void verify(List<String> hosts, int port) throws IOException;
  }

  @FunctionalInterface
  interface EndpointConnector {
    void connect(String host, int port, int timeoutMillis) throws IOException;
  }

  private final StructType schema;
  private final DorisReaderPartition readerPartition;
  private final String endpointIdentity;
  private final List<String> frontendHosts;
  private final int flightPort;
  private final ReaderCreator arrowCreator;
  private final ReaderCreator thriftCreator;
  private final EndpointProbe endpointProbe;

  private State state = State.ARROW_NOT_STARTED;
  private DorisReader arrowReader;
  private DorisReader thriftReader;
  private boolean thriftRowReady;

  DorisArrowFallbackPartitionReader35(
      InputPartition partition,
      StructType schema,
      DorisConfig config,
      String endpointIdentity,
      List<String> frontendHosts,
      int flightPort) {
    this(
        partition,
        schema,
        config,
        endpointIdentity,
        frontendHosts,
        flightPort,
        DorisFlightSqlReader::new,
        DorisThriftReader::new,
        DorisArrowFallbackPartitionReader35::probeEndpoints);
  }

  DorisArrowFallbackPartitionReader35(
      InputPartition partition,
      StructType schema,
      DorisConfig config,
      String endpointIdentity,
      List<String> frontendHosts,
      int flightPort,
      ReaderCreator arrowCreator,
      ReaderCreator thriftCreator,
      EndpointProbe endpointProbe) {
    DorisChecks.checkArgument(
        partition instanceof DorisInputPartition, "Unexpected Doris input partition");
    this.schema = schema;
    this.readerPartition = toReaderPartition((DorisInputPartition) partition, config);
    this.endpointIdentity = endpointIdentity;
    this.frontendHosts = List.copyOf(frontendHosts);
    this.flightPort = flightPort;
    this.arrowCreator = arrowCreator;
    this.thriftCreator = thriftCreator;
    this.endpointProbe = endpointProbe;
  }

  @Override
  public boolean next() throws IOException {
    switch (state) {
      case CLOSED:
        throw new IOException("Doris partition reader is closed");
      case ARROW_EXHAUSTED:
        return false;
      case THRIFT_FALLBACK:
        return advanceThrift();
      case ARROW_ROW_READY_NOT_DELIVERED:
        throw new IOException("Doris partition reader get() was not called after next()");
      case ARROW_NOT_STARTED:
        if (DorisArrowFallbackCircuitBreaker35.isOpen(endpointIdentity)) {
          startThrift();
          return advanceThrift();
        }
        DorisArrowFallbackCircuitBreaker35.recordAttempt(endpointIdentity);
        try {
          endpointProbe.verify(frontendHosts, flightPort);
        } catch (IOException probeFailure) {
          if (Thread.currentThread().isInterrupted()) {
            throw readFailure(probeFailure);
          }
          openCircuitAndStartThrift(probeFailure);
          return advanceThrift();
        }
        try {
          arrowReader = arrowCreator.create(readerPartition);
        } catch (Throwable creationFailure) {
          if (DorisArrowFallbackFailureClassifier35.isFallbackEligible(creationFailure)) {
            openCircuitAndStartThrift(creationFailure);
            return advanceThrift();
          }
          throw readFailure(creationFailure);
        }
        return advanceArrow();
      case ARROW_DELIVERED:
        return advanceArrow();
      default:
        throw new IOException("Doris partition reader entered an invalid state");
    }
  }

  @Override
  public InternalRow get() {
    if (state == State.THRIFT_FALLBACK) {
      if (!thriftRowReady) {
        throw new IllegalStateException("Doris Thrift row is not ready");
      }
      thriftRowReady = false;
      return readAndConvert(thriftReader, false);
    }
    if (state != State.ARROW_ROW_READY_NOT_DELIVERED) {
      throw new IllegalStateException("Doris Arrow row is not ready");
    }

    Object[] values;
    try {
      values = values(arrowReader.next());
    } catch (Throwable readFailure) {
      if (!DorisArrowFallbackFailureClassifier35.isFallbackEligible(readFailure)) {
        throw runtimeReadFailure(readFailure);
      }
      try {
        openCircuitAndStartThrift(readFailure);
        if (!advanceThrift()) {
          IllegalStateException inconsistentResult =
              new IllegalStateException("Doris Thrift fallback returned no replacement row");
          inconsistentResult.addSuppressed(sanitizedFailure(readFailure));
          throw inconsistentResult;
        }
      } catch (IOException fallbackFailure) {
        throw new IllegalStateException("Doris Thrift fallback failed", fallbackFailure);
      }
      thriftRowReady = false;
      return readAndConvert(thriftReader, false);
    }

    InternalRow row = convert(values);
    state = State.ARROW_DELIVERED;
    return row;
  }

  @Override
  public void close() {
    if (state == State.CLOSED) {
      return;
    }
    try {
      closeReader(arrowReader);
    } finally {
      try {
        closeReader(thriftReader);
      } finally {
        arrowReader = null;
        thriftReader = null;
        thriftRowReady = false;
        state = State.CLOSED;
      }
    }
  }

  State stateForTests() {
    return state;
  }

  private boolean advanceArrow() throws IOException {
    boolean fallbackAllowed = state == State.ARROW_NOT_STARTED;
    try {
      if (arrowReader.hasNext()) {
        state = State.ARROW_ROW_READY_NOT_DELIVERED;
        return true;
      }
      state = State.ARROW_EXHAUSTED;
      return false;
    } catch (Throwable readFailure) {
      if (!fallbackAllowed
          || !DorisArrowFallbackFailureClassifier35.isFallbackEligible(readFailure)) {
        throw readFailure(readFailure);
      }
      openCircuitAndStartThrift(readFailure);
      return advanceThrift();
    }
  }

  private boolean advanceThrift() throws IOException {
    try {
      thriftRowReady = thriftReader.hasNext();
      return thriftRowReady;
    } catch (Throwable readFailure) {
      throw readFailure(readFailure);
    }
  }

  private InternalRow readAndConvert(DorisReader reader, boolean allowFallback) {
    try {
      return convert(values(reader.next()));
    } catch (Throwable readFailure) {
      if (allowFallback && DorisArrowFallbackFailureClassifier35.isFallbackEligible(readFailure)) {
        throw new IllegalStateException("Doris Arrow fallback must be handled before conversion");
      }
      throw runtimeReadFailure(readFailure);
    }
  }

  private InternalRow convert(Object[] values) {
    if (values.length != schema.length()) {
      throw new IllegalStateException("Doris row and schema column counts differ");
    }
    GenericInternalRow row = new GenericInternalRow(schema.length());
    for (int index = 0; index < values.length; index++) {
      Object value = values[index];
      if (value == null) {
        row.setNullAt(index);
      } else {
        row.update(
            index,
            RowConvertors.convertValue(
                value,
                schema.fields()[index].dataType(),
                readerPartition.getDateTimeJava8APIEnabled()));
      }
    }
    return row;
  }

  private void openCircuitAndStartThrift(Throwable fallbackTrigger) throws IOException {
    closeReader(arrowReader);
    arrowReader = null;
    DorisArrowFallbackCircuitBreaker35.open(endpointIdentity);
    try {
      startThrift();
    } catch (IOException thriftFailure) {
      thriftFailure.addSuppressed(sanitizedFailure(fallbackTrigger));
      throw thriftFailure;
    }
  }

  private void startThrift() throws IOException {
    try {
      thriftReader = thriftCreator.create(readerPartition);
      state = State.THRIFT_FALLBACK;
    } catch (Throwable creationFailure) {
      throw readFailure(creationFailure);
    }
  }

  private static DorisReaderPartition toReaderPartition(
      DorisInputPartition partition, DorisConfig config) {
    Long[] tablets = Arrays.stream(partition.tablets()).boxed().toArray(Long[]::new);
    return new DorisReaderPartition(
        partition.database(),
        partition.table(),
        partition.backend(),
        tablets,
        partition.opaquedQueryPlan(),
        partition.readCols(),
        partition.predicates(),
        partition.limit(),
        config,
        partition.datetimeJava8ApiEnabled());
  }

  private static Object[] values(Object value) {
    if (!(value instanceof Object[])) {
      throw new IllegalStateException("Doris reader returned an invalid row representation");
    }
    return (Object[]) value;
  }

  private static void closeReader(DorisReader reader) {
    if (reader != null) {
      reader.close();
    }
  }

  private static void probeEndpoints(List<String> hosts, int port) throws IOException {
    probeEndpoints(
        hosts, port, DorisArrowFallbackPartitionReader35::connectEndpoint, System::nanoTime);
  }

  static void probeEndpoints(
      List<String> hosts, int port, EndpointConnector connector, LongSupplier nanoTime)
      throws IOException {
    long startedNanos = nanoTime.getAsLong();
    long totalBudgetNanos = TimeUnit.MILLISECONDS.toNanos(PROBE_TOTAL_TIMEOUT_MILLIS);
    for (String host : hosts) {
      long elapsedNanos = nanoTime.getAsLong() - startedNanos;
      if (elapsedNanos >= totalBudgetNanos) {
        break;
      }
      long remainingNanos = totalBudgetNanos - Math.max(0L, elapsedNanos);
      long remainingMillis = Math.max(1L, TimeUnit.NANOSECONDS.toMillis(remainingNanos));
      int connectTimeoutMillis = (int) Math.min(PROBE_CONNECT_TIMEOUT_MILLIS, remainingMillis);
      try {
        connector.connect(host, port, connectTimeoutMillis);
        return;
      } catch (IOException ignored) {
        // Continue across the catalog-managed FE host list while the total budget remains.
      }
    }
    throw new ConnectException("Doris Arrow Flight SQL endpoint is unavailable");
  }

  private static void connectEndpoint(String host, int port, int timeoutMillis) throws IOException {
    try (Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress(host, port), timeoutMillis);
    }
  }

  private static IOException readFailure(Throwable failure) {
    if (failure instanceof InterruptedException) {
      Thread.currentThread().interrupt();
    }
    if (failure instanceof Error) {
      throw (Error) failure;
    }
    IOException sanitized = new IOException("Doris partition read failed");
    sanitized.addSuppressed(sanitizedFailure(failure));
    return sanitized;
  }

  private static RuntimeException runtimeReadFailure(Throwable failure) {
    if (failure instanceof InterruptedException) {
      Thread.currentThread().interrupt();
    }
    if (failure instanceof Error) {
      throw (Error) failure;
    }
    IllegalStateException sanitized = new IllegalStateException("Doris partition read failed");
    sanitized.addSuppressed(sanitizedFailure(failure));
    return sanitized;
  }

  private static IOException sanitizedFailure(Throwable failure) {
    java.util.ArrayDeque<Throwable> pending = new java.util.ArrayDeque<>();
    java.util.Set<Throwable> visited =
        java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
    StringBuilder types = new StringBuilder("Redacted failure types:");
    pending.add(failure);
    while (!pending.isEmpty() && visited.size() < 16) {
      Throwable current = pending.removeFirst();
      if (!visited.add(current)) {
        continue;
      }
      types.append(' ').append(current.getClass().getName());
      if (current.getCause() != null) {
        pending.addLast(current.getCause());
      }
      for (Throwable suppressed : current.getSuppressed()) {
        if (suppressed != null) {
          pending.addLast(suppressed);
        }
      }
    }
    if (!pending.isEmpty()) {
      types.append(" truncated");
    }
    return new IOException(types.toString());
  }
}
