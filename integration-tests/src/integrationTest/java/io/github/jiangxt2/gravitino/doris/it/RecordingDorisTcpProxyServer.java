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

package io.github.jiangxt2.gravitino.doris.it;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** JDK-only TCP forwarding process that records connection counts but never application data. */
public final class RecordingDorisTcpProxyServer {

  private static final int BUFFER_SIZE = 8192;

  private RecordingDorisTcpProxyServer() {}

  /**
   * Starts two independently counted TCP listeners and a numeric-only control endpoint.
   *
   * @param args target host, target port, control listener port, denial listener port, admin port
   * @throws Exception if the proxy cannot start
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 5) {
      throw new IllegalArgumentException("Expected target host and four numeric ports");
    }
    String targetHost = args[0];
    int targetPort = parsePort(args[1]);
    int controlPort = parsePort(args[2]);
    int denialPort = parsePort(args[3]);
    int adminPort = parsePort(args[4]);

    CountDownLatch stopped = new CountDownLatch(1);
    ProxyRuntime runtime =
        new ProxyRuntime(targetHost, targetPort, controlPort, denialPort, adminPort);
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  runtime.close();
                  stopped.countDown();
                },
                "doris-tcp-proxy-shutdown"));
    try {
      runtime.start();
      stopped.await();
    } finally {
      runtime.close();
    }
  }

  private static int parsePort(String value) {
    int port = Integer.parseInt(value);
    if (port < 1 || port > 65535) {
      throw new IllegalArgumentException("Proxy ports must be between 1 and 65535");
    }
    return port;
  }

  private static final class ProxyRuntime implements AutoCloseable {

    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final ProxyLane control;
    private final ProxyLane denial;
    private final HttpServer admin;

    private ProxyRuntime(
        String targetHost, int targetPort, int controlPort, int denialPort, int adminPort)
        throws IOException {
      control = new ProxyLane("control", controlPort, targetHost, targetPort, executor);
      denial = new ProxyLane("denial", denialPort, targetHost, targetPort, executor);
      admin = HttpServer.create(new InetSocketAddress(adminPort), 0);
      admin.setExecutor(executor);
      admin.createContext("/health", this::health);
      admin.createContext("/state", this::state);
      admin.createContext("/reset", this::reset);
    }

    private void start() {
      control.start();
      denial.start();
      admin.start();
    }

    private void health(HttpExchange exchange) throws IOException {
      if (!"GET".equals(exchange.getRequestMethod())) {
        respond(exchange, 405, "method-not-allowed");
        return;
      }
      respond(exchange, 200, "ok");
    }

    private void state(HttpExchange exchange) throws IOException {
      if (!"GET".equals(exchange.getRequestMethod())) {
        respond(exchange, 405, "method-not-allowed");
        return;
      }
      ProxyLane lane = lane(exchange);
      if (lane == null) {
        respond(exchange, 400, "unknown-lane");
        return;
      }
      respond(exchange, 200, lane.state());
    }

    private void reset(HttpExchange exchange) throws IOException {
      if (!"POST".equals(exchange.getRequestMethod())) {
        respond(exchange, 405, "method-not-allowed");
        return;
      }
      ProxyLane lane = lane(exchange);
      if (lane == null) {
        respond(exchange, 400, "unknown-lane");
        return;
      }
      String state = lane.reset();
      respond(exchange, state == null ? 409 : 200, state == null ? "connections-active" : state);
    }

    private ProxyLane lane(HttpExchange exchange) {
      String query = exchange.getRequestURI().getRawQuery();
      if ("lane=control".equals(query)) {
        return control;
      }
      if ("lane=denial".equals(query)) {
        return denial;
      }
      return null;
    }

    @Override
    public void close() {
      admin.stop(0);
      control.close();
      denial.close();
      executor.shutdownNow();
    }

    private static void respond(HttpExchange exchange, int status, String value)
        throws IOException {
      byte[] bytes = value.getBytes(StandardCharsets.US_ASCII);
      exchange.getResponseHeaders().put("Content-Type", java.util.List.of("text/plain"));
      exchange.sendResponseHeaders(status, bytes.length);
      try (OutputStream output = exchange.getResponseBody()) {
        output.write(bytes);
      } finally {
        exchange.close();
      }
    }
  }

  private static final class ProxyLane implements AutoCloseable {

    private final Object stateLock = new Object();
    private final String name;
    private final String targetHost;
    private final int targetPort;
    private final ExecutorService executor;
    private final ServerSocket listener;
    private long accepted;
    private int active;
    private long generation;

    private ProxyLane(
        String name, int listenPort, String targetHost, int targetPort, ExecutorService executor)
        throws IOException {
      this.name = name;
      this.targetHost = targetHost;
      this.targetPort = targetPort;
      this.executor = executor;
      listener = new ServerSocket();
      listener.bind(new InetSocketAddress("0.0.0.0", listenPort));
    }

    private void start() {
      executor.execute(this::acceptConnections);
    }

    private void acceptConnections() {
      while (!listener.isClosed()) {
        try {
          Socket client = listener.accept();
          synchronized (stateLock) {
            accepted++;
            active++;
          }
          executor.execute(() -> forward(client));
        } catch (SocketException e) {
          if (!listener.isClosed()) {
            close();
          }
        } catch (IOException e) {
          close();
        }
      }
    }

    private void forward(Socket client) {
      try (client;
          Socket upstream = new Socket()) {
        upstream.connect(new InetSocketAddress(targetHost, targetPort));
        CompletableFuture<Void> request =
            CompletableFuture.runAsync(() -> copy(client, upstream), executor);
        CompletableFuture<Void> response =
            CompletableFuture.runAsync(() -> copy(upstream, client), executor);
        CompletableFuture.allOf(request, response).join();
      } catch (RuntimeException | IOException ignored) {
        // A transport failure only closes this connection. No application data is retained.
      } finally {
        synchronized (stateLock) {
          active--;
        }
      }
    }

    private String state() {
      synchronized (stateLock) {
        return formatState();
      }
    }

    private String reset() {
      synchronized (stateLock) {
        if (active != 0) {
          return null;
        }
        accepted = 0;
        generation++;
        return formatState();
      }
    }

    private String formatState() {
      return String.format(
          Locale.ROOT,
          "lane=%s%naccepted=%d%nactive=%d%ngeneration=%d%n",
          name,
          accepted,
          active,
          generation);
    }

    @Override
    public void close() {
      try {
        listener.close();
      } catch (IOException ignored) {
        // Closing an already closed listener is harmless during container shutdown.
      }
    }

    private static void copy(Socket source, Socket target) {
      try {
        InputStream input = source.getInputStream();
        OutputStream output = target.getOutputStream();
        byte[] bytes = new byte[BUFFER_SIZE];
        int length;
        while ((length = input.read(bytes)) >= 0) {
          output.write(bytes, 0, length);
          output.flush();
        }
        target.shutdownOutput();
      } catch (IOException ignored) {
        // The owning forward operation closes both sockets.
      }
    }
  }
}
