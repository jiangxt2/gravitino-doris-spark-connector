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

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A transparent FE HTTP proxy that records only request methods and paths.
 *
 * <p>The proxy deliberately does not retain request headers, bodies, response bodies, or
 * credentials. Tests can also inject one path-specific response to exercise a physical metadata
 * failure after Gravitino authorization has succeeded.
 */
final class RecordingDorisHttpProxy implements AutoCloseable {

  private static final int BUFFER_SIZE = 8192;

  private final URI target;
  private final HttpServer server;
  private final ConcurrentMap<String, AtomicInteger> requestCounts = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, FailureResponse> nextFailures = new ConcurrentHashMap<>();
  private final AtomicInteger totalRequestCount = new AtomicInteger();

  static RecordingDorisHttpProxy start(String targetEndpoint) throws IOException {
    return new RecordingDorisHttpProxy(URI.create("http://" + targetEndpoint));
  }

  private RecordingDorisHttpProxy(URI target) throws IOException {
    this.target = target;
    server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    server.createContext("/", this::handle);
    server.start();
  }

  String endpoint() {
    return String.format("127.0.0.1:%d", server.getAddress().getPort());
  }

  int requestCount(String method, String path) {
    return requestCounts.getOrDefault(requestKey(method, path), new AtomicInteger()).get();
  }

  int totalRequestCount() {
    return totalRequestCount.get();
  }

  void reset() {
    requestCounts.clear();
    totalRequestCount.set(0);
    nextFailures.clear();
  }

  void failNextRequest(String path, int status, String responseBody) {
    nextFailures.put(path, new FailureResponse(status, responseBody));
  }

  @Override
  public void close() {
    server.stop(0);
  }

  private void handle(HttpExchange exchange) throws IOException {
    String path = exchange.getRequestURI().getPath();
    totalRequestCount.incrementAndGet();
    requestCounts
        .computeIfAbsent(
            requestKey(exchange.getRequestMethod(), path), ignored -> new AtomicInteger())
        .incrementAndGet();

    FailureResponse failure = nextFailures.remove(path);
    if (failure != null) {
      sendResponse(exchange, failure.status, failure.body.getBytes(StandardCharsets.UTF_8), null);
      return;
    }

    HttpURLConnection connection = null;
    try {
      URI requestTarget = target.resolve(exchange.getRequestURI().toString());
      connection = (HttpURLConnection) requestTarget.toURL().openConnection(Proxy.NO_PROXY);
      connection.setInstanceFollowRedirects(false);
      connection.setRequestMethod(exchange.getRequestMethod());
      copyRequestHeaders(exchange.getRequestHeaders(), connection);

      byte[] requestBody = readAllBytes(exchange.getRequestBody());
      if (requestBody.length > 0) {
        connection.setDoOutput(true);
        try (OutputStream output = connection.getOutputStream()) {
          output.write(requestBody);
        }
      }

      int status = connection.getResponseCode();
      InputStream responseStream =
          status >= HttpURLConnection.HTTP_BAD_REQUEST
              ? connection.getErrorStream()
              : connection.getInputStream();
      byte[] responseBody = responseStream == null ? new byte[0] : readAllBytes(responseStream);
      sendResponse(exchange, status, responseBody, connection.getHeaderFields());
    } catch (IOException e) {
      byte[] responseBody = "Doris FE proxy forwarding failed".getBytes(StandardCharsets.UTF_8);
      sendResponse(exchange, HttpURLConnection.HTTP_BAD_GATEWAY, responseBody, null);
    } finally {
      if (connection != null) {
        connection.disconnect();
      }
      exchange.close();
    }
  }

  private static void copyRequestHeaders(Headers headers, HttpURLConnection connection) {
    headers.forEach(
        (name, values) -> {
          if (isHopByHopHeader(name) || "host".equalsIgnoreCase(name)) {
            return;
          }
          for (String value : values) {
            connection.addRequestProperty(name, value);
          }
        });
  }

  private static void sendResponse(
      HttpExchange exchange, int status, byte[] body, Map<String, List<String>> responseHeaders)
      throws IOException {
    if (responseHeaders != null) {
      responseHeaders.forEach(
          (name, values) -> {
            if (name == null || isHopByHopHeader(name) || "content-length".equalsIgnoreCase(name)) {
              return;
            }
            exchange.getResponseHeaders().put(name, values);
          });
    }
    exchange.sendResponseHeaders(status, body.length);
    try (OutputStream output = exchange.getResponseBody()) {
      output.write(body);
    }
  }

  private static byte[] readAllBytes(InputStream input) throws IOException {
    try (InputStream stream = input;
        ByteArrayOutputStream output = new ByteArrayOutputStream()) {
      byte[] buffer = new byte[BUFFER_SIZE];
      int bytesRead;
      while ((bytesRead = stream.read(buffer)) != -1) {
        output.write(buffer, 0, bytesRead);
      }
      return output.toByteArray();
    }
  }

  private static boolean isHopByHopHeader(String name) {
    String normalized = name.toLowerCase(Locale.ROOT);
    return "connection".equals(normalized)
        || "keep-alive".equals(normalized)
        || "proxy-authenticate".equals(normalized)
        || "proxy-authorization".equals(normalized)
        || "te".equals(normalized)
        || "trailer".equals(normalized)
        || "transfer-encoding".equals(normalized)
        || "upgrade".equals(normalized);
  }

  private static String requestKey(String method, String path) {
    return method.toUpperCase(Locale.ROOT) + " " + path;
  }

  private static final class FailureResponse {
    private final int status;
    private final String body;

    private FailureResponse(int status, String body) {
      this.status = status;
      this.body = body;
    }
  }
}
