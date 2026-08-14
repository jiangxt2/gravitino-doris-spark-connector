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

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/** Testcontainers lifecycle and numeric control client for the JDK-only Doris TCP proxy. */
final class RecordingDorisTcpProxy implements AutoCloseable {

  enum Lane {
    CONTROL("control", 19030),
    DENIAL("denial", 19031),
    FE_HTTP("fe-http", 18030),
    FLIGHT("flight", 18070),
    FLIGHT_FAILURE("flight-failure", 18071);

    private final String queryValue;
    private final int port;

    Lane(String queryValue, int port) {
      this.queryValue = queryValue;
      this.port = port;
    }
  }

  static final class State {

    private final long accepted;
    private final int active;
    private final long generation;

    private State(long accepted, int active, long generation) {
      this.accepted = accepted;
      this.active = active;
      this.generation = generation;
    }

    long accepted() {
      return accepted;
    }

    int active() {
      return active;
    }

    long generation() {
      return generation;
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(RecordingDorisTcpProxy.class);
  private static final int ADMIN_PORT = 18080;
  private static final int FE_HTTP_TARGET_PORT = 8030;
  private static final int FE_FLIGHT_TARGET_PORT = 8070;
  private static final int FE_QUERY_TARGET_PORT = 9030;
  private static final String IMAGE =
      "apache/gravitino:1.3.0@sha256:4ff340f1160600ecac8126c2a0c4b88ea2178d3f1954966af559bab526485af6";
  private static final String SERVER_CLASS = RecordingDorisTcpProxyServer.class.getName();
  private static final int CONNECT_TIMEOUT_MILLIS = 5000;
  private static final int READ_TIMEOUT_MILLIS = 5000;

  private final GenericContainer<?> container;
  private String proxyAddress;

  RecordingDorisTcpProxy(DockerTestNetwork network) {
    Path classes = integrationTestClasses();
    container =
        new GenericContainer<>(DockerImageName.parse(IMAGE))
            .withCreateContainerCmdModifier(command -> command.withEntrypoint("java"))
            .withCommand(
                "--add-modules=jdk.httpserver",
                "-cp",
                "/opt/recording-proxy/classes",
                SERVER_CLASS,
                "doris-fe",
                Integer.toString(FE_QUERY_TARGET_PORT),
                Integer.toString(FE_HTTP_TARGET_PORT),
                Integer.toString(FE_FLIGHT_TARGET_PORT),
                Integer.toString(Lane.CONTROL.port),
                Integer.toString(Lane.DENIAL.port),
                Integer.toString(Lane.FE_HTTP.port),
                Integer.toString(Lane.FLIGHT.port),
                Integer.toString(Lane.FLIGHT_FAILURE.port),
                Integer.toString(ADMIN_PORT))
            .withLabel(IntegrationTestContainerLabels.KEY, IntegrationTestContainerLabels.VALUE)
            .withNetworkMode(network.name())
            .withFileSystemBind(
                classes.toString(), "/opt/recording-proxy/classes", BindMode.READ_ONLY)
            .withExposedPorts(
                Lane.CONTROL.port,
                Lane.DENIAL.port,
                Lane.FE_HTTP.port,
                Lane.FLIGHT.port,
                Lane.FLIGHT_FAILURE.port,
                ADMIN_PORT)
            .waitingFor(
                Wait.forHttp("/health")
                    .forPort(ADMIN_PORT)
                    .forStatusCode(200)
                    .withStartupTimeout(Duration.ofMinutes(1)))
            .withLogConsumer(new Slf4jLogConsumer(LOG).withPrefix("doris-tcp-proxy"));
  }

  void start() {
    container.start();
    IntegrationTestContainerLabels.assertProjectLabel(
        "Doris TCP proxy",
        container.getContainerInfo().getConfig() == null
            ? null
            : container.getContainerInfo().getConfig().getLabels());
    com.github.dockerjava.api.model.ContainerNetwork network =
        container.getContainerInfo().getNetworkSettings().getNetworks().values().stream()
            .findFirst()
            .orElseThrow(
                () -> new IllegalStateException("Doris TCP proxy has no Docker network metadata"));
    proxyAddress = network.getIpAddress();
    if (proxyAddress == null || proxyAddress.isEmpty()) {
      throw new IllegalStateException("Doris TCP proxy has no internal IP address");
    }
  }

  String jdbcUrl(Lane lane) {
    if (proxyAddress == null) {
      throw new IllegalStateException("Doris TCP proxy has not started");
    }
    if (lane != Lane.CONTROL && lane != Lane.DENIAL) {
      throw new IllegalArgumentException("The selected TCP proxy lane is not a JDBC lane");
    }
    return String.format("jdbc:mysql://%s:%d/", proxyAddress, lane.port);
  }

  String feEndpoint() {
    if (proxyAddress == null) {
      throw new IllegalStateException("Doris TCP proxy has not started");
    }
    return String.format("%s:%d", proxyAddress, Lane.FE_HTTP.port);
  }

  int flightPort(Lane lane) {
    if (lane != Lane.FLIGHT && lane != Lane.FLIGHT_FAILURE) {
      throw new IllegalArgumentException("The selected TCP proxy lane is not a Flight lane");
    }
    return lane.port;
  }

  State state(Lane lane) {
    return request("GET", "/state?lane=" + lane.queryValue);
  }

  State reset(Lane lane) {
    return request("POST", "/reset?lane=" + lane.queryValue);
  }

  State setFlightFailureAvailable(boolean available) {
    return request(
        "POST", "/availability?lane=" + Lane.FLIGHT_FAILURE.queryValue + "&available=" + available);
  }

  @Override
  public void close() {
    container.stop();
  }

  private State request(String method, String path) {
    HttpURLConnection connection = null;
    try {
      URI uri =
          new URI(
              "http",
              null,
              container.getHost(),
              container.getMappedPort(ADMIN_PORT),
              path.substring(0, path.indexOf('?')),
              path.substring(path.indexOf('?') + 1),
              null);
      connection = (HttpURLConnection) uri.toURL().openConnection();
      connection.setRequestMethod(method);
      connection.setConnectTimeout(CONNECT_TIMEOUT_MILLIS);
      connection.setReadTimeout(READ_TIMEOUT_MILLIS);
      if ("POST".equals(method)) {
        connection.setDoOutput(true);
        connection.getOutputStream().close();
      }
      int status = connection.getResponseCode();
      if (status != 200) {
        throw new IllegalStateException(
            "Doris TCP proxy control request failed with status " + status);
      }
      Properties values = new Properties();
      try (InputStream input = connection.getInputStream()) {
        values.load(input);
      }
      return new State(
          Long.parseLong(values.getProperty("accepted")),
          Integer.parseInt(values.getProperty("active")),
          Long.parseLong(values.getProperty("generation")));
    } catch (IOException | URISyntaxException | RuntimeException e) {
      throw new IllegalStateException("Unable to read Doris TCP proxy counters", e);
    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }
  }

  private static Path integrationTestClasses() {
    try {
      Path classes =
          Paths.get(
                  RecordingDorisTcpProxyServer.class
                      .getProtectionDomain()
                      .getCodeSource()
                      .getLocation()
                      .toURI())
              .toAbsolutePath();
      if (!Files.isDirectory(classes)) {
        throw new IllegalStateException("Integration-test classes are not a directory");
      }
      return classes;
    } catch (URISyntaxException e) {
      throw new IllegalStateException("Unable to locate integration-test proxy classes", e);
    }
  }
}
