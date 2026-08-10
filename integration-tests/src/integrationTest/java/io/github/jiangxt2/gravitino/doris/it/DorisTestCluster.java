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

import static java.lang.String.format;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import java.io.File;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.ContainerState;
import org.testcontainers.containers.wait.strategy.Wait;

/** A one-FE, one-BE Doris cluster based on the official split Docker images. */
final class DorisTestCluster implements AutoCloseable {

  static final String ROOT_USER = "root";
  static final String TEST_USER = "governed_reader";
  static final String TEST_PASSWORD = "DorisGovernedIt42";

  private static final Logger LOG = LoggerFactory.getLogger(DorisTestCluster.class);
  private static final String FE_SERVICE = "doris-fe";
  private static final String BE_SERVICE = "doris-be";
  private static final int FE_HTTP_PORT = 8030;
  private static final int FE_MYSQL_PORT = 9030;
  private static final int BE_HTTP_PORT = 8040;
  private static final int BE_HEARTBEAT_PORT = 9050;

  private final DockerTestNetwork network;
  private final String version;
  private final ComposeContainer compose;
  private String internalFeAddress;
  private String hostFeAddress;
  private int hostHttpPort;
  private int hostMysqlPort;

  DorisTestCluster(DockerTestNetwork network, String version, Path repositoryRoot) {
    this.network = network;
    this.version = version;
    Path resources =
        repositoryRoot.resolve("integration-tests/src/integrationTest/resources/doris");
    File composeFile = resources.resolve("docker-compose.yaml").toFile();
    File networkFile = resources.resolve("docker-compose-network.yaml").toFile();
    compose =
        new ComposeContainer(Arrays.asList(composeFile, networkFile))
            .withEnv("GOVERNED_DORIS_FE_IMAGE", "apache/doris:fe-" + version)
            .withEnv("GOVERNED_DORIS_BE_IMAGE", "apache/doris:be-" + version)
            .withEnv("GOVERNED_DORIS_NETWORK_NAME", network.name())
            .withExposedService(
                FE_SERVICE,
                FE_MYSQL_PORT,
                Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(4)))
            .withExposedService(FE_SERVICE, FE_HTTP_PORT)
            .withExposedService(
                BE_SERVICE,
                1,
                BE_HEARTBEAT_PORT,
                Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(4)))
            .withExposedService(BE_SERVICE, 1, BE_HTTP_PORT)
            .withScaledService(BE_SERVICE, 1)
            .withStartupTimeout(Duration.ofMinutes(4))
            .withTailChildContainers(true)
            .withLocalCompose(true);
  }

  void start() {
    compose.start();
    assertProjectLabel(FE_SERVICE);
    assertProjectLabel(BE_SERVICE);
    hostFeAddress = compose.getServiceHost(FE_SERVICE, FE_MYSQL_PORT);
    hostMysqlPort = compose.getServicePort(FE_SERVICE, FE_MYSQL_PORT);
    hostHttpPort = compose.getServicePort(FE_SERVICE, FE_HTTP_PORT);
    internalFeAddress = resolveInternalFeAddress();
    await()
        .atMost(4, TimeUnit.MINUTES)
        .pollInterval(5, TimeUnit.SECONDS)
        .until(this::allBackendsReady);
    createTechnicalUser();
    LOG.info(
        "Doris {} is ready at JDBC {} and FE HTTP {}", version, hostJdbcUrl(), hostFeEndpoint());
  }

  String version() {
    return version;
  }

  String hostJdbcUrl() {
    return format("jdbc:mysql://%s:%d/", hostFeAddress, hostMysqlPort);
  }

  String internalJdbcUrl() {
    return format("jdbc:mysql://%s:%d/", internalFeAddress, FE_MYSQL_PORT);
  }

  String hostFeEndpoint() {
    return format("%s:%d", hostFeAddress, hostHttpPort);
  }

  int hostMysqlPort() {
    return hostMysqlPort;
  }

  @Override
  public void close() {
    compose.stop();
  }

  private boolean allBackendsReady() {
    try (Connection connection = DriverManager.getConnection(hostJdbcUrl(), ROOT_USER, "");
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SHOW PROC '/backends'")) {
      int alive = 0;
      while (resultSet.next()) {
        if (resultSet.getBoolean("Alive")
            && !resultSet.getString("TotalCapacity").startsWith("0")) {
          alive++;
        }
      }
      return alive == 1;
    } catch (Exception e) {
      LOG.info("Waiting for Doris {} backend readiness: {}", version, e.getClass().getSimpleName());
      return false;
    }
  }

  private void createTechnicalUser() {
    try (Connection connection = DriverManager.getConnection(hostJdbcUrl(), ROOT_USER, "");
        Statement statement = connection.createStatement()) {
      statement.execute(
          format("CREATE USER IF NOT EXISTS '%s' IDENTIFIED BY '%s'", TEST_USER, TEST_PASSWORD));
      statement.execute(format("GRANT ADMIN_PRIV ON *.*.* TO '%s'", TEST_USER));
    } catch (Exception e) {
      throw new IllegalStateException("Failed to create the Doris integration-test user", e);
    }
  }

  private String resolveInternalFeAddress() {
    ContainerState fe = requireContainer(FE_SERVICE);
    com.github.dockerjava.api.model.ContainerNetwork settings =
        fe.getContainerInfo().getNetworkSettings().getNetworks().get(network.name());
    if (settings == null || settings.getIpAddress() == null) {
      throw new IllegalStateException(
          "Doris FE is not attached to integration-test network " + network.name());
    }
    return settings.getIpAddress();
  }

  private void assertProjectLabel(String service) {
    ContainerState state = requireContainer(service);
    IntegrationTestContainerLabels.assertProjectLabel(
        "Doris service " + service,
        state.getContainerInfo().getConfig() == null
            ? null
            : state.getContainerInfo().getConfig().getLabels());
  }

  private ContainerState requireContainer(String service) {
    Optional<ContainerState> state = compose.getContainerByServiceName(service);
    return state.orElseThrow(
        () -> new IllegalStateException("Doris service container is unavailable: " + service));
  }
}
