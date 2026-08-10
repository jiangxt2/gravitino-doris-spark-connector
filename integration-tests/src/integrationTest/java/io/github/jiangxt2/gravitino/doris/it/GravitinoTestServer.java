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

import java.nio.file.Path;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/** Real Gravitino 1.3.0 server with the independent catalog provider installed. */
final class GravitinoTestServer implements AutoCloseable {

  static final String ADMIN_USER = "admin";

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoTestServer.class);
  private static final int HTTP_PORT = 8090;

  private final GenericContainer<?> container;

  GravitinoTestServer(DockerTestNetwork network, Path providerDirectory) {
    container =
        new GenericContainer<>(DockerImageName.parse("apache/gravitino:1.3.0"))
            .withLabel(IntegrationTestContainerLabels.KEY, IntegrationTestContainerLabels.VALUE)
            .withNetworkMode(network.name())
            .withFileSystemBind(
                providerDirectory.toAbsolutePath().toString(),
                "/opt/gravitino/catalogs/doris-governed",
                BindMode.READ_ONLY)
            .withEnv("GRAVITINO_AUTHORIZATION_ENABLE", "true")
            .withEnv("GRAVITINO_AUTHORIZATION_SERVICE_ADMINS", ADMIN_USER)
            .withEnv("GRAVITINO_AUX_SERVICE_NAMES", "")
            .withExposedPorts(HTTP_PORT)
            .waitingFor(
                Wait.forHttp("/api/version")
                    .forStatusCode(200)
                    .withStartupTimeout(Duration.ofMinutes(3)))
            .withLogConsumer(new Slf4jLogConsumer(LOG).withPrefix("gravitino-1.3.0"));
  }

  void start() {
    container.start();
    IntegrationTestContainerLabels.assertProjectLabel(
        "Gravitino server",
        container.getContainerInfo().getConfig() == null
            ? null
            : container.getContainerInfo().getConfig().getLabels());
  }

  String uri() {
    return String.format("http://%s:%d", container.getHost(), container.getMappedPort(HTTP_PORT));
  }

  @Override
  public void close() {
    container.stop();
  }
}
