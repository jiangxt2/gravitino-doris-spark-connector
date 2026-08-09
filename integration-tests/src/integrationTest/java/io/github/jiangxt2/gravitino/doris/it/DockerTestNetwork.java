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

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.model.Network;
import com.github.dockerjava.api.model.Network.Ipam;
import com.github.dockerjava.api.model.Network.Ipam.Config;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;

/** Host-routable Docker network shared by the Doris and Gravitino integration containers. */
final class DockerTestNetwork implements AutoCloseable {

  static final String SUBNET = "10.20.30.0/28";

  private static final Logger LOG = LoggerFactory.getLogger(DockerTestNetwork.class);
  private static final String GATEWAY = "10.20.30.1";
  private static final String GATEWAY_MODE_OPTION = "com.docker.network.bridge.gateway_mode_ipv4";
  private static final String NAT_UNPROTECTED = "nat-unprotected";

  private final DockerClient dockerClient;
  private final String id;
  private final String name;
  private final boolean owned;

  private DockerTestNetwork(DockerClient dockerClient, String id, String name, boolean owned) {
    this.dockerClient = dockerClient;
    this.id = id;
    this.name = name;
    this.owned = owned;
  }

  static DockerTestNetwork create() {
    DockerClient client = DockerClientFactory.instance().client();
    Map<String, String> options = gatewayOptions(client.versionCmd().exec().getVersion());
    Network reusable = findNetworkForSubnet(client.listNetworksCmd().exec());
    if (reusable != null) {
      if (!options.isEmpty()
          && (reusable.getOptions() == null
              || !NAT_UNPROTECTED.equals(reusable.getOptions().get(GATEWAY_MODE_OPTION)))) {
        throw new IllegalStateException(
            "Docker Engine 28 or later requires gateway_mode_ipv4=nat-unprotected on network "
                + reusable.getName()
                + "; recreate only that exact test network after stopping its containers");
      }
      LOG.info("Reusing Docker network {} for integration tests", reusable.getName());
      return new DockerTestNetwork(client, reusable.getId(), reusable.getName(), false);
    }

    String networkName = "gravitino-doris-it-" + UUID.randomUUID().toString().substring(0, 8);
    Config config = new Config().withSubnet(SUBNET).withGateway(GATEWAY).withIpRange(SUBNET);
    String networkId =
        client
            .createNetworkCmd()
            .withName(networkName)
            .withIpam(new Ipam().withConfig(config))
            .withOptions(options)
            .exec()
            .getId();
    LOG.info("Created Docker network {} ({}) for integration tests", networkName, networkId);
    return new DockerTestNetwork(client, networkId, networkName, true);
  }

  String name() {
    return name;
  }

  @Override
  public void close() {
    if (!owned) {
      return;
    }
    try {
      dockerClient.removeNetworkCmd(id).exec();
      LOG.info("Removed integration-test Docker network {}", name);
    } catch (RuntimeException e) {
      LOG.warn("Unable to remove integration-test Docker network {}", name, e);
    }
  }

  static Map<String, String> gatewayOptions(String dockerVersion) {
    if (dockerVersion == null || dockerVersion.trim().isEmpty()) {
      return Collections.emptyMap();
    }
    String normalized = dockerVersion.trim();
    if (normalized.startsWith("v")) {
      normalized = normalized.substring(1);
    }
    int separator = normalized.indexOf('.');
    String major = separator < 0 ? normalized : normalized.substring(0, separator);
    try {
      if (Integer.parseInt(major) >= 28) {
        return Collections.singletonMap(GATEWAY_MODE_OPTION, NAT_UNPROTECTED);
      }
    } catch (NumberFormatException e) {
      LOG.warn("Unable to parse Docker version {}; using the default gateway mode", dockerVersion);
    }
    return Collections.emptyMap();
  }

  private static Network findNetworkForSubnet(List<Network> networks) {
    for (Network network : networks) {
      if (network.getIpam() == null || network.getIpam().getConfig() == null) {
        continue;
      }
      for (Config config : network.getIpam().getConfig()) {
        if (SUBNET.equals(config.getSubnet())) {
          return network;
        }
      }
    }
    return null;
  }
}
