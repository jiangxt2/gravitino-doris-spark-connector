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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** Tests Docker Engine gateway-mode selection without contacting Docker. */
public class TestDockerTestNetwork {

  @Test
  void selectsUnprotectedNatOnlyForDocker28AndLater() {
    assertThat(DockerTestNetwork.gatewayOptions("27.5.1")).isEmpty();
    assertThat(DockerTestNetwork.gatewayOptions("28.0.0"))
        .containsEntry("com.docker.network.bridge.gateway_mode_ipv4", "nat-unprotected");
    assertThat(DockerTestNetwork.gatewayOptions("v29.6.2"))
        .containsEntry("com.docker.network.bridge.gateway_mode_ipv4", "nat-unprotected");
    assertThat(DockerTestNetwork.gatewayOptions(null)).isEmpty();
    assertThat(DockerTestNetwork.gatewayOptions("development")).isEmpty();
  }
}
