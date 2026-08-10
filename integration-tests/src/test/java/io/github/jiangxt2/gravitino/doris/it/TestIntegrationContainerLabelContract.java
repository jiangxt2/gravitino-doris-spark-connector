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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Verifies that repository configuration uses the integration-test container label contract. */
public class TestIntegrationContainerLabelContract {

  @Test
  void scopesComposeContainersAndFailureLogsToTheProjectLabel() throws IOException {
    String repositoryRoot = System.getProperty("connector.repository.root");
    assertThat(repositoryRoot)
        .as("connector.repository.root must identify the repository under test")
        .isNotBlank();
    Path root = Path.of(repositoryRoot);

    String compose =
        Files.readString(
            root.resolve(
                "integration-tests/src/integrationTest/resources/doris/docker-compose.yaml"));
    String composeLabel =
        IntegrationTestContainerLabels.KEY + ": \"" + IntegrationTestContainerLabels.VALUE + "\"";
    long composeLabelCount =
        compose.lines().filter(line -> line.trim().equals(composeLabel)).count();
    assertThat(composeLabelCount).isEqualTo(2);

    String workflow = Files.readString(root.resolve(".github/workflows/build.yml"));
    int collectionStepStart = workflow.indexOf("- name: Collect container logs");
    int collectionStepEnd = workflow.indexOf("- name: Upload integration reports");
    assertThat(collectionStepStart).isGreaterThanOrEqualTo(0);
    assertThat(collectionStepEnd).isGreaterThan(collectionStepStart);

    String collectionStep = workflow.substring(collectionStepStart, collectionStepEnd);
    String normalizedStep = collectionStep.replace("\\\n", " ");
    String expectedFilter = "--filter 'label=" + IntegrationTestContainerLabels.ASSIGNMENT + "'";
    List<String> dockerPsAllCommands =
        normalizedStep.lines().filter(line -> line.contains("docker ps -a ")).toList();
    List<String> dockerPsAllQuietCommands =
        normalizedStep.lines().filter(line -> line.contains("docker ps -aq ")).toList();

    assertThat(dockerPsAllCommands)
        .singleElement()
        .satisfies(command -> assertThat(command).contains(expectedFilter));
    assertThat(dockerPsAllQuietCommands)
        .singleElement()
        .satisfies(command -> assertThat(command).contains(expectedFilter));
  }
}
