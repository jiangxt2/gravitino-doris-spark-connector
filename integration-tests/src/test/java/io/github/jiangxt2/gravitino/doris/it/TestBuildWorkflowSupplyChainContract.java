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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/** Verifies the immutable-action and least-privilege contract of the build workflow. */
public class TestBuildWorkflowSupplyChainContract {
  private static final Pattern ACTION_REFERENCE =
      Pattern.compile(
          "^\\s*(?:-\\s+)?uses:\\s+([^\\s#]+)\\s+#\\s+(v[^\\s]+)\\s*$", Pattern.MULTILINE);
  private static final Pattern IMMUTABLE_ACTION =
      Pattern.compile("[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)*@[0-9a-f]{40}");

  @Test
  void pinsEveryActionToAFullCommitSha() throws IOException {
    String workflow = readWorkflow();
    List<String> usesLines =
        workflow.lines().filter(line -> line.trim().matches("(?:-\\s+)?uses:.*")).toList();
    Matcher matcher = ACTION_REFERENCE.matcher(workflow);
    int matchedReferences = 0;
    while (matcher.find()) {
      matchedReferences++;
      assertThat(matcher.group(1)).matches(IMMUTABLE_ACTION);
      assertThat(matcher.group(2)).matches("v\\d+(?:\\.\\d+){1,2}");
    }

    assertThat(usesLines).isNotEmpty();
    assertThat(matchedReferences).isEqualTo(usesLines.size());
  }

  @Test
  void preservesBuildTriggerPermissionAndMatrixContracts() throws IOException {
    String workflow = readWorkflow();
    long permissionsBlocks =
        workflow.lines().filter(line -> line.trim().equals("permissions:")).count();
    int permissionsStart = workflow.indexOf("\npermissions:\n") + 1;
    int jobsStart = workflow.indexOf("\njobs:\n", permissionsStart) + 1;

    assertThat(workflow)
        .contains("on:\n  pull_request:\n  push:\n    branches: [master]")
        .contains("spark: [\"3.5.0\", \"3.5.9\"]")
        .contains("doris: [\"3.0.6.2\", \"4.0.6\"]")
        .contains("needs: [unit, spark-compatibility]")
        .contains(":distribution:verifyDistributionDependencyContract")
        .doesNotContain("permissions: write", "pull-requests: write", "contents: write");
    assertThat(permissionsBlocks).isOne();
    assertThat(permissionsStart).isPositive();
    assertThat(jobsStart).isGreaterThan(permissionsStart);
    assertThat(workflow.substring(permissionsStart, jobsStart).trim())
        .isEqualTo("permissions:\n  contents: read");
  }

  private String readWorkflow() throws IOException {
    String repositoryRoot = System.getProperty("connector.repository.root");
    assertThat(repositoryRoot)
        .as("connector.repository.root must identify the repository under test")
        .isNotBlank();
    return Files.readString(Path.of(repositoryRoot).resolve(".github/workflows/build.yml"));
  }
}
