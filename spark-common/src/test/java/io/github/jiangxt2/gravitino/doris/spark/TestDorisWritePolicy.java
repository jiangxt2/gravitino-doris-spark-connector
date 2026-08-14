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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.junit.jupiter.api.Test;

public class TestDorisWritePolicy {

  @Test
  void defaultsToDisabledRejectMode() {
    DorisWritePolicy policy = DorisWritePolicy.from(Map.of());
    assertThat(policy.enabled()).isFalse();
    assertThat(policy.allowsTruncate()).isFalse();
    assertThat(policy.forcedConnectorOptions()).isEmpty();
  }

  @Test
  void forcesTheReviewedStreamLoadContract() {
    DorisWritePolicy policy =
        DorisWritePolicy.from(Map.of(DorisConnectorConstants.WRITE_MODE, "batch"));
    assertThat(policy.enabled()).isTrue();
    assertThat(policy.allowsTruncate()).isFalse();
    assertThat(policy.forcedConnectorOptions())
        .containsExactlyInAnyOrderEntriesOf(
            Map.of(
                "doris.sink.mode",
                "stream_load",
                "doris.sink.auto-redirect",
                "false",
                "doris.sink.enable-2pc",
                "true",
                "doris.sink.properties.strict_mode",
                "true",
                "doris.max.filter.ratio",
                "0",
                "doris.write.schemaless",
                "false"));
  }

  @Test
  void enablesTruncateOnlyWithBatchAndRejectsStrictTransport() {
    DorisWritePolicy truncate =
        DorisWritePolicy.from(
            Map.of(
                DorisConnectorConstants.WRITE_MODE,
                "batch",
                DorisConnectorConstants.WRITE_OVERWRITE_MODE,
                "truncate"));
    assertThat(truncate.allowsTruncate()).isTrue();

    assertThatThrownBy(
            () ->
                DorisWritePolicy.from(
                    Map.of(DorisConnectorConstants.WRITE_OVERWRITE_MODE, "truncate")))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () ->
                DorisWritePolicy.from(
                    Map.of(
                        DorisConnectorConstants.WRITE_MODE,
                        "batch",
                        DorisConnectorConstants.READ_TRANSPORT,
                        DorisConnectorConstants.STRICT_JDBC_TLS_TRANSPORT)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Strict JDBC TLS");
  }
}
