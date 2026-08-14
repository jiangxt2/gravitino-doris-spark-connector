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

import java.util.Map;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.junit.jupiter.api.Test;

public class TestDorisCapabilityPolicy {

  @Test
  void exposesOnlyCertifiedCapabilities() {
    DorisWritePolicy append =
        DorisWritePolicy.from(Map.of(DorisConnectorConstants.WRITE_MODE, "batch"));
    assertThat(DorisCapabilityPolicy.from(append, false).tableCapabilities())
        .containsExactly(TableCapability.BATCH_READ);
    assertThat(DorisCapabilityPolicy.from(append, true).tableCapabilities())
        .containsExactlyInAnyOrder(TableCapability.BATCH_READ, TableCapability.BATCH_WRITE);

    DorisWritePolicy truncate =
        DorisWritePolicy.from(
            Map.of(
                DorisConnectorConstants.WRITE_MODE,
                "batch",
                DorisConnectorConstants.WRITE_OVERWRITE_MODE,
                "truncate"));
    assertThat(DorisCapabilityPolicy.from(truncate, true).tableCapabilities())
        .containsExactlyInAnyOrder(
            TableCapability.BATCH_READ, TableCapability.BATCH_WRITE, TableCapability.TRUNCATE)
        .doesNotContain(TableCapability.STREAMING_WRITE, TableCapability.OVERWRITE_BY_FILTER);
  }
}
