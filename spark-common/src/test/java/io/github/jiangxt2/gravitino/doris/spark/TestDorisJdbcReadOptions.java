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

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

public class TestDorisJdbcReadOptions {

  @Test
  void parsesSparkJdbcPartitioningAndFetchSize() {
    DorisJdbcReadOptions options =
        DorisJdbcReadOptions.from(
            ImmutableMap.<String, String>builder()
                .put(DorisConnectorConstants.JDBC_PARTITION_COLUMN, " id ")
                .put(DorisConnectorConstants.JDBC_LOWER_BOUND, "0")
                .put(DorisConnectorConstants.JDBC_UPPER_BOUND, "100000")
                .put(DorisConnectorConstants.JDBC_NUM_PARTITIONS, "8")
                .put(DorisConnectorConstants.JDBC_FETCH_SIZE, "4096")
                .build());

    assertThat(options.asSparkOptions())
        .containsExactly(
            org.assertj.core.data.MapEntry.entry("partitionColumn", "id"),
            org.assertj.core.data.MapEntry.entry("lowerBound", "0"),
            org.assertj.core.data.MapEntry.entry("upperBound", "100000"),
            org.assertj.core.data.MapEntry.entry("numPartitions", "8"),
            org.assertj.core.data.MapEntry.entry("fetchsize", "4096"));
  }

  @Test
  void requiresTheCompletePartitionTuple() {
    assertThatThrownBy(
            () ->
                DorisJdbcReadOptions.from(
                    ImmutableMap.of(DorisConnectorConstants.JDBC_PARTITION_COLUMN, "id")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("requires partition column");
  }

  @Test
  void rejectsNonPositiveNumericOptions() {
    assertThatThrownBy(
            () ->
                DorisJdbcReadOptions.from(
                    ImmutableMap.of(DorisConnectorConstants.JDBC_FETCH_SIZE, "0")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(DorisConnectorConstants.JDBC_FETCH_SIZE);
  }
}
