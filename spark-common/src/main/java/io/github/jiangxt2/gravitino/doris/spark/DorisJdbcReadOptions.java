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

import static io.github.jiangxt2.gravitino.doris.spark.DorisConnectorConstants.JDBC_FETCH_SIZE;
import static io.github.jiangxt2.gravitino.doris.spark.DorisConnectorConstants.JDBC_LOWER_BOUND;
import static io.github.jiangxt2.gravitino.doris.spark.DorisConnectorConstants.JDBC_NUM_PARTITIONS;
import static io.github.jiangxt2.gravitino.doris.spark.DorisConnectorConstants.JDBC_PARTITION_COLUMN;
import static io.github.jiangxt2.gravitino.doris.spark.DorisConnectorConstants.JDBC_UPPER_BOUND;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Validated Spark JDBC tuning options for the SQL normalization lane. */
public final class DorisJdbcReadOptions {

  private final Map<String, String> sparkOptions;

  private DorisJdbcReadOptions(Map<String, String> sparkOptions) {
    this.sparkOptions = Collections.unmodifiableMap(new LinkedHashMap<>(sparkOptions));
  }

  /**
   * Parses catalog properties using Spark JDBC's standard partitioning contract.
   *
   * @param properties visible Gravitino catalog properties
   * @return validated JDBC read options
   */
  public static DorisJdbcReadOptions from(Map<String, String> properties) {
    Map<String, String> source = properties == null ? Collections.emptyMap() : properties;
    String partitionColumn = trimToNull(source.get(JDBC_PARTITION_COLUMN));
    String lowerBound = trimToNull(source.get(JDBC_LOWER_BOUND));
    String upperBound = trimToNull(source.get(JDBC_UPPER_BOUND));
    String numPartitions = trimToNull(source.get(JDBC_NUM_PARTITIONS));

    int partitionOptionCount = countPresent(partitionColumn, lowerBound, upperBound, numPartitions);
    if (partitionOptionCount != 0 && partitionOptionCount != 4) {
      throw new IllegalArgumentException(
          "Doris JDBC partitioning requires partition column, lower bound, upper bound, and "
              + "number of partitions together");
    }

    Map<String, String> result = new LinkedHashMap<>();
    if (partitionOptionCount == 4) {
      requirePositiveInteger(JDBC_NUM_PARTITIONS, numPartitions);
      result.put("partitionColumn", partitionColumn);
      result.put("lowerBound", lowerBound);
      result.put("upperBound", upperBound);
      result.put("numPartitions", numPartitions);
    }

    String fetchSize = trimToNull(source.get(JDBC_FETCH_SIZE));
    if (fetchSize != null) {
      requirePositiveInteger(JDBC_FETCH_SIZE, fetchSize);
      result.put("fetchsize", fetchSize);
    }
    return new DorisJdbcReadOptions(result);
  }

  /** Returns Spark JDBC option names and values without connection credentials. */
  public Map<String, String> asSparkOptions() {
    return sparkOptions;
  }

  private static int countPresent(String... values) {
    int count = 0;
    for (String value : values) {
      if (value != null) {
        count++;
      }
    }
    return count;
  }

  private static String trimToNull(String value) {
    if (value == null || value.trim().isEmpty()) {
      return null;
    }
    return value.trim();
  }

  private static void requirePositiveInteger(String property, String value) {
    try {
      if (Integer.parseInt(value) < 1) {
        throw new NumberFormatException("not positive");
      }
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(property + " must be a positive integer");
    }
  }
}
