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

import java.util.List;
import org.apache.doris.spark.config.DorisConfig;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;

/** Serializable factory for partition-local Arrow-first readers. */
final class DorisArrowFallbackPartitionReaderFactory35 implements PartitionReaderFactory {

  private static final long serialVersionUID = 1L;

  private final StructType schema;
  private final DorisConfig config;
  private final String endpointIdentity;
  private final List<String> frontendHosts;

  DorisArrowFallbackPartitionReaderFactory35(
      StructType schema, DorisConfig config, String endpointIdentity, List<String> frontendHosts) {
    this.schema = schema;
    this.config = config;
    this.endpointIdentity = endpointIdentity;
    this.frontendHosts = List.copyOf(frontendHosts);
  }

  @Override
  public PartitionReader<InternalRow> createReader(InputPartition partition) {
    String portValue = config.toMap().get(DorisConnectorConstants.DORIS_ARROW_FLIGHT_SQL_PORT);
    DorisChecks.checkState(portValue != null, "Doris Arrow Flight SQL port is missing");
    int port;
    try {
      port = Integer.parseInt(portValue);
    } catch (NumberFormatException e) {
      throw new IllegalStateException("Doris Arrow Flight SQL port is invalid");
    }
    return new DorisArrowFallbackPartitionReader35(
        partition, schema, config, endpointIdentity, frontendHosts, port);
  }
}
