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
import java.util.Objects;
import org.apache.doris.spark.config.DorisConfig;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;

/** Preserves the official Doris scan plan while replacing only its native reader factory. */
final class DorisArrowFallbackScan35 implements Scan, Batch {

  private final Scan delegate;
  private final Batch delegateBatch;
  private final DorisConfig config;
  private final String endpointIdentity;
  private final List<String> frontendHosts;

  DorisArrowFallbackScan35(
      Scan delegate, DorisConfig config, String endpointIdentity, List<String> frontendHosts) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    this.delegateBatch = delegate.toBatch();
    this.config = Objects.requireNonNull(config, "config");
    DorisChecks.checkArgument(
        endpointIdentity != null && !endpointIdentity.isEmpty(),
        "Doris Arrow endpoint identity is required");
    DorisChecks.checkArgument(
        frontendHosts != null && !frontendHosts.isEmpty(), "Doris Arrow FE hosts are required");
    this.endpointIdentity = endpointIdentity;
    this.frontendHosts = List.copyOf(frontendHosts);
  }

  @Override
  public StructType readSchema() {
    return delegate.readSchema();
  }

  @Override
  public String description() {
    return delegate.description();
  }

  @Override
  public Batch toBatch() {
    return this;
  }

  @Override
  public InputPartition[] planInputPartitions() {
    return delegateBatch.planInputPartitions();
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new DorisArrowFallbackPartitionReaderFactory35(
        readSchema(), config, endpointIdentity, frontendHosts);
  }
}
