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

import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/** Spark 3.5 read delegate that selects the native Doris lane or the JDBC SQL lane per scan. */
final class DorisHybridTable35 implements Table, SupportsRead {

  private final Identifier identifier;
  private final DorisReadSchema readSchema;
  private final SupportsRead nativeReadDelegate;
  private final SupportsRead sqlReadDelegate;

  DorisHybridTable35(
      Table nativeTable, Table sqlTable, Identifier identifier, DorisReadSchema readSchema) {
    DorisChecks.checkArgument(
        nativeTable instanceof SupportsRead,
        "Native Doris table for %s does not support reads",
        identifier);
    DorisChecks.checkArgument(
        sqlTable instanceof SupportsRead,
        "Doris SQL table for %s does not support reads",
        identifier);
    this.identifier = identifier;
    this.readSchema = readSchema;
    this.nativeReadDelegate = (SupportsRead) nativeTable;
    this.sqlReadDelegate = (SupportsRead) sqlTable;
  }

  @Override
  public String name() {
    return identifier.toString();
  }

  @Override
  @SuppressWarnings("deprecation")
  public StructType schema() {
    return readSchema.schema();
  }

  @Override
  public Map<String, String> properties() {
    return Map.of();
  }

  @Override
  public Set<TableCapability> capabilities() {
    return DorisCapabilityPolicy.readOnly().tableCapabilities();
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    return new DorisHybridScanBuilder35(
        nativeReadDelegate.newScanBuilder(options),
        sqlReadDelegate.newScanBuilder(options),
        readSchema.requiresSqlExecution(),
        readSchema.normalizedColumns());
  }
}
