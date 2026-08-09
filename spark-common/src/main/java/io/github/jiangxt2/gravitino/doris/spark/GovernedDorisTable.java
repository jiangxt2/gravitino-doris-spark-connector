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
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.gravitino.spark.connector.SparkTransformConverter;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.gravitino.spark.connector.utils.GravitinoTableInfoHelper;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/** A governed, read-only Spark table that delegates scan construction to the Doris Connector. */
public class GovernedDorisTable implements Table, SupportsRead, SupportsWrite {

  private final String name;
  private final StructType schema;
  private final Map<String, String> properties;
  private final SupportsRead readDelegate;
  private final Table tableDelegate;
  private final DorisCapabilityPolicy capabilityPolicy;

  /**
   * Creates a governed read-only wrapper around an official Doris Spark table.
   *
   * @param identifier the authorized Spark table identifier
   * @param gravitinoTable the governed Gravitino table
   * @param delegate the schema-seeded official Doris table
   * @param validatedSchema the cached, validated physical schema
   * @param propertiesConverter the Doris property converter
   * @param transformConverter the Spark transform converter
   * @param typeConverter the Doris Spark type converter
   * @param capabilityPolicy the capabilities explicitly exposed by the governed facade
   */
  public GovernedDorisTable(
      Identifier identifier,
      org.apache.gravitino.rel.Table gravitinoTable,
      Table delegate,
      StructType validatedSchema,
      PropertiesConverter propertiesConverter,
      SparkTransformConverter transformConverter,
      SparkTypeConverter typeConverter,
      DorisCapabilityPolicy capabilityPolicy) {
    DorisChecks.checkArgument(
        delegate instanceof SupportsRead,
        "Doris Connector table for %s does not implement SupportsRead",
        identifier);
    DorisChecks.checkArgument(
        delegate.capabilities().contains(TableCapability.BATCH_READ),
        "Doris Connector table for %s does not support BATCH_READ",
        identifier);
    DorisChecks.checkArgument(
        !capabilityPolicy.allowsTableWrites() || delegate instanceof SupportsWrite,
        "Writable Doris table for %s does not implement SupportsWrite",
        identifier);

    GravitinoTableInfoHelper tableInfoHelper =
        new GravitinoTableInfoHelper(
            false,
            identifier,
            gravitinoTable,
            propertiesConverter,
            transformConverter,
            typeConverter);
    this.name = tableInfoHelper.name();
    this.schema = validatedSchema;
    this.properties = Map.copyOf(tableInfoHelper.properties());
    this.tableDelegate = delegate;
    this.readDelegate = (SupportsRead) delegate;
    this.capabilityPolicy = capabilityPolicy;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  @SuppressWarnings("deprecation")
  public StructType schema() {
    return schema;
  }

  @Override
  public Map<String, String> properties() {
    return properties;
  }

  @Override
  public Set<TableCapability> capabilities() {
    return capabilityPolicy.tableCapabilities();
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    return new GovernedDorisScanBuilder(newDelegateScanBuilder(options));
  }

  /**
   * Delegates a future authorized write while keeping the initial capability set read-only.
   *
   * <p>Spark consults {@link #capabilities()} before this method. The structural implementation is
   * present now so a future release only replaces the policy and authorized table delegate.
   */
  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
    if (!capabilityPolicy.allowsTableWrites()) {
      throw capabilityPolicy.reject("table writes");
    }
    DorisChecks.checkState(
        tableDelegate instanceof SupportsWrite,
        "Writable Doris table delegate does not implement SupportsWrite");
    return ((SupportsWrite) tableDelegate).newWriteBuilder(info);
  }

  /** Returns the version-specific Doris scan builder before the governed wrapper is applied. */
  protected ScanBuilder newDelegateScanBuilder(CaseInsensitiveStringMap options) {
    return readDelegate.newScanBuilder(options);
  }
}
