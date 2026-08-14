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
import java.util.Map;
import java.util.Set;
import org.apache.doris.spark.config.DorisConfig;
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

/** Spark 3.5 read delegate that selects the native Doris lane or the JDBC SQL lane per scan. */
final class DorisHybridTable35 implements Table, SupportsRead, SupportsWrite {

  private final Identifier identifier;
  private final DorisReadSchema readSchema;
  private final SupportsRead nativeReadDelegate;
  private final SupportsRead sqlReadDelegate;
  private final SupportsWrite writeDelegate;
  private final org.apache.gravitino.rel.Table logicalTable;
  private final DorisWritePolicy writePolicy;
  private final DorisConfig dorisConfig;
  private final boolean arrowPreferred;
  private final String endpointIdentity;
  private final List<String> frontendHosts;

  DorisHybridTable35(
      Table nativeTable, Table sqlTable, Identifier identifier, DorisReadSchema readSchema) {
    this(nativeTable, sqlTable, identifier, readSchema, null, false, "", List.of());
  }

  DorisHybridTable35(
      Table nativeTable,
      Table sqlTable,
      Identifier identifier,
      DorisReadSchema readSchema,
      DorisConfig dorisConfig,
      boolean arrowPreferred,
      String endpointIdentity,
      List<String> frontendHosts) {
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
    this.writeDelegate = null;
    this.logicalTable = null;
    this.writePolicy = DorisWritePolicy.disabled();
    this.dorisConfig = dorisConfig;
    this.arrowPreferred = arrowPreferred;
    this.endpointIdentity = endpointIdentity;
    this.frontendHosts = List.copyOf(frontendHosts);
  }

  private DorisHybridTable35(
      DorisHybridTable35 source,
      SupportsWrite writeDelegate,
      org.apache.gravitino.rel.Table logicalTable,
      DorisWritePolicy writePolicy) {
    this.identifier = source.identifier;
    this.readSchema = source.readSchema;
    this.nativeReadDelegate = source.nativeReadDelegate;
    this.sqlReadDelegate = source.sqlReadDelegate;
    this.writeDelegate = writeDelegate;
    this.logicalTable = logicalTable;
    this.writePolicy = writePolicy;
    this.dorisConfig = source.dorisConfig;
    this.arrowPreferred = source.arrowPreferred;
    this.endpointIdentity = source.endpointIdentity;
    this.frontendHosts = source.frontendHosts;
  }

  DorisHybridTable35 withGovernedWrite(DorisAuthorizedTableContext context) {
    DorisChecks.checkArgument(
        context.physicalTable() instanceof SupportsWrite,
        "Physical Doris table for %s does not support writes",
        identifier);
    DorisChecks.checkArgument(
        context.writePolicy().enabled(),
        "Governed Doris write policy is disabled for %s",
        identifier);
    return new DorisHybridTable35(
        this,
        (SupportsWrite) context.physicalTable(),
        context.gravitinoTable(),
        context.writePolicy());
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
    return DorisCapabilityPolicy.from(writePolicy).tableCapabilities();
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    return new DorisHybridScanBuilder35(
        nativeReadDelegate.newScanBuilder(options),
        sqlReadDelegate.newScanBuilder(options),
        readSchema.requiresSqlExecution(),
        readSchema.normalizedColumns(),
        dorisConfig,
        arrowPreferred,
        endpointIdentity,
        frontendHosts);
  }

  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
    if (!DorisCatalogClassResolver.supportsWriteAwareLoad()
        || !writePolicy.enabled()
        || writeDelegate == null
        || logicalTable == null) {
      throw DorisCapabilityPolicy.readOnly().reject("table writes");
    }
    DorisWriteSchemaCompatibility.Validator validator =
        DorisWriteSchemaCompatibility.validate(logicalTable, readSchema, info.schema());
    return new GovernedDorisWriteBuilder35(
        writeDelegate.newWriteBuilder(info), writePolicy, validator);
  }
}
