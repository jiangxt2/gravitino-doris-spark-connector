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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.util.List;
import java.util.Map;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.types.Types;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

@SuppressWarnings("deprecation")
public class TestDorisHybridTable35 {

  private static final Identifier IDENTIFIER = Identifier.of(new String[] {"analytics"}, "events");
  private static final StructType SCHEMA = new StructType().add("id", DataTypes.IntegerType, false);
  private static final DorisReadSchema READ_SCHEMA =
      new DorisReadSchema(SCHEMA, List.of("`id`"), false);

  @Test
  void readConstructionNeverLeaksTheStructuralWriteInterface() {
    DorisHybridTable35 table = readTable();
    LogicalWriteInfo writeInfo = mock(LogicalWriteInfo.class);

    assertThat(table.capabilities()).containsExactly(TableCapability.BATCH_READ);
    assertThatThrownBy(() -> table.newWriteBuilder(writeInfo))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("read-only");
  }

  @Test
  void governedConstructionValidatesSchemaBeforeDelegatingBatchWrite() {
    DorisHybridTable35 readTable = readTable();
    org.apache.gravitino.rel.Table logicalTable = mock(org.apache.gravitino.rel.Table.class);
    when(logicalTable.columns())
        .thenReturn(
            new Column[] {
              Column.of(
                  "id", Types.IntegerType.get(), null, false, false, Column.DEFAULT_VALUE_NOT_SET)
            });
    Table physicalTable = mock(Table.class, withSettings().extraInterfaces(SupportsWrite.class));
    WriteBuilder delegateBuilder = mock(WriteBuilder.class);
    BatchWrite batchWrite = mock(BatchWrite.class);
    LogicalWriteInfo writeInfo = mock(LogicalWriteInfo.class);
    when(writeInfo.schema()).thenReturn(SCHEMA);
    when(((SupportsWrite) physicalTable).newWriteBuilder(writeInfo)).thenReturn(delegateBuilder);
    when(delegateBuilder.buildForBatch()).thenReturn(batchWrite);
    DorisWritePolicy policy =
        DorisWritePolicy.from(Map.of(DorisConnectorConstants.WRITE_MODE, "batch"));

    DorisHybridTable35 writable =
        readTable.withGovernedWrite(
            new DorisAuthorizedTableContext(
                IDENTIFIER,
                logicalTable,
                mock(TableCatalog.class),
                physicalTable,
                readTable,
                READ_SCHEMA,
                connectionInfo(),
                DorisJdbcReadOptions.from(Map.of()),
                policy));

    if (!DorisCatalogClassResolver.supportsWriteAwareLoad()) {
      assertThat(writable.capabilities()).containsExactly(TableCapability.BATCH_READ);
      assertThatThrownBy(() -> writable.newWriteBuilder(writeInfo))
          .isInstanceOf(UnsupportedOperationException.class)
          .hasMessageContaining("read-only");
      verify((SupportsWrite) physicalTable, never()).newWriteBuilder(writeInfo);
      return;
    }

    assertThat(writable.capabilities())
        .contains(TableCapability.BATCH_READ, TableCapability.BATCH_WRITE)
        .doesNotContain(
            TableCapability.STREAMING_WRITE,
            TableCapability.OVERWRITE_BY_FILTER,
            TableCapability.OVERWRITE_DYNAMIC);
    WriteBuilder governedBuilder = writable.newWriteBuilder(writeInfo);
    assertThat(governedBuilder).isInstanceOf(GovernedDorisWriteBuilder35.class);
    assertThat(governedBuilder.buildForBatch()).isNotSameAs(batchWrite);
    verify((SupportsWrite) physicalTable).newWriteBuilder(writeInfo);

    LogicalWriteInfo incompatible = mock(LogicalWriteInfo.class);
    when(incompatible.schema()).thenReturn(new StructType().add("other", DataTypes.IntegerType));
    assertThatThrownBy(() -> writable.newWriteBuilder(incompatible))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("schema is incompatible");
    verify((SupportsWrite) physicalTable, never()).newWriteBuilder(incompatible);
  }

  private static DorisHybridTable35 readTable() {
    Table nativeTable = mock(Table.class, withSettings().extraInterfaces(SupportsRead.class));
    Table sqlTable = mock(Table.class, withSettings().extraInterfaces(SupportsRead.class));
    when(((SupportsRead) nativeTable).newScanBuilder(org.mockito.ArgumentMatchers.any()))
        .thenReturn(mock(ScanBuilder.class));
    when(((SupportsRead) sqlTable).newScanBuilder(org.mockito.ArgumentMatchers.any()))
        .thenReturn(mock(ScanBuilder.class));
    return new DorisHybridTable35(nativeTable, sqlTable, IDENTIFIER, READ_SCHEMA);
  }

  private static DorisJdbcConnectionInfo connectionInfo() {
    return new DorisJdbcConnectionInfo(
        "jdbc:mysql://fe:9030/", "com.mysql.cj.jdbc.Driver", "reader", "test-password");
  }
}
