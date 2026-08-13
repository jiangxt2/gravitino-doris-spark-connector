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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.spark.connector.ConnectorConstants;
import org.apache.gravitino.spark.connector.SparkTransformConverter;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;

public class TestGovernedDorisTable {

  private static final Identifier IDENTIFIER = Identifier.of(new String[] {"Analytics"}, "Events");

  @Test
  @SuppressWarnings("deprecation")
  void testReadOnlyCapabilitiesMetadataAndScanDelegation() {
    Table gravitinoTable = mock(Table.class);
    when(gravitinoTable.name()).thenReturn("Events");
    when(gravitinoTable.comment()).thenReturn("governed table");
    when(gravitinoTable.properties())
        .thenReturn(ImmutableMap.of("replication_num", "1", "jdbc-password", "secret"));

    org.apache.spark.sql.connector.catalog.Table delegate = readDelegate();
    SupportsRead readDelegate = (SupportsRead) delegate;
    ScanBuilder scanBuilder =
        mock(
            ScanBuilder.class,
            withSettings()
                .extraInterfaces(
                    org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns.class,
                    org.apache.spark.sql.connector.read.SupportsPushDownV2Filters.class,
                    org.apache.spark.sql.connector.read.SupportsPushDownLimit.class,
                    org.apache.spark.sql.connector.read.SupportsPushDownAggregates.class,
                    org.apache.spark.sql.connector.read.SupportsPushDownTopN.class));
    CaseInsensitiveStringMap options =
        new CaseInsensitiveStringMap(ImmutableMap.of("doris.request.retries", "2"));
    when(readDelegate.newScanBuilder(same(options))).thenReturn(scanBuilder);

    StructType schema =
        DataTypes.createStructType(
            new org.apache.spark.sql.types.StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, true)
            });
    GovernedDorisTable table = wrapper(gravitinoTable, delegate, schema);

    assertEquals("analytics.Events", table.name());
    assertSame(schema, table.schema());
    assertEquals(ImmutableSet.of(TableCapability.BATCH_READ), table.capabilities());
    assertInstanceOf(SupportsWrite.class, table);
    assertEquals("1", table.properties().get("replication_num"));
    assertEquals("governed table", table.properties().get(ConnectorConstants.COMMENT));
    assertFalse(table.properties().containsKey("jdbc-password"));
    assertTrue(table.newScanBuilder(options) instanceof GovernedDorisScanBuilder);
    assertThrows(
        UnsupportedOperationException.class,
        () -> table.newWriteBuilder(mock(LogicalWriteInfo.class)));
    verify(readDelegate).newScanBuilder(options);
  }

  @Test
  void testFutureWritePolicyDelegatesWithoutChangingTheFacade() {
    Table gravitinoTable = mock(Table.class);
    when(gravitinoTable.name()).thenReturn("Events");
    when(gravitinoTable.properties()).thenReturn(ImmutableMap.of());
    org.apache.spark.sql.connector.catalog.Table delegate =
        mock(
            org.apache.spark.sql.connector.catalog.Table.class,
            withSettings().extraInterfaces(SupportsRead.class, SupportsWrite.class));
    when(delegate.capabilities())
        .thenReturn(ImmutableSet.of(TableCapability.BATCH_READ, TableCapability.BATCH_WRITE));
    WriteBuilder writeBuilder = mock(WriteBuilder.class);
    LogicalWriteInfo info = mock(LogicalWriteInfo.class);
    when(((SupportsWrite) delegate).newWriteBuilder(info)).thenReturn(writeBuilder);

    GovernedDorisTable table =
        new GovernedDorisTable(
            IDENTIFIER,
            gravitinoTable,
            delegate,
            new StructType(),
            DorisPropertiesConverter.getInstance(),
            new SparkTransformConverter(false),
            new SparkTypeConverter(),
            DorisCapabilityPolicy.of(
                ImmutableSet.of(TableCapability.BATCH_READ, TableCapability.BATCH_WRITE)));

    assertSame(writeBuilder, table.newWriteBuilder(info));
    verify((SupportsWrite) delegate).newWriteBuilder(info);
  }

  @Test
  void testRejectsUnexpectedDelegateContract() {
    Table gravitinoTable = mock(Table.class);
    when(gravitinoTable.name()).thenReturn("Events");
    when(gravitinoTable.properties()).thenReturn(ImmutableMap.of());

    org.apache.spark.sql.connector.catalog.Table notReadable =
        mock(org.apache.spark.sql.connector.catalog.Table.class);
    assertThrows(
        IllegalArgumentException.class,
        () -> wrapper(gravitinoTable, notReadable, new StructType()));

    org.apache.spark.sql.connector.catalog.Table noBatchRead =
        mock(
            org.apache.spark.sql.connector.catalog.Table.class,
            withSettings().extraInterfaces(SupportsRead.class));
    when(noBatchRead.capabilities()).thenReturn(ImmutableSet.of(TableCapability.BATCH_WRITE));
    assertThrows(
        IllegalArgumentException.class,
        () -> wrapper(gravitinoTable, noBatchRead, new StructType()));
  }

  private static org.apache.spark.sql.connector.catalog.Table readDelegate() {
    org.apache.spark.sql.connector.catalog.Table delegate =
        mock(
            org.apache.spark.sql.connector.catalog.Table.class,
            withSettings().extraInterfaces(SupportsRead.class));
    when(delegate.capabilities())
        .thenReturn(
            ImmutableSet.of(
                TableCapability.BATCH_READ,
                TableCapability.BATCH_WRITE,
                TableCapability.STREAMING_WRITE,
                TableCapability.TRUNCATE));
    ScanBuilder scanBuilder =
        mock(
            ScanBuilder.class,
            withSettings()
                .extraInterfaces(
                    org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns.class,
                    org.apache.spark.sql.connector.read.SupportsPushDownV2Filters.class,
                    org.apache.spark.sql.connector.read.SupportsPushDownLimit.class,
                    org.apache.spark.sql.connector.read.SupportsPushDownAggregates.class,
                    org.apache.spark.sql.connector.read.SupportsPushDownTopN.class));
    when(((SupportsRead) delegate).newScanBuilder(any())).thenReturn(scanBuilder);
    return delegate;
  }

  private static GovernedDorisTable wrapper(
      Table gravitinoTable,
      org.apache.spark.sql.connector.catalog.Table delegate,
      StructType schema) {
    return new GovernedDorisTable(
        IDENTIFIER,
        gravitinoTable,
        delegate,
        schema,
        DorisPropertiesConverter.getInstance(),
        new SparkTransformConverter(false),
        new SparkTypeConverter(),
        DorisCapabilityPolicy.readOnly());
  }
}
