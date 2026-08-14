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
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.types.Types;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.SupportsTruncate;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;

@SuppressWarnings("deprecation")
public class TestGovernedDorisWriteBuilder35 {

  @Test
  void delegatesOnlyBatchAndExplicitTruncate() {
    WriteBuilder delegate =
        mock(WriteBuilder.class, withSettings().extraInterfaces(SupportsTruncate.class));
    BatchWrite batch = mock(BatchWrite.class);
    when(delegate.buildForBatch()).thenReturn(batch);
    when(((SupportsTruncate) delegate).truncate()).thenReturn(delegate);
    DorisWritePolicy policy =
        DorisWritePolicy.from(
            Map.of(
                DorisConnectorConstants.WRITE_MODE,
                "batch",
                DorisConnectorConstants.WRITE_OVERWRITE_MODE,
                "truncate"));
    GovernedDorisWriteBuilder35 builder = new GovernedDorisWriteBuilder35(delegate, policy);

    assertThat(builder.truncate()).isSameAs(builder);
    assertThat(builder.buildForBatch()).isNotSameAs(batch);
    assertThatThrownBy(builder::buildForStreaming)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("streaming");
    verify((SupportsTruncate) delegate).truncate();
    verify(delegate).buildForBatch();
  }

  @Test
  void validatesDatetimeBeforeDelegatingRows() throws Exception {
    WriteBuilder delegate = mock(WriteBuilder.class);
    BatchWrite batch = mock(BatchWrite.class);
    DataWriterFactory factory = mock(DataWriterFactory.class);
    @SuppressWarnings("unchecked")
    DataWriter<org.apache.spark.sql.catalyst.InternalRow> writer = mock(DataWriter.class);
    PhysicalWriteInfo info = mock(PhysicalWriteInfo.class);
    when(delegate.buildForBatch()).thenReturn(batch);
    when(batch.createBatchWriterFactory(info)).thenReturn(factory);
    when(factory.createWriter(0, 1L)).thenReturn(writer);
    Table logicalTable = mock(Table.class);
    when(logicalTable.columns())
        .thenReturn(
            new Column[] {
              Column.of(
                  "event_time",
                  Types.TimestampType.withoutTimeZone(3),
                  null,
                  true,
                  false,
                  Column.DEFAULT_VALUE_NOT_SET)
            });
    StructType stringSchema = new StructType().add("event_time", DataTypes.StringType, true);
    DorisWriteSchemaCompatibility.Validator validator =
        DorisWriteSchemaCompatibility.validate(
            logicalTable,
            new DorisReadSchema(stringSchema, List.of("`event_time`"), true, Set.of("event_time")),
            stringSchema);
    GovernedDorisWriteBuilder35 builder =
        new GovernedDorisWriteBuilder35(
            delegate,
            DorisWritePolicy.from(Map.of(DorisConnectorConstants.WRITE_MODE, "batch")),
            validator);
    DataWriter<org.apache.spark.sql.catalyst.InternalRow> governedWriter =
        builder.buildForBatch().createBatchWriterFactory(info).createWriter(0, 1L);
    GenericInternalRow valid =
        new GenericInternalRow(new Object[] {UTF8String.fromString("2026-08-14 00:00:00.123")});
    governedWriter.write(valid);
    verify(writer).write(valid);

    GenericInternalRow invalid =
        new GenericInternalRow(new Object[] {UTF8String.fromString("2026-08-14 00:00:00.12")});
    assertThatThrownBy(() -> governedWriter.write(invalid))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageNotContaining("2026-08-14");
    verify(writer, never()).write(invalid);
  }

  @Test
  void defaultOverwritePolicyRejectsBeforeDelegateMutation() {
    WriteBuilder delegate =
        mock(WriteBuilder.class, withSettings().extraInterfaces(SupportsTruncate.class));
    DorisWritePolicy policy =
        DorisWritePolicy.from(Map.of(DorisConnectorConstants.WRITE_MODE, "batch"));
    GovernedDorisWriteBuilder35 builder = new GovernedDorisWriteBuilder35(delegate, policy);

    assertThatThrownBy(builder::truncate).isInstanceOf(UnsupportedOperationException.class);
    verifyNoInteractions(delegate);
  }
}
