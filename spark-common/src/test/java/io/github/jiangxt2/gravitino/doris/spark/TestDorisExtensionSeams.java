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

import com.google.common.collect.ImmutableSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

/** Contract tests for the stable mutation and write extension seams. */
public class TestDorisExtensionSeams {

  @Test
  void readOnlyPolicyAndMutationDelegateShareOneCapabilityDecision() {
    DorisCapabilityPolicy policy = DorisCapabilityPolicy.readOnly();
    DorisCatalogMutationDelegate mutations = DorisCatalogMutationDelegate.readOnly(policy);
    Identifier identifier = Identifier.of(new String[] {"analytics"}, "events");

    assertThat(policy.tableCapabilities()).containsExactly(TableCapability.BATCH_READ);
    assertThatThrownBy(
            () -> mutations.createTable(identifier, new StructType(), new Transform[0], Map.of()))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("read-only")
        .hasMessageContaining("create table");
    assertThatThrownBy(() -> mutations.dropTable(identifier))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("drop table");
  }

  @Test
  void writeFactoryReceivesCompleteAuthorizedContextAndDefaultsToIdentity() {
    Identifier identifier = Identifier.of(new String[] {"analytics"}, "events");
    org.apache.gravitino.rel.Table gravitinoTable = mock(org.apache.gravitino.rel.Table.class);
    TableCatalog physicalCatalog = mock(TableCatalog.class);
    Table physicalTable = mock(Table.class);
    Table readDelegate = mock(Table.class);
    Table futureWriteDelegate = mock(Table.class);
    StructType schema = new StructType();
    DorisJdbcConnectionInfo connectionInfo =
        new DorisJdbcConnectionInfo(
            "jdbc:mysql://fe:9030/db", "com.mysql.cj.jdbc.Driver", "user", "secret");
    DorisJdbcReadOptions readOptions = DorisJdbcReadOptions.from(Map.of());
    DorisAuthorizedTableContext context =
        new DorisAuthorizedTableContext(
            identifier,
            gravitinoTable,
            physicalCatalog,
            physicalTable,
            readDelegate,
            schema,
            connectionInfo,
            readOptions);

    assertThat(DorisWriteDelegateFactory.readOnly().create(context)).isSameAs(readDelegate);

    AtomicReference<DorisAuthorizedTableContext> received = new AtomicReference<>();
    DorisWriteDelegateFactory factory =
        authorized -> {
          received.set(authorized);
          return futureWriteDelegate;
        };

    assertThat(factory.create(context)).isSameAs(futureWriteDelegate);
    assertThat(received.get().identifier()).isEqualTo(identifier);
    assertThat(received.get().physicalTable()).isSameAs(physicalTable);
    assertThat(received.get().readDelegate()).isSameAs(readDelegate);
    assertThat(received.get().validatedSchema()).isSameAs(schema);
    assertThat(received.get().toString()).doesNotContain("secret");
  }

  @Test
  void futureCapabilityPolicyCanBeAddedWithoutChangingTheFacadeContract() {
    DorisCapabilityPolicy policy =
        DorisCapabilityPolicy.of(
            ImmutableSet.of(TableCapability.BATCH_READ, TableCapability.BATCH_WRITE));

    assertThat(policy.tableCapabilities())
        .containsExactlyInAnyOrder(TableCapability.BATCH_READ, TableCapability.BATCH_WRITE);
    assertThat(policy.allowsTableWrites()).isTrue();
    assertThat(DorisCapabilityPolicy.readOnly().allowsTableWrites()).isFalse();
    assertThatThrownBy(() -> DorisCapabilityPolicy.of(ImmutableSet.of(TableCapability.BATCH_WRITE)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("BATCH_READ");
  }
}
