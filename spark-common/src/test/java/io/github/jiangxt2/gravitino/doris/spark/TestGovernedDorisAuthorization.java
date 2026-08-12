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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.gravitino.spark.connector.SparkTransformConverter;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.gravitino.spark.connector.catalog.GravitinoCatalogManager;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class TestGovernedDorisAuthorization {

  private GravitinoClient client;
  private Catalog catalog;
  private org.apache.gravitino.rel.TableCatalog gravitinoTables;
  private TableCatalog physicalCatalog;
  private TestCatalog adapter;

  @BeforeEach
  void setUp() {
    client = mock(GravitinoClient.class);
    catalog = mock(Catalog.class);
    gravitinoTables = mock(org.apache.gravitino.rel.TableCatalog.class);
    physicalCatalog = mock(TableCatalog.class);
    when(client.loadCatalog("doris")).thenReturn(catalog);
    when(catalog.type()).thenReturn(Catalog.Type.RELATIONAL);
    when(catalog.provider()).thenReturn(DorisConnectorConstants.PROVIDER);
    when(catalog.properties()).thenReturn(ImmutableMap.of());
    when(catalog.asTableCatalog()).thenReturn(gravitinoTables);
    GravitinoCatalogManager.create(() -> client);
    adapter = new TestCatalog(physicalCatalog);
    adapter.initialize("doris", new CaseInsensitiveStringMap(ImmutableMap.of()));
  }

  @AfterEach
  void tearDown() {
    GravitinoCatalogManager.get().close();
  }

  @Test
  void checksSelectBeforeTouchingThePhysicalCatalog() throws Exception {
    Identifier identifier = Identifier.of(new String[] {"analytics"}, "events");
    org.apache.gravitino.rel.Table logical = mock(org.apache.gravitino.rel.Table.class);
    Table physical = mock(Table.class);
    when(gravitinoTables.loadTable(any(NameIdentifier.class), anySet())).thenReturn(logical);
    when(physicalCatalog.loadTable(identifier)).thenReturn(physical);

    assertThat(adapter.loadTable(identifier)).isSameAs(physical);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Set<Privilege.Name>> privileges = ArgumentCaptor.forClass(Set.class);
    verify(gravitinoTables).loadTable(any(NameIdentifier.class), privileges.capture());
    assertThat(privileges.getValue()).containsExactly(Privilege.Name.SELECT_TABLE);
    verify(physicalCatalog).loadTable(identifier);
  }

  @Test
  void authorizationFailureProducesZeroPhysicalRequests() throws Exception {
    Identifier identifier = Identifier.of(new String[] {"analytics"}, "events");
    when(gravitinoTables.loadTable(any(NameIdentifier.class), anySet()))
        .thenThrow(new ForbiddenException("SELECT_TABLE denied"));

    assertThatThrownBy(() -> adapter.loadTable(identifier))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("SELECT_TABLE");
    verify(physicalCatalog, never()).loadTable(any(Identifier.class));
  }

  @Test
  void missingGovernedTableProducesZeroPhysicalRequests() throws Exception {
    Identifier identifier = Identifier.of(new String[] {"analytics"}, "missing_events");
    when(gravitinoTables.loadTable(any(NameIdentifier.class), anySet()))
        .thenThrow(new org.apache.gravitino.exceptions.NoSuchTableException("missing table"));

    assertThatThrownBy(() -> adapter.loadTable(identifier))
        .isInstanceOf(org.apache.spark.sql.catalyst.analysis.NoSuchTableException.class);
    verify(physicalCatalog, never()).loadTable(any(Identifier.class));
  }

  @Test
  void invalidJdbcMetadataFailsBeforePhysicalCatalogConstruction() {
    SecurityOrderingCatalog securityCatalog = new SecurityOrderingCatalog();
    Map<String, String> properties =
        Map.of(
            DorisConnectorConstants.JDBC_URL,
            "jdbc:mysql://user:secret-canary@fe:9030/",
            DorisConnectorConstants.JDBC_DRIVER,
            "com.mysql.cj.jdbc.Driver");

    assertThatThrownBy(() -> securityCatalog.createPhysicalCatalog(properties))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageNotContaining("secret-canary");
    assertThat(securityCatalog.physicalCatalogCreations).isZero();
  }

  @Test
  void missingExternalJdbcDriverFailsBeforePhysicalCatalogConstruction() {
    SecurityOrderingCatalog securityCatalog = new SecurityOrderingCatalog();
    Map<String, String> properties =
        Map.of(
            DorisConnectorConstants.JDBC_URL,
            "jdbc:mysql://fe:9030/",
            DorisConnectorConstants.JDBC_DRIVER,
            "com.mysql.cj.jdbc.Driver",
            DorisConnectorConstants.GRAVITINO_DORIS_FE_NODES,
            "fe:8030",
            DorisConnectorConstants.GRAVITINO_DORIS_QUERY_PORT,
            "9030");

    assertThatThrownBy(() -> securityCatalog.createPhysicalCatalog(properties))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("com.mysql:mysql-connector-j:8.0.33")
        .hasMessageContaining("Spark driver")
        .hasMessageContaining("every executor")
        .hasMessageNotContaining("jdbc:mysql://fe:9030/");
    assertThat(securityCatalog.physicalCatalogCreations).isZero();
  }

  @Test
  void physicalTableCreationFailureIsSanitizedAfterAuthorization() throws Exception {
    Identifier identifier = Identifier.of(new String[] {"analytics"}, "events");
    org.apache.gravitino.rel.Table logical = mock(org.apache.gravitino.rel.Table.class);
    when(gravitinoTables.loadTable(any(NameIdentifier.class), anySet())).thenReturn(logical);
    when(physicalCatalog.loadTable(identifier))
        .thenThrow(new IllegalArgumentException("third-party-secret"));

    assertThatThrownBy(() -> adapter.loadTable(identifier))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("authorized table")
        .hasMessageNotContaining("third-party-secret");
    verify(physicalCatalog).loadTable(identifier);
  }

  @Test
  void catalogMutationsUseTheReplaceableDelegate() {
    DorisCatalogMutationDelegate mutationDelegate = mock(DorisCatalogMutationDelegate.class);
    TestCatalog catalogWithMutations = new TestCatalog(physicalCatalog, mutationDelegate);
    catalogWithMutations.initialize("doris", new CaseInsensitiveStringMap(ImmutableMap.of()));
    Identifier identifier = Identifier.of(new String[] {"analytics"}, "events");
    when(mutationDelegate.dropTable(identifier)).thenReturn(true);

    assertThat(catalogWithMutations.dropTable(identifier)).isTrue();
    verify(mutationDelegate).dropTable(identifier);
  }

  private static final class TestCatalog extends GovernedDorisCatalog {

    private final TableCatalog delegate;
    private final DorisCatalogMutationDelegate mutationDelegate;

    private TestCatalog(TableCatalog delegate) {
      this(delegate, null);
    }

    private TestCatalog(TableCatalog delegate, DorisCatalogMutationDelegate mutationDelegate) {
      this.delegate = delegate;
      this.mutationDelegate = mutationDelegate;
    }

    @Override
    protected TableCatalog createAndInitSparkCatalog(
        String name, CaseInsensitiveStringMap options, java.util.Map<String, String> properties) {
      return delegate;
    }

    @Override
    protected Table createSparkTable(
        Identifier identifier,
        org.apache.gravitino.rel.Table gravitinoTable,
        Table sparkTable,
        TableCatalog sparkCatalog,
        PropertiesConverter propertiesConverter,
        SparkTransformConverter sparkTransformConverter,
        SparkTypeConverter sparkTypeConverter) {
      return sparkTable;
    }

    @Override
    protected SparkTypeConverter getSparkTypeConverter() {
      return new SparkTypeConverter();
    }

    @Override
    protected TableCatalog createDorisTableCatalog() {
      return delegate;
    }

    @Override
    protected DorisCatalogMutationDelegate getMutationDelegate() {
      return mutationDelegate == null ? super.getMutationDelegate() : mutationDelegate;
    }

    @Override
    protected Table createSchemaSeededDorisTable(
        TableCatalog sparkCatalog,
        Identifier identifier,
        DorisReadSchema readSchema,
        DorisJdbcConnectionInfo connectionInfo,
        DorisJdbcReadOptions readOptions) {
      throw new AssertionError("Not used by this authorization unit test");
    }
  }

  private static final class SecurityOrderingCatalog extends GovernedDorisCatalog {

    private int physicalCatalogCreations;

    private TableCatalog createPhysicalCatalog(Map<String, String> properties) {
      return super.createAndInitSparkCatalog(
          "doris", new CaseInsensitiveStringMap(Map.of()), properties);
    }

    @Override
    protected TableCatalog createDorisTableCatalog() {
      physicalCatalogCreations++;
      throw new AssertionError("Unsafe JDBC metadata reached physical catalog construction");
    }

    @Override
    protected SparkTypeConverter getSparkTypeConverter() {
      throw new AssertionError("Unsafe JDBC metadata reached Spark type conversion");
    }

    @Override
    protected Table createSchemaSeededDorisTable(
        TableCatalog sparkCatalog,
        Identifier identifier,
        DorisReadSchema readSchema,
        DorisJdbcConnectionInfo connectionInfo,
        DorisJdbcReadOptions readOptions) {
      throw new AssertionError("Unsafe JDBC metadata reached reader construction");
    }
  }
}
