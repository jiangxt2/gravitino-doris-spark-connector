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

import io.github.jiangxt2.gravitino.doris.security.DorisJdbcSecurity;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.credential.JdbcCredential;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.gravitino.spark.connector.SparkTransformConverter;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.gravitino.spark.connector.jdbc.GravitinoJdbcCatalog;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.NonEmptyNamespaceException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A governed, read-only Spark catalog that delegates Doris IO to the official Connector. */
public abstract class GovernedDorisCatalog extends GravitinoJdbcCatalog {

  private static final Logger LOG = LoggerFactory.getLogger(GovernedDorisCatalog.class);

  /** The official Connector artifact required on Spark driver and executor classpaths. */
  private DorisJdbcConnectionInfo jdbcConnectionInfo;

  private DorisJdbcReadOptions jdbcReadOptions;
  private DorisPhysicalSchemaCache physicalSchemaCache;
  private final DorisCapabilityPolicy defaultCapabilityPolicy = DorisCapabilityPolicy.readOnly();
  private final DorisCatalogMutationDelegate defaultMutationDelegate =
      DorisCatalogMutationDelegate.readOnly(defaultCapabilityPolicy);
  private final DorisWriteDelegateFactory defaultWriteDelegateFactory =
      DorisWriteDelegateFactory.readOnly();

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    org.apache.gravitino.rel.Table gravitinoTable;
    try {
      gravitinoTable =
          gravitinoCatalogClient
              .asTableCatalog()
              .loadTable(
                  NameIdentifier.of(getDatabase(ident), ident.name()),
                  Set.of(Privilege.Name.SELECT_TABLE));
    } catch (org.apache.gravitino.exceptions.NoSuchTableException e) {
      throw new NoSuchTableException(ident);
    }

    // Authorization above is deliberately completed before any physical delegate is touched.
    Table physicalTable;
    try {
      physicalTable = loadSparkTable(ident);
    } catch (LinkageError e) {
      throw missingConnectorDependency(e);
    } catch (RuntimeException e) {
      // The official Connector reconstructs table configuration from an option map that contains
      // the vended credential. Its exception text therefore stays behind this boundary.
      throw new IllegalArgumentException(
          String.format(
              "Failed to create the physical Doris table for authorized table %s",
              qualifiedIdentifier(ident)));
    }
    return createSparkTable(
        ident,
        gravitinoTable,
        physicalTable,
        sparkCatalog,
        propertiesConverter,
        sparkTransformConverter,
        getSparkTypeConverter());
  }

  @Override
  public Table createTable(
      Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws TableAlreadyExistsException, NoSuchNamespaceException {
    return getMutationDelegate().createTable(ident, schema, partitions, properties);
  }

  @Override
  public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
    return getMutationDelegate().alterTable(ident, changes);
  }

  @Override
  public boolean dropTable(Identifier ident) {
    return getMutationDelegate().dropTable(ident);
  }

  @Override
  public boolean purgeTable(Identifier ident) {
    return getMutationDelegate().purgeTable(ident);
  }

  @Override
  public void renameTable(Identifier oldIdent, Identifier newIdent)
      throws NoSuchTableException, TableAlreadyExistsException {
    getMutationDelegate().renameTable(oldIdent, newIdent);
  }

  @Override
  public void createNamespace(String[] namespace, Map<String, String> metadata)
      throws NamespaceAlreadyExistsException {
    getMutationDelegate().createNamespace(namespace, metadata);
  }

  @Override
  public void alterNamespace(String[] namespace, NamespaceChange... changes)
      throws NoSuchNamespaceException {
    getMutationDelegate().alterNamespace(namespace, changes);
  }

  @Override
  public boolean dropNamespace(String[] namespace, boolean cascade)
      throws NoSuchNamespaceException, NonEmptyNamespaceException {
    return getMutationDelegate().dropNamespace(namespace, cascade);
  }

  @Override
  protected TableCatalog createAndInitSparkCatalog(
      String name, CaseInsensitiveStringMap options, Map<String, String> properties) {
    DorisJdbcSecurity.validateConnection(
        properties.get(DorisConnectorConstants.JDBC_URL),
        properties.get(DorisConnectorConstants.JDBC_DRIVER));

    TableCatalog dorisCatalog;
    try {
      dorisCatalog = createDorisTableCatalog();
    } catch (LinkageError e) {
      throw missingConnectorDependency(e);
    }

    Map<String, String> connectorProperties =
        getPropertiesConverter().toSparkCatalogProperties(options, properties);
    JdbcCredential credential = DorisCredentialResolver.resolve(gravitinoCatalogClient);
    connectorProperties.put(DorisConnectorConstants.DORIS_USER, credential.jdbcUser());
    connectorProperties.put(DorisConnectorConstants.DORIS_PASSWORD, credential.jdbcPassword());
    jdbcConnectionInfo =
        new DorisJdbcConnectionInfo(
            properties.get(DorisConnectorConstants.JDBC_URL),
            properties.get(DorisConnectorConstants.JDBC_DRIVER),
            credential.jdbcUser(),
            credential.jdbcPassword());
    jdbcReadOptions = DorisJdbcReadOptions.from(properties);
    physicalSchemaCache = DorisPhysicalSchemaCache.from(properties);

    try {
      dorisCatalog.initialize(name, new CaseInsensitiveStringMap(connectorProperties));
      return dorisCatalog;
    } catch (LinkageError e) {
      throw missingConnectorDependency(e);
    } catch (RuntimeException e) {
      // Third-party configuration exceptions are outside the adapter's credential-redaction
      // boundary and may have access to the complete option map.
      LOG.warn(
          "Native Doris catalog initialization failed for catalog {} with exception type {}",
          name,
          e.getClass().getSimpleName());
      throw new IllegalArgumentException("Failed to initialize the native Doris Spark catalog");
    }
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
    DorisPhysicalSchema physicalSchema;
    try {
      // This is the first and only physical schema request for one successfully authorized
      // BaseCatalog.loadTable result. The version-specific implementation may retain the original
      // FE type names that the Connector's Catalyst schema intentionally normalizes.
      physicalSchema =
          physicalSchemaCache.get(
              identifier, () -> loadPhysicalSchema(sparkCatalog, identifier, sparkTable));
    } catch (LinkageError e) {
      throw missingConnectorDependency(e);
    } catch (RuntimeException e) {
      // Do not retain or log the Connector exception: third-party exception text is outside the
      // adapter's credential-redaction boundary.
      throw new IllegalArgumentException(
          String.format(
              "Failed to load physical Doris schema for authorized table %s",
              qualifiedIdentifier(identifier)));
    }

    DorisReadSchema readSchema =
        DorisSchemaCompatibility.planReadSchema(
            identifier, gravitinoTable, physicalSchema, sparkTypeConverter);
    Table readDelegate;
    try {
      readDelegate =
          createSchemaSeededDorisTable(
              sparkCatalog, identifier, readSchema, jdbcConnectionInfo, jdbcReadOptions);
    } catch (LinkageError e) {
      throw missingConnectorDependency(e);
    } catch (RuntimeException e) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to create the Doris read table for authorized table %s",
              qualifiedIdentifier(identifier)));
    }
    DorisAuthorizedTableContext authorizedContext =
        new DorisAuthorizedTableContext(
            identifier,
            gravitinoTable,
            sparkCatalog,
            sparkTable,
            readDelegate,
            readSchema.schema(),
            jdbcConnectionInfo,
            jdbcReadOptions);
    Table tableDelegate;
    try {
      tableDelegate = getWriteDelegateFactory().create(authorizedContext);
      if (tableDelegate == null) {
        throw new IllegalStateException("Doris table delegate factory returned null");
      }
    } catch (RuntimeException e) {
      // A future write factory receives credential-bearing context, so its exception text must
      // stay behind the same redaction boundary as the read delegate.
      throw new IllegalArgumentException(
          String.format(
              "Failed to create the governed Doris table for authorized table %s",
              qualifiedIdentifier(identifier)));
    }
    return createGovernedDorisTable(
        identifier,
        gravitinoTable,
        tableDelegate,
        readSchema.schema(),
        propertiesConverter,
        sparkTransformConverter,
        sparkTypeConverter);
  }

  @Override
  protected PropertiesConverter getPropertiesConverter() {
    return DorisPropertiesConverter.getInstance();
  }

  @Override
  protected SparkTransformConverter getSparkTransformConverter() {
    return new SparkTransformConverter(false);
  }

  /** Returns the Doris-specific Spark type converter for the active Spark version. */
  @Override
  protected abstract SparkTypeConverter getSparkTypeConverter();

  /** Creates the version-specific official Doris table catalog. */
  protected abstract TableCatalog createDorisTableCatalog();

  /**
   * Loads one physical schema snapshot for an authorized table.
   *
   * <p>The default path supports test and compatibility catalogs that expose only a Catalyst
   * schema. Version-specific Doris adapters should retain the FE type names as well because JDBC
   * metadata may omit the outer type of complex columns.
   */
  @SuppressWarnings("deprecation")
  protected DorisPhysicalSchema loadPhysicalSchema(
      TableCatalog sparkCatalog, Identifier identifier, Table sparkTable) {
    return DorisPhysicalSchema.withoutTypeNames(sparkTable.schema());
  }

  /**
   * Creates the version-specific Doris read delegate from one validated schema snapshot.
   *
   * @param sparkCatalog the initialized official Doris table catalog
   * @param identifier the authorized Spark table identifier
   * @param readSchema the validated read schema and SQL normalization plan
   * @param connectionInfo the credential-vended JDBC connection material
   * @return a readable Doris table delegate
   */
  protected abstract Table createSchemaSeededDorisTable(
      TableCatalog sparkCatalog,
      Identifier identifier,
      DorisReadSchema readSchema,
      DorisJdbcConnectionInfo connectionInfo,
      DorisJdbcReadOptions readOptions);

  /**
   * Creates the governed table facade for the active Spark version.
   *
   * <p>This factory is the stable extension seam for future batch writes, streaming writes, and
   * governed DDL. The initial implementation deliberately exposes only batch reads.
   *
   * @param identifier the authorized table identifier
   * @param gravitinoTable the governed table metadata
   * @param readDelegate the version-specific read delegate
   * @param validatedSchema the validated Spark-visible schema
   * @param propertiesConverter the Doris property converter
   * @param sparkTransformConverter the Spark transform converter
   * @param sparkTypeConverter the Doris Spark type converter
   * @return the governed table facade
   */
  protected Table createGovernedDorisTable(
      Identifier identifier,
      org.apache.gravitino.rel.Table gravitinoTable,
      Table readDelegate,
      StructType validatedSchema,
      PropertiesConverter propertiesConverter,
      SparkTransformConverter sparkTransformConverter,
      SparkTypeConverter sparkTypeConverter) {
    return new GovernedDorisTable(
        identifier,
        gravitinoTable,
        readDelegate,
        validatedSchema,
        propertiesConverter,
        sparkTransformConverter,
        sparkTypeConverter,
        getCapabilityPolicy());
  }

  /** Returns the centralized capability policy used by the governed table facade. */
  protected DorisCapabilityPolicy getCapabilityPolicy() {
    return defaultCapabilityPolicy;
  }

  /** Returns the catalog mutation delegate. Future versions may override this seam. */
  protected DorisCatalogMutationDelegate getMutationDelegate() {
    return defaultMutationDelegate;
  }

  /** Returns the table delegate decorator. Future versions may override this seam. */
  protected DorisWriteDelegateFactory getWriteDelegateFactory() {
    return defaultWriteDelegateFactory;
  }

  @Override
  protected String getCatalogDefaultNamespace() {
    throw new IllegalArgumentException(
        "Doris table identifiers require a schema; use <catalog>.<schema>.<table> "
            + "or set the current namespace");
  }

  @Override
  public void invalidateTable(Identifier ident) {
    super.invalidateTable(ident);
    if (physicalSchemaCache != null) {
      physicalSchemaCache.invalidate(ident);
    }
  }

  private String qualifiedIdentifier(Identifier identifier) {
    return String.format(
        "%s.%s.%s", name(), String.join(".", identifier.namespace()), identifier.name());
  }

  private IllegalStateException missingConnectorDependency(LinkageError cause) {
    return new IllegalStateException(
        String.format(
            "The Gravitino Doris Spark adapter requires %s on the Spark driver and executor "
                + "classpaths for Spark 3.5 and Scala 2.12",
            DorisConnectorConstants.DORIS_CONNECTOR_COORDINATES),
        cause);
  }
}
