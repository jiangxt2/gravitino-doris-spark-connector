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
import java.util.function.Supplier;
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

/** A governed Spark catalog for native-compatible or strict JDBC Doris access. */
public abstract class GovernedDorisCatalog extends GravitinoJdbcCatalog {

  private static final Logger LOG = LoggerFactory.getLogger(GovernedDorisCatalog.class);
  private static final String MYSQL_DRIVER_CLASS = "com.mysql.cj.jdbc.Driver";
  private static final String MISSING_MYSQL_DRIVER_MESSAGE =
      "MySQL Connector/J (tested with com.mysql:mysql-connector-j:8.0.33) is required on the "
          + "Spark driver and every executor classpath; distribute it with --jars or spark.jars";

  /** Credential-vended connection material shared by the selected read transport. */
  private DorisJdbcConnectionInfo jdbcConnectionInfo;

  private DorisJdbcReadOptions jdbcReadOptions;
  private DorisPhysicalSchemaCache physicalSchemaCache;
  private DorisReadTransport readTransport;
  private DorisWritePolicy writePolicy = DorisWritePolicy.disabled();
  private DorisCapabilityPolicy capabilityPolicy = DorisCapabilityPolicy.readOnly();
  private final DorisCatalogMutationDelegate defaultMutationDelegate =
      DorisCatalogMutationDelegate.readOnly(DorisCapabilityPolicy.readOnly());
  private final DorisWriteDelegateFactory defaultWriteDelegateFactory =
      DorisWriteDelegateFactory.readOnly();

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    return loadAuthorizedTable(ident, Set.of(Privilege.Name.SELECT_TABLE), false);
  }

  /** Loads a write-capable table only after both read and modify authorization succeed. */
  protected final Table loadTableForGovernedWrite(Identifier ident) throws NoSuchTableException {
    if (!writePolicy.enabled() || !DorisCatalogClassResolver.supportsWriteAwareLoad()) {
      throw capabilityPolicy.reject("table writes");
    }
    return loadAuthorizedTable(
        ident, Set.of(Privilege.Name.SELECT_TABLE, Privilege.Name.MODIFY_TABLE), true);
  }

  private Table loadAuthorizedTable(
      Identifier ident, Set<Privilege.Name> privileges, boolean writeAuthorized)
      throws NoSuchTableException {
    org.apache.gravitino.rel.Table gravitinoTable;
    try {
      gravitinoTable =
          gravitinoCatalogClient
              .asTableCatalog()
              .loadTable(NameIdentifier.of(getDatabase(ident), ident.name()), privileges);
    } catch (org.apache.gravitino.exceptions.NoSuchTableException e) {
      throw new NoSuchTableException(ident);
    }

    // Authorization above is deliberately completed before any physical delegate is touched.
    if (readTransport == DorisReadTransport.STRICT_JDBC_TLS) {
      if (writeAuthorized) {
        throw new IllegalStateException("Strict JDBC TLS tables must remain read-only");
      }
      return createStrictSparkTable(
          ident,
          gravitinoTable,
          sparkCatalog,
          propertiesConverter,
          sparkTransformConverter,
          getSparkTypeConverter());
    }
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
    if (!writeAuthorized) {
      return createSparkTable(
          ident,
          gravitinoTable,
          physicalTable,
          sparkCatalog,
          propertiesConverter,
          sparkTransformConverter,
          getSparkTypeConverter());
    }
    return createSparkTableAfterAuthorization(
        ident,
        gravitinoTable,
        physicalTable,
        sparkCatalog,
        propertiesConverter,
        sparkTransformConverter,
        getSparkTypeConverter(),
        true);
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
    DorisJdbcSecurity.validateServerCatalogProperties(properties);
    readTransport = DorisReadTransport.from(properties);
    writePolicy = DorisWritePolicy.from(properties);
    capabilityPolicy = DorisCapabilityPolicy.from(writePolicy);
    DorisJdbcSecurity.validateConnection(
        properties.get(DorisConnectorConstants.JDBC_URL),
        properties.get(DorisConnectorConstants.JDBC_DRIVER),
        readTransport == DorisReadTransport.STRICT_JDBC_TLS
            ? DorisConnectorConstants.STRICT_JDBC_TLS_TRANSPORT
            : DorisConnectorConstants.HYBRID_TRANSPORT);
    requireMysqlDriver();

    TableCatalog dorisCatalog;
    try {
      dorisCatalog = createDorisTableCatalog(readTransport);
    } catch (LinkageError e) {
      throw missingConnectorDependency(e);
    }

    Map<String, String> connectorProperties =
        getPropertiesConverter().toSparkCatalogProperties(options, properties);
    JdbcCredential credential = DorisCredentialResolver.resolve(gravitinoCatalogClient);
    if (readTransport.allowsNativeLane()) {
      connectorProperties.put(DorisConnectorConstants.DORIS_USER, credential.jdbcUser());
      connectorProperties.put(DorisConnectorConstants.DORIS_PASSWORD, credential.jdbcPassword());
    } else {
      connectorProperties.put("url", properties.get(DorisConnectorConstants.JDBC_URL));
      connectorProperties.put("driver", properties.get(DorisConnectorConstants.JDBC_DRIVER));
      connectorProperties.put("user", credential.jdbcUser());
      connectorProperties.put("password", credential.jdbcPassword());
    }
    jdbcConnectionInfo =
        new DorisJdbcConnectionInfo(
            properties.get(DorisConnectorConstants.JDBC_URL),
            properties.get(DorisConnectorConstants.JDBC_DRIVER),
            credential.jdbcUser(),
            credential.jdbcPassword(),
            readTransport);
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
          "Doris read catalog initialization failed for catalog {} with exception type {}",
          name,
          e.getClass().getSimpleName());
      throw new IllegalArgumentException("Failed to initialize the Doris Spark read catalog");
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
    return createSparkTableAfterAuthorization(
        identifier,
        gravitinoTable,
        sparkTable,
        sparkCatalog,
        propertiesConverter,
        sparkTransformConverter,
        sparkTypeConverter,
        false);
  }

  private Table createSparkTableAfterAuthorization(
      Identifier identifier,
      org.apache.gravitino.rel.Table gravitinoTable,
      Table sparkTable,
      TableCatalog sparkCatalog,
      PropertiesConverter propertiesConverter,
      SparkTransformConverter sparkTransformConverter,
      SparkTypeConverter sparkTypeConverter,
      boolean writeAuthorized) {
    Supplier<DorisPhysicalSchema> physicalSchemaLoader =
        () -> loadPhysicalSchema(sparkCatalog, identifier, sparkTable);
    DorisReadSchema readSchema =
        loadAndValidateReadSchema(
            identifier,
            gravitinoTable,
            physicalSchemaLoader,
            sparkTypeConverter,
            "Failed to load physical Doris schema for authorized table %s");
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
            readSchema,
            jdbcConnectionInfo,
            jdbcReadOptions,
            writePolicy);
    Table tableDelegate;
    try {
      tableDelegate =
          writeAuthorized ? getWriteDelegateFactory().create(authorizedContext) : readDelegate;
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
        sparkTypeConverter,
        writeAuthorized ? capabilityPolicy : DorisCapabilityPolicy.readOnly());
  }

  private Table createStrictSparkTable(
      Identifier identifier,
      org.apache.gravitino.rel.Table gravitinoTable,
      TableCatalog strictCatalog,
      PropertiesConverter propertiesConverter,
      SparkTransformConverter sparkTransformConverter,
      SparkTypeConverter sparkTypeConverter) {
    Supplier<DorisPhysicalSchema> physicalSchemaLoader =
        () -> loadStrictPhysicalSchema(strictCatalog, identifier, jdbcConnectionInfo);
    DorisReadSchema readSchema =
        loadAndValidateReadSchema(
            identifier,
            gravitinoTable,
            physicalSchemaLoader,
            sparkTypeConverter,
            "Failed to load verified JDBC schema for authorized table %s");
    Table readDelegate;
    try {
      readDelegate =
          createStrictJdbcTable(
              strictCatalog, identifier, readSchema, jdbcConnectionInfo, jdbcReadOptions);
    } catch (LinkageError e) {
      throw missingConnectorDependency(e);
    } catch (RuntimeException e) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to create the verified JDBC read table for authorized table %s",
              qualifiedIdentifier(identifier)));
    }

    // The existing write seam is intentionally tied to the official Doris physical table used by
    // the hybrid profile. A strict JDBC-only read has no equivalent native physical table, so it
    // remains structurally read-only instead of disguising its JDBC read delegate as one.
    return createGovernedDorisTable(
        identifier,
        gravitinoTable,
        readDelegate,
        readSchema.schema(),
        propertiesConverter,
        sparkTransformConverter,
        sparkTypeConverter,
        DorisCapabilityPolicy.readOnly());
  }

  private DorisReadSchema loadAndValidateReadSchema(
      Identifier identifier,
      org.apache.gravitino.rel.Table gravitinoTable,
      Supplier<DorisPhysicalSchema> physicalSchemaLoader,
      SparkTypeConverter sparkTypeConverter,
      String loadFailureMessage) {
    DorisPhysicalSchemaCache.Lookup physicalSchemaLookup;
    try {
      // The normal path makes at most one physical schema request after authorization. The
      // transport-specific implementation may retain physical type names that its Catalyst
      // schema intentionally normalizes.
      physicalSchemaLookup = physicalSchemaCache.getWithStatus(identifier, physicalSchemaLoader);
    } catch (LinkageError e) {
      throw missingConnectorDependency(e);
    } catch (RuntimeException e) {
      // Do not retain or log the physical loader exception: third-party exception text is outside
      // the adapter's credential-redaction boundary.
      throw new IllegalArgumentException(
          String.format(loadFailureMessage, qualifiedIdentifier(identifier)));
    }

    try {
      return DorisSchemaCompatibility.planReadSchema(
          identifier, gravitinoTable, physicalSchemaLookup.schema(), sparkTypeConverter);
    } catch (IllegalArgumentException incompatibleCachedSnapshot) {
      if (!physicalSchemaLookup.cacheHit()) {
        throw incompatibleCachedSnapshot;
      }

      // Spark resolves a REFRESH TABLE relation before it invokes invalidateTable. If that
      // resolution observes a cached snapshot made stale by Doris DDL, conditionally replace only
      // that exact snapshot and validate once more. A fresh mismatch still fails closed, and this
      // path never retries physical load failures.
      DorisPhysicalSchema refreshedPhysicalSchema;
      try {
        refreshedPhysicalSchema =
            physicalSchemaCache.reloadIfSame(
                identifier, physicalSchemaLookup.schema(), physicalSchemaLoader);
      } catch (LinkageError e) {
        throw missingConnectorDependency(e);
      } catch (RuntimeException e) {
        throw new IllegalArgumentException(
            String.format(loadFailureMessage, qualifiedIdentifier(identifier)));
      }
      return DorisSchemaCompatibility.planReadSchema(
          identifier, gravitinoTable, refreshedPhysicalSchema, sparkTypeConverter);
    }
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

  /** Creates the version-specific read catalog without opening a connection. */
  protected TableCatalog createDorisTableCatalog(DorisReadTransport transport) {
    if (!transport.allowsNativeLane()) {
      throw new UnsupportedOperationException(
          "This Spark adapter does not implement strict JDBC TLS reads");
    }
    return createDorisTableCatalog();
  }

  /** Creates the version-specific official Doris table catalog for compatible hybrid reads. */
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

  /** Loads a physical schema over the verified JDBC transport after authorization. */
  protected DorisPhysicalSchema loadStrictPhysicalSchema(
      TableCatalog sparkCatalog, Identifier identifier, DorisJdbcConnectionInfo connectionInfo) {
    throw new UnsupportedOperationException(
        "This Spark adapter does not implement strict JDBC TLS schema loading");
  }

  /** Creates the Spark JDBC V2 table used by a strict read after schema validation. */
  protected Table createStrictJdbcTable(
      TableCatalog sparkCatalog,
      Identifier identifier,
      DorisReadSchema readSchema,
      DorisJdbcConnectionInfo connectionInfo,
      DorisJdbcReadOptions readOptions) {
    throw new UnsupportedOperationException(
        "This Spark adapter does not implement strict JDBC TLS table loading");
  }

  /**
   * Creates the governed table facade for the active Spark version.
   *
   * <p>This factory is the stable extension seam for authorized batch writes. Streaming writes and
   * governed DDL remain unsupported.
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
      SparkTypeConverter sparkTypeConverter,
      DorisCapabilityPolicy tableCapabilityPolicy) {
    return new GovernedDorisTable(
        identifier,
        gravitinoTable,
        readDelegate,
        validatedSchema,
        propertiesConverter,
        sparkTransformConverter,
        sparkTypeConverter,
        tableCapabilityPolicy);
  }

  /** Returns the centralized capability policy used by the governed table facade. */
  protected DorisCapabilityPolicy getCapabilityPolicy() {
    return capabilityPolicy;
  }

  /** Returns the catalog-managed write policy. */
  protected DorisWritePolicy getWritePolicy() {
    return writePolicy;
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
    if (readTransport == null || readTransport.allowsNativeLane()) {
      super.invalidateTable(ident);
    }
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

  private static void requireMysqlDriver() {
    ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
    ClassLoader adapterClassLoader = GovernedDorisCatalog.class.getClassLoader();
    try {
      Class.forName(
          MYSQL_DRIVER_CLASS,
          false,
          contextClassLoader == null ? adapterClassLoader : contextClassLoader);
    } catch (ClassNotFoundException | LinkageError | SecurityException firstFailure) {
      if (contextClassLoader != null && contextClassLoader != adapterClassLoader) {
        try {
          Class.forName(MYSQL_DRIVER_CLASS, false, adapterClassLoader);
          return;
        } catch (ClassNotFoundException | LinkageError | SecurityException ignored) {
          firstFailure.addSuppressed(ignored);
        }
      }
      throw new IllegalStateException(MISSING_MYSQL_DRIVER_MESSAGE, firstFailure);
    }
  }
}
