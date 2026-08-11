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

package io.github.jiangxt2.gravitino.doris.server;

import io.github.jiangxt2.gravitino.doris.security.DorisJdbcSecurity;
import java.util.Map;
import org.apache.gravitino.catalog.doris.DorisCatalog;
import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.connector.PropertiesMetadata;

/** Gravitino 1.3 catalog provider dedicated to governed Doris Spark reads. */
public final class GovernedDorisCatalogProvider extends DorisCatalog {

  private static final String MYSQL_DRIVER_CLASS = "com.mysql.cj.jdbc.Driver";
  private static final String MISSING_DRIVER_MESSAGE =
      "MySQL Connector/J (tested with com.mysql:mysql-connector-j:8.0.33) is not available to "
          + "the Gravitino Server catalog; install it in "
          + "$GRAVITINO_HOME/catalogs/doris-governed/libs before starting Gravitino";
  private static final GovernedDorisCatalogPropertiesMetadata CATALOG_PROPERTIES =
      new GovernedDorisCatalogPropertiesMetadata();

  @Override
  public String shortName() {
    return "doris-governed";
  }

  @Override
  public GovernedDorisCatalogProvider withCatalogConf(Map<String, String> conf) {
    DorisJdbcSecurity.validateServerCatalogProperties(conf);
    requireMysqlDriver();
    super.withCatalogConf(conf);
    return this;
  }

  @Override
  protected JdbcTypeConverter createJdbcTypeConverter() {
    return new GovernedDorisTypeConverter();
  }

  @Override
  public PropertiesMetadata catalogPropertiesMetadata() {
    return CATALOG_PROPERTIES;
  }

  private static void requireMysqlDriver() {
    ClassLoader providerClassLoader = GovernedDorisCatalogProvider.class.getClassLoader();
    try {
      Class.forName(MYSQL_DRIVER_CLASS, false, providerClassLoader);
    } catch (ClassNotFoundException | LinkageError | SecurityException e) {
      throw new IllegalStateException(MISSING_DRIVER_MESSAGE, e);
    }
  }
}
