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

/** Shared property names and version constraints of the governed Doris connector. */
public final class DorisConnectorConstants {

  /** Gravitino provider short name owned by this project. */
  public static final String PROVIDER = "doris-governed";

  /** Official Doris Connector coordinate required by the Spark runtime. */
  public static final String DORIS_CONNECTOR_COORDINATES =
      "org.apache.doris:spark-doris-connector-spark-3.5:26.0.0";

  public static final String JDBC_URL = "jdbc-url";
  public static final String JDBC_DATABASE = "jdbc-database";
  public static final String JDBC_DRIVER = "jdbc-driver";
  public static final String JDBC_USER = "jdbc-user";
  public static final String JDBC_PASSWORD = "jdbc-password";

  public static final String GRAVITINO_DORIS_FE_NODES = "doris-fenodes";
  public static final String GRAVITINO_DORIS_QUERY_PORT = "doris-query-port";
  public static final String DORIS_FE_NODES = "doris.fenodes";
  public static final String DORIS_QUERY_PORT = "doris.query.port";
  public static final String DORIS_USER = "doris.user";
  public static final String DORIS_PASSWORD = "doris.password";

  public static final String JDBC_PARTITION_COLUMN = "doris-jdbc-partition-column";
  public static final String JDBC_LOWER_BOUND = "doris-jdbc-lower-bound";
  public static final String JDBC_UPPER_BOUND = "doris-jdbc-upper-bound";
  public static final String JDBC_NUM_PARTITIONS = "doris-jdbc-num-partitions";
  public static final String JDBC_FETCH_SIZE = "doris-jdbc-fetch-size";

  public static final String SCHEMA_CACHE_TTL_MS = "doris-schema-cache-ttl-ms";
  public static final String SCHEMA_CACHE_MAX_ENTRIES = "doris-schema-cache-max-entries";
  public static final long DEFAULT_SCHEMA_CACHE_TTL_MS = 30_000L;
  public static final int DEFAULT_SCHEMA_CACHE_MAX_ENTRIES = 1_000;

  private DorisConnectorConstants() {}
}
