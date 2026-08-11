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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * Converts Gravitino Doris properties into the option contract of the official Doris Spark
 * Connector.
 *
 * <p>Only explicitly reviewed read options are passed through. Endpoints and credentials are
 * protected from Spark options and {@code spark.bypass.*} catalog properties.
 */
public class DorisPropertiesConverter implements PropertiesConverter {

  private static final Set<String> PROTECTED_CONNECTOR_PROPERTIES =
      Set.of(
          DorisConnectorConstants.DORIS_FE_NODES,
          DorisConnectorConstants.DORIS_QUERY_PORT,
          DorisConnectorConstants.DORIS_USER,
          DorisConnectorConstants.DORIS_PASSWORD,
          DorisConnectorConstants.JDBC_URL,
          DorisConnectorConstants.JDBC_DRIVER,
          DorisConnectorConstants.JDBC_USER,
          DorisConnectorConstants.JDBC_PASSWORD,
          "url",
          "driver",
          "user",
          "password",
          "dbtable");

  private static final Set<String> ALLOWED_CONNECTOR_PROPERTIES =
      Set.of(
          "doris.request.retries",
          "doris.request.connect.timeout.ms",
          "doris.request.read.timeout.ms",
          "doris.request.query.timeout.s",
          "doris.request.tablet.size",
          "doris.batch.size",
          "doris.exec.mem.limit",
          "doris.filter.query.in.max.count",
          "doris.thrift.max.message.size");

  private static final Set<String> CONNECTION_TABLE_PROPERTIES =
      Set.of(
          DorisConnectorConstants.JDBC_URL,
          DorisConnectorConstants.JDBC_DATABASE,
          DorisConnectorConstants.JDBC_DRIVER,
          DorisConnectorConstants.JDBC_USER,
          DorisConnectorConstants.JDBC_PASSWORD,
          DorisConnectorConstants.GRAVITINO_DORIS_FE_NODES,
          DorisConnectorConstants.GRAVITINO_DORIS_QUERY_PORT,
          DorisConnectorConstants.DORIS_FE_NODES,
          DorisConnectorConstants.DORIS_QUERY_PORT,
          DorisConnectorConstants.DORIS_USER,
          DorisConnectorConstants.DORIS_PASSWORD,
          "url",
          "driver",
          "user",
          "password",
          "dbtable",
          DorisConnectorConstants.JDBC_PARTITION_COLUMN,
          DorisConnectorConstants.JDBC_LOWER_BOUND,
          DorisConnectorConstants.JDBC_UPPER_BOUND,
          DorisConnectorConstants.JDBC_NUM_PARTITIONS,
          DorisConnectorConstants.JDBC_FETCH_SIZE,
          DorisConnectorConstants.SCHEMA_CACHE_TTL_MS,
          DorisConnectorConstants.SCHEMA_CACHE_MAX_ENTRIES);

  private static final Pattern FE_ENDPOINT_PATTERN = Pattern.compile("([\\w.-]+):(\\d+)");

  private DorisPropertiesConverter() {}

  /** Returns the singleton Doris property converter. */
  public static DorisPropertiesConverter getInstance() {
    return Holder.INSTANCE;
  }

  @Override
  public Map<String, String> toSparkCatalogProperties(
      CaseInsensitiveStringMap options, Map<String, String> properties) {
    validateNoCredentialBackfill(properties);

    Map<String, String> result = new HashMap<>();
    result.putAll(extractCatalogBypassProperties(properties));
    if (options != null) {
      result.putAll(
          validateConnectorProperties(options.asCaseSensitiveMap(), "Spark catalog options"));
    }

    Map<String, String> endpointProperties = toSparkCatalogProperties(properties);
    if (!endpointProperties.containsKey(DorisConnectorConstants.DORIS_FE_NODES)) {
      throw new IllegalArgumentException(
          "Governed Doris reads require catalog property "
              + DorisConnectorConstants.GRAVITINO_DORIS_FE_NODES);
    }
    if (!endpointProperties.containsKey(DorisConnectorConstants.DORIS_QUERY_PORT)) {
      throw new IllegalArgumentException(
          "Governed Doris reads require catalog property "
              + DorisConnectorConstants.GRAVITINO_DORIS_QUERY_PORT);
    }

    // Catalog-managed endpoints are applied after pass-through options.
    result.putAll(endpointProperties);
    return result;
  }

  @Override
  public Map<String, String> toSparkCatalogProperties(Map<String, String> properties) {
    if (properties == null) {
      throw new IllegalArgumentException("Doris catalog properties must not be null");
    }
    validateNoCredentialBackfill(properties);

    Map<String, String> result = new HashMap<>();
    String feNodes = properties.get(DorisConnectorConstants.GRAVITINO_DORIS_FE_NODES);
    if (feNodes != null) {
      result.put(DorisConnectorConstants.DORIS_FE_NODES, normalizeFeNodes(feNodes));
    }

    String queryPort = properties.get(DorisConnectorConstants.GRAVITINO_DORIS_QUERY_PORT);
    if (queryPort != null) {
      validatePort(DorisConnectorConstants.GRAVITINO_DORIS_QUERY_PORT, queryPort.trim());
      result.put(DorisConnectorConstants.DORIS_QUERY_PORT, queryPort.trim());
    }
    return result;
  }

  @Override
  public Map<String, String> toGravitinoTableProperties(Map<String, String> properties) {
    return properties == null ? new HashMap<>() : new HashMap<>(properties);
  }

  @Override
  public Map<String, String> toSparkTableProperties(Map<String, String> properties) {
    if (properties == null) {
      return new HashMap<>();
    }

    Map<String, String> result = new HashMap<>();
    properties.forEach(
        (key, value) -> {
          String canonicalKey = key.toLowerCase(Locale.ROOT);
          if (!CONNECTION_TABLE_PROPERTIES.contains(canonicalKey)
              && !canonicalKey.startsWith(SPARK_PROPERTY_PREFIX)) {
            result.put(key, value);
          }
        });
    return result;
  }

  private Map<String, String> extractCatalogBypassProperties(Map<String, String> properties) {
    if (properties == null) {
      return new HashMap<>();
    }

    Map<String, String> bypassProperties = new HashMap<>();
    properties.forEach(
        (key, value) -> {
          if (key.startsWith(SPARK_PROPERTY_PREFIX)) {
            bypassProperties.put(key.substring(SPARK_PROPERTY_PREFIX.length()), value);
          }
        });
    return validateConnectorProperties(
        bypassProperties, "Gravitino spark.bypass catalog properties");
  }

  private Map<String, String> validateConnectorProperties(
      Map<String, String> input, String source) {
    if (input == null || input.isEmpty()) {
      return new HashMap<>();
    }

    Map<String, String> result = new HashMap<>();
    Set<String> seen = new HashSet<>();
    input.forEach(
        (key, value) -> {
          if (key == null) {
            throw new IllegalArgumentException(source + " contain a null option key");
          }

          String canonicalKey = key.toLowerCase(Locale.ROOT);
          if (!seen.add(canonicalKey)) {
            throw new IllegalArgumentException(
                source + " contain duplicate option " + canonicalKey);
          }
          if (PROTECTED_CONNECTOR_PROPERTIES.contains(canonicalKey)) {
            throw new IllegalArgumentException(
                source + " must not override protected option " + canonicalKey);
          }
          if (!canonicalKey.startsWith("doris.")) {
            throw new IllegalArgumentException(
                source + " must use canonical doris.* option names: " + key);
          }
          if (!ALLOWED_CONNECTOR_PROPERTIES.contains(canonicalKey)) {
            throw new IllegalArgumentException(
                source + " contain unsupported Doris read option " + canonicalKey);
          }
          if (isBlank(value)) {
            throw new IllegalArgumentException(
                source + " contain a blank value for option " + canonicalKey);
          }
          result.put(canonicalKey, value);
        });
    return result;
  }

  private void validateNoCredentialBackfill(Map<String, String> properties) {
    if (properties == null) {
      return;
    }

    Set<String> unexpectedKeys = new TreeSet<>();
    properties
        .keySet()
        .forEach(
            key -> {
              String canonicalKey = key.toLowerCase(Locale.ROOT);
              if (DorisConnectorConstants.JDBC_USER.equals(canonicalKey)
                  || DorisConnectorConstants.JDBC_PASSWORD.equals(canonicalKey)) {
                unexpectedKeys.add(canonicalKey);
              }
            });
    if (!unexpectedKeys.isEmpty()) {
      throw new IllegalArgumentException(
          "Ordinary Doris catalog properties must not contain hidden credential keys: "
              + unexpectedKeys);
    }
  }

  private String normalizeFeNodes(String value) {
    if (isBlank(value)) {
      throw new IllegalArgumentException(
          DorisConnectorConstants.GRAVITINO_DORIS_FE_NODES
              + " must use host:httpPort[,host2:httpPort] format");
    }

    StringBuilder normalized = new StringBuilder();
    for (String endpoint : value.split(",", -1)) {
      String trimmedEndpoint = endpoint.trim();
      Matcher matcher = FE_ENDPOINT_PATTERN.matcher(trimmedEndpoint);
      if (!matcher.matches()) {
        throw new IllegalArgumentException(
            DorisConnectorConstants.GRAVITINO_DORIS_FE_NODES
                + " must use host:httpPort[,host2:httpPort] format");
      }
      validatePort(DorisConnectorConstants.GRAVITINO_DORIS_FE_NODES, matcher.group(2));
      if (normalized.length() > 0) {
        normalized.append(',');
      }
      normalized.append(trimmedEndpoint);
    }
    return normalized.toString();
  }

  private void validatePort(String property, String value) {
    try {
      if (value == null || !value.equals(value.trim())) {
        throw new NumberFormatException("port contains whitespace");
      }
      int port = Integer.parseInt(value);
      if (port < 1 || port > 65535) {
        throw new NumberFormatException("port out of range");
      }
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(property + " must contain a port between 1 and 65535");
    }
  }

  private boolean isBlank(String value) {
    return value == null || value.trim().isEmpty();
  }

  private static class Holder {
    private static final DorisPropertiesConverter INSTANCE = new DorisPropertiesConverter();
  }
}
