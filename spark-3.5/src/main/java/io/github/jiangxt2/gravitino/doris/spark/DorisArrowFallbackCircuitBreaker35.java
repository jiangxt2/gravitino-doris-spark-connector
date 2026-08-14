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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.Properties;
import org.apache.spark.SparkEnv;

/**
 * Executor-local, application-scoped, fail-sticky Arrow transport circuit.
 *
 * <p>JDK system properties are intentionally used as the executor-JVM store. A static map would be
 * scoped to the connector classloader and would not reliably share state across catalog and task
 * classloaders.
 */
final class DorisArrowFallbackCircuitBreaker35 {

  private static final String PROPERTY_PREFIX = "spark.gravitino.doris.arrow.";
  private static final String CIRCUIT_PREFIX = PROPERTY_PREFIX + "circuit.";
  private static final String ATTEMPT_PREFIX = PROPERTY_PREFIX + "attempt.";

  private DorisArrowFallbackCircuitBreaker35() {}

  static boolean isOpen(String endpointIdentity) {
    return isOpen(applicationId(), endpointIdentity);
  }

  static void open(String endpointIdentity) {
    open(applicationId(), endpointIdentity);
  }

  static boolean isOpen(String applicationId, String endpointIdentity) {
    return Boolean.parseBoolean(
        System.getProperty(circuitPropertyKey(applicationId, endpointIdentity)));
  }

  static void open(String applicationId, String endpointIdentity) {
    System.setProperty(circuitPropertyKey(applicationId, endpointIdentity), "true");
  }

  static long recordAttempt(String endpointIdentity) {
    return recordAttempt(applicationId(), endpointIdentity);
  }

  static long recordAttempt(String applicationId, String endpointIdentity) {
    Properties properties = System.getProperties();
    String propertyKey = attemptPropertyKey(applicationId, endpointIdentity);
    // Properties synchronizes individual operations. Hold the same monitor for this short,
    // once-per-partition read-modify-write so the diagnostic attempt counter remains atomic.
    synchronized (properties) {
      long current = Long.parseLong(properties.getProperty(propertyKey, "0"));
      long updated = current == Long.MAX_VALUE ? Long.MAX_VALUE : current + 1L;
      properties.setProperty(propertyKey, Long.toString(updated));
      return updated;
    }
  }

  static long attemptCount(String applicationId, String endpointIdentity) {
    return Long.parseLong(
        System.getProperty(attemptPropertyKey(applicationId, endpointIdentity), "0"));
  }

  static void clearForTests() {
    Properties properties = System.getProperties();
    synchronized (properties) {
      for (Object property : new ArrayList<>(properties.keySet())) {
        if (property instanceof String && ((String) property).startsWith(PROPERTY_PREFIX)) {
          properties.remove(property);
        }
      }
    }
  }

  private static String applicationId() {
    SparkEnv environment = SparkEnv.get();
    return environment == null ? "no-spark-environment" : environment.conf().getAppId();
  }

  private static String key(String applicationId, String endpointIdentity) {
    DorisChecks.checkArgument(
        applicationId != null && !applicationId.isEmpty(), "Spark application ID is required");
    DorisChecks.checkArgument(
        endpointIdentity != null && !endpointIdentity.isEmpty(),
        "Doris endpoint identity is required");
    return applicationId + ':' + endpointIdentity;
  }

  private static String circuitPropertyKey(String applicationId, String endpointIdentity) {
    return propertyKey(CIRCUIT_PREFIX, applicationId, endpointIdentity);
  }

  private static String attemptPropertyKey(String applicationId, String endpointIdentity) {
    return propertyKey(ATTEMPT_PREFIX, applicationId, endpointIdentity);
  }

  private static String propertyKey(String prefix, String applicationId, String endpointIdentity) {
    try {
      byte[] digest =
          MessageDigest.getInstance("SHA-256")
              .digest(key(applicationId, endpointIdentity).getBytes(StandardCharsets.UTF_8));
      return prefix + HexFormat.of().formatHex(digest);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is required for the Arrow circuit identity", e);
    }
  }
}
