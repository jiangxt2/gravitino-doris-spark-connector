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

package io.github.jiangxt2.gravitino.doris.it;

import java.util.Map;

/** Shared Docker label contract for containers owned by the integration tests. */
public final class IntegrationTestContainerLabels {

  static final String KEY = "io.github.jiangxt2.gravitino-doris-spark-connector.it";
  static final String VALUE = "true";
  static final String ASSIGNMENT = KEY + "=" + VALUE;

  private IntegrationTestContainerLabels() {}

  static void assertProjectLabel(String containerRole, Map<String, String> labels) {
    if (labels == null || !labels.containsKey(KEY)) {
      throw new IllegalStateException(
          String.format(
              "Integration-test container %s is missing required Docker label %s",
              containerRole, KEY));
    }
    if (!VALUE.equals(labels.get(KEY))) {
      throw new IllegalStateException(
          String.format(
              "Integration-test container %s has an unexpected value for Docker label %s; "
                  + "expected %s",
              containerRole, KEY, VALUE));
    }
  }
}
