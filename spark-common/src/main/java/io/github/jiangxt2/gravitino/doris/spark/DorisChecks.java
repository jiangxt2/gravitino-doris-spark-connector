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

import java.util.Locale;

/** Dependency-free argument and state validation for the Spark-side runtime. */
final class DorisChecks {

  private DorisChecks() {}

  static void checkArgument(boolean expression, String message, Object... arguments) {
    if (!expression) {
      throw new IllegalArgumentException(format(message, arguments));
    }
  }

  static void checkState(boolean expression, String message, Object... arguments) {
    if (!expression) {
      throw new IllegalStateException(format(message, arguments));
    }
  }

  private static String format(String message, Object... arguments) {
    return arguments.length == 0 ? message : String.format(Locale.ROOT, message, arguments);
  }
}
