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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.spark.sql.types.StructType;

/** One physical Doris schema snapshot with both Catalyst and original Doris type information. */
public final class DorisPhysicalSchema {

  private final StructType schema;
  private final List<String> dorisTypeNames;

  /**
   * Creates one physical schema snapshot.
   *
   * @param schema the Catalyst schema derived from the Doris FE response
   * @param dorisTypeNames the corresponding original Doris FE type names
   */
  public DorisPhysicalSchema(StructType schema, List<String> dorisTypeNames) {
    Objects.requireNonNull(schema, "Doris physical schema must not be null");
    Objects.requireNonNull(dorisTypeNames, "Doris type names must not be null");
    DorisChecks.checkArgument(
        schema.length() == dorisTypeNames.size(),
        "Doris physical fields and type names must have the same size");
    this.schema = schema;
    this.dorisTypeNames = Collections.unmodifiableList(new ArrayList<>(dorisTypeNames));
  }

  /** Returns a snapshot when only the Connector's Catalyst schema is available. */
  public static DorisPhysicalSchema withoutTypeNames(StructType schema) {
    return new DorisPhysicalSchema(schema, Collections.nCopies(schema.length(), ""));
  }

  /** Returns the physical Catalyst schema. */
  public StructType schema() {
    return schema;
  }

  /** Returns the original Doris FE type name for a field. */
  public String dorisTypeName(int index) {
    return dorisTypeNames.get(index);
  }
}
