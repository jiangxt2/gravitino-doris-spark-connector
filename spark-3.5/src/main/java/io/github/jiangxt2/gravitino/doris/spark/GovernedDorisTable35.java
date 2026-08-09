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

import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.gravitino.spark.connector.SparkTransformConverter;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/** Spark 3.5 governed Doris table facade. */
public class GovernedDorisTable35 extends GovernedDorisTable {

  /**
   * Creates a governed read-only Doris table for Spark 3.5.
   *
   * @param identifier the authorized Spark table identifier
   * @param gravitinoTable the governed table metadata
   * @param delegate the version-specific hybrid read delegate
   * @param validatedSchema the cached Spark-visible schema
   * @param propertiesConverter the Doris property converter
   * @param transformConverter the Spark transform converter
   * @param typeConverter the Doris Spark type converter
   * @param capabilityPolicy the capabilities explicitly exposed by the governed facade
   */
  public GovernedDorisTable35(
      Identifier identifier,
      org.apache.gravitino.rel.Table gravitinoTable,
      Table delegate,
      StructType validatedSchema,
      PropertiesConverter propertiesConverter,
      SparkTransformConverter transformConverter,
      SparkTypeConverter typeConverter,
      DorisCapabilityPolicy capabilityPolicy) {
    super(
        identifier,
        gravitinoTable,
        delegate,
        validatedSchema,
        propertiesConverter,
        transformConverter,
        typeConverter,
        capabilityPolicy);
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    return new GovernedDorisScanBuilder35(newDelegateScanBuilder(options));
  }
}
