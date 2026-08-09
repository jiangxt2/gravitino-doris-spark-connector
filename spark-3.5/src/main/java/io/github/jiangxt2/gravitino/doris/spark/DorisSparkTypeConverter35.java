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

import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.spark.connector.jdbc.SparkJdbcTypeConverter;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.TimestampType;

/** Doris-specific logical-to-execution type mappings for Spark 3.5. */
public class DorisSparkTypeConverter35 extends SparkJdbcTypeConverter {

  @Override
  public Type toGravitinoType(DataType sparkType) {
    if (sparkType instanceof TimestampType) {
      // Doris DATETIME is timezone-naive even though Connector 26.0.0 exposes TimestampType.
      return Types.TimestampType.withoutTimeZone();
    }
    return super.toGravitinoType(sparkType);
  }

  @Override
  public DataType toSparkType(Type gravitinoType) {
    if (gravitinoType instanceof Types.FixedCharType
        || gravitinoType instanceof Types.VarCharType) {
      return DataTypes.StringType;
    }
    if (gravitinoType instanceof Types.TimestampType
        && !((Types.TimestampType) gravitinoType).hasTimeZone()) {
      // Doris DATETIME is normalized by the SQL lane, whose physical planning type is timestamp.
      return DataTypes.TimestampType;
    }
    if (gravitinoType instanceof Types.ExternalType) {
      return DataTypes.StringType;
    }
    return super.toSparkType(gravitinoType);
  }
}
