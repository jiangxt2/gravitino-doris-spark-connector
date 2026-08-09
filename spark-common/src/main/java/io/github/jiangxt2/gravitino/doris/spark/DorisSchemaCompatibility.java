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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.spark.connector.ConnectorConstants;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/** Validates a governed Gravitino Doris schema against the Connector's physical Spark schema. */
public final class DorisSchemaCompatibility {

  private static final Pattern EXTERNAL_DECIMAL_PATTERN =
      Pattern.compile(
          "^(?:decimal|decimalv2|decimal32|decimal64|decimal128|decimal256)\\s*"
              + "\\(\\s*(\\d+)\\s*,\\s*(\\d+)\\s*\\)$",
          Pattern.CASE_INSENSITIVE);

  private DorisSchemaCompatibility() {}

  /**
   * Validates the directional logical-to-physical schema contract and merges logical comments.
   *
   * <p>The returned schema retains physical names, types, order, and nullability. Gravitino column
   * comments are added to the corresponding physical fields after compatibility is established.
   *
   * @param identifier the authorized Spark table identifier
   * @param logicalTable the governed Gravitino table
   * @param physicalSchema the schema loaded from the Doris Connector
   * @param typeConverter the Doris-specific logical-to-physical type converter
   * @return the validated physical schema with Gravitino comments
   */
  public static StructType validateAndMergeComments(
      Identifier identifier,
      Table logicalTable,
      StructType physicalSchema,
      SparkTypeConverter typeConverter) {
    return planReadSchema(identifier, logicalTable, physicalSchema, typeConverter).schema();
  }

  /**
   * Validates the directional schema contract and plans any required Doris SQL normalization.
   *
   * <p>Types that the native Connector represents without loss retain their physical Catalyst type.
   * Doris datetime, binary, complex, unsigned, wide decimal, and external types are read through a
   * Doris SQL projection and exposed as strings, so they remain readable without inheriting
   * ambiguous Catalyst semantics.
   *
   * @param identifier the authorized Spark table identifier
   * @param logicalTable the governed Gravitino table
   * @param physicalSchema the schema loaded from the Doris Connector
   * @param typeConverter the Doris-specific logical-to-physical type converter
   * @return the validated schema and SQL projection plan
   */
  public static DorisReadSchema planReadSchema(
      Identifier identifier,
      Table logicalTable,
      StructType physicalSchema,
      SparkTypeConverter typeConverter) {
    return planReadSchema(
        identifier,
        logicalTable,
        DorisPhysicalSchema.withoutTypeNames(physicalSchema),
        typeConverter);
  }

  /**
   * Validates a physical Doris schema snapshot and plans required SQL normalization.
   *
   * @param identifier the authorized Spark table identifier
   * @param logicalTable the governed Gravitino table
   * @param physicalSchema the Catalyst schema and original Doris FE type names
   * @param typeConverter the Doris-specific logical-to-physical type converter
   * @return the validated schema and SQL projection plan
   */
  public static DorisReadSchema planReadSchema(
      Identifier identifier,
      Table logicalTable,
      DorisPhysicalSchema physicalSchema,
      SparkTypeConverter typeConverter) {
    Column[] logicalColumns = logicalTable.columns();
    StructField[] physicalFields = physicalSchema.schema().fields();
    if (logicalColumns.length != physicalFields.length) {
      throw incompatible(
          identifier,
          String.format(
              "column count differs: logical=%d, physical=%d",
              logicalColumns.length, physicalFields.length));
    }

    List<StructField> validatedFields = new ArrayList<>(logicalColumns.length);
    List<String> projections = new ArrayList<>(logicalColumns.length);
    Set<String> normalizedColumns = new LinkedHashSet<>();
    boolean requiresSqlExecution = false;
    for (int index = 0; index < logicalColumns.length; index++) {
      Column logicalColumn = logicalColumns[index];
      StructField physicalField = physicalFields[index];
      String dorisTypeName = physicalSchema.dorisTypeName(index);
      validateColumnIdentity(identifier, logicalColumn, physicalField, index);

      MetadataBuilder metadataBuilder =
          new MetadataBuilder().withMetadata(physicalField.metadata());
      if (logicalColumn.comment() != null) {
        metadataBuilder.putString(ConnectorConstants.COMMENT, logicalColumn.comment());
      }
      if (requiresStringNormalization(
          logicalColumn.dataType(), physicalField.dataType(), dorisTypeName)) {
        validateNormalizedType(
            identifier, logicalColumn, physicalField, dorisTypeName, typeConverter);
        validatedFields.add(
            DataTypes.createStructField(
                physicalField.name(),
                DataTypes.StringType,
                physicalField.nullable(),
                metadataBuilder.build()));
        projections.add(
            normalizationProjection(logicalColumn.dataType(), physicalField.name(), dorisTypeName));
        normalizedColumns.add(physicalField.name());
        requiresSqlExecution = true;
      } else {
        validateDirectType(identifier, logicalColumn, physicalField, typeConverter);
        validatedFields.add(
            DataTypes.createStructField(
                physicalField.name(),
                physicalField.dataType(),
                physicalField.nullable(),
                metadataBuilder.build()));
        projections.add(DorisReadSchema.quoteIdentifier(physicalField.name()));
      }
    }
    return new DorisReadSchema(
        DataTypes.createStructType(validatedFields),
        projections,
        requiresSqlExecution,
        normalizedColumns);
  }

  private static void validateColumnIdentity(
      Identifier identifier, Column logicalColumn, StructField physicalField, int index) {
    if (!logicalColumn.name().equalsIgnoreCase(physicalField.name())) {
      throw incompatible(
          identifier,
          String.format(
              "column %d name differs: logical=%s, physical=%s",
              index, logicalColumn.name(), physicalField.name()));
    }

    // Connector 26.0.0 reports every loaded field as nullable. A stricter Gravitino field may
    // therefore map to a nullable physical field, but the reverse direction is unsafe.
    if (logicalColumn.nullable() && !physicalField.nullable()) {
      throw incompatible(
          identifier,
          String.format(
              "column %s nullability differs: logical is nullable but physical is not nullable",
              logicalColumn.name()));
    }
  }

  private static void validateDirectType(
      Identifier identifier,
      Column logicalColumn,
      StructField physicalField,
      SparkTypeConverter typeConverter) {
    Type logicalType = logicalColumn.dataType();
    if (logicalType instanceof Types.ExternalType
        && isExternalDecimal(externalBaseType((Types.ExternalType) logicalType))
        && physicalField.dataType() instanceof DecimalType) {
      validateExternalDecimal(
          identifier,
          logicalColumn,
          (Types.ExternalType) logicalType,
          (DecimalType) physicalField.dataType());
      return;
    }
    DataType expectedPhysicalType;
    try {
      expectedPhysicalType = typeConverter.toSparkType(logicalType);
    } catch (RuntimeException e) {
      throw incompatible(
          identifier,
          String.format(
              "column %s cannot map logical type %s to a Doris execution type",
              logicalColumn.name(), logicalType.simpleString()));
    }
    if (!expectedPhysicalType.equals(physicalField.dataType())) {
      throw incompatible(
          identifier,
          String.format(
              "column %s type differs: logical=%s, expected physical=%s, actual physical=%s",
              logicalColumn.name(),
              logicalType.simpleString(),
              expectedPhysicalType.catalogString(),
              physicalField.dataType().catalogString()));
    }
  }

  private static void validateNormalizedType(
      Identifier identifier,
      Column logicalColumn,
      StructField physicalField,
      String dorisTypeName,
      SparkTypeConverter typeConverter) {
    String physicalBaseType = dorisBaseType(dorisTypeName);
    if (logicalColumn.dataType() instanceof Types.ExternalType
        && isExternalDecimal(externalBaseType((Types.ExternalType) logicalColumn.dataType()))
        && isExternalDecimal(physicalBaseType)) {
      validateNormalizedExternalDecimal(
          identifier, logicalColumn, (Types.ExternalType) logicalColumn.dataType(), dorisTypeName);
      return;
    }
    if ((physicalBaseType.isEmpty()
            && isCompatibleNormalizedPhysicalType(
                logicalColumn.dataType(), physicalField.dataType(), typeConverter))
        || isJdbcLossyNormalizedType(physicalBaseType)
        || isCompatibleNormalizedType(logicalColumn.dataType(), physicalBaseType)
        || (isGenericJdbcPlaceholder(logicalColumn.dataType())
            && !isStrictNormalizedType(physicalBaseType))) {
      return;
    }
    throw incompatible(
        identifier,
        String.format(
            "column %s type differs: logical=%s, actual Doris type=%s",
            logicalColumn.name(), logicalColumn.dataType().simpleString(), dorisTypeName));
  }

  private static boolean isCompatibleNormalizedPhysicalType(
      Type logicalType, DataType physicalType, SparkTypeConverter typeConverter) {
    if (logicalType instanceof Types.ExternalType) {
      return DataTypes.StringType.equals(physicalType);
    }
    if (logicalType instanceof Types.TimestampType) {
      return DataTypes.TimestampType.equals(physicalType);
    }
    try {
      return typeConverter.toSparkType(logicalType).equals(physicalType);
    } catch (RuntimeException e) {
      return false;
    }
  }

  private static boolean isJdbcLossyNormalizedType(String physicalBaseType) {
    // The Gravitino 1.3 Doris catalog obtains its logical schema through JDBC metadata. JDBC is
    // known to report ARRAY as its element scalar, lose MAP/STRUCT containers, report LARGEINT as
    // INTEGER, and expose sketches through generic MySQL types. These proven lossy families use
    // the FE type as the String/base64 execution authority.
    return "array".equals(physicalBaseType)
        || "map".equals(physicalBaseType)
        || "struct".equals(physicalBaseType)
        || "largeint".equals(physicalBaseType)
        || "bitmap".equals(physicalBaseType)
        || "hll".equals(physicalBaseType);
  }

  private static boolean isStrictNormalizedType(String physicalBaseType) {
    return "datetime".equals(physicalBaseType)
        || "datetimev2".equals(physicalBaseType)
        || "binary".equals(physicalBaseType)
        || "varbinary".equals(physicalBaseType)
        || physicalBaseType.endsWith(" unsigned");
  }

  private static boolean isGenericJdbcPlaceholder(Type logicalType) {
    if (!(logicalType instanceof Types.ExternalType)) {
      return false;
    }
    String logicalBaseType = externalBaseType((Types.ExternalType) logicalType);
    return "unknown".equals(logicalBaseType) || "other".equals(logicalBaseType);
  }

  private static boolean isCompatibleNormalizedType(Type logicalType, String physicalBaseType) {
    if (logicalType instanceof Types.TimestampType) {
      return "datetime".equals(physicalBaseType) || "datetimev2".equals(physicalBaseType);
    }
    if (logicalType instanceof Types.BinaryType) {
      return "binary".equals(physicalBaseType) || "varbinary".equals(physicalBaseType);
    }
    if (logicalType instanceof Types.ListType) {
      return "array".equals(physicalBaseType);
    }
    if (logicalType instanceof Types.MapType) {
      return "map".equals(physicalBaseType);
    }
    if (logicalType instanceof Types.StructType) {
      return "struct".equals(physicalBaseType);
    }
    if (logicalType instanceof Type.IntegralType && !((Type.IntegralType) logicalType).signed()) {
      return physicalBaseType.endsWith(" unsigned");
    }
    if (logicalType instanceof Types.ExternalType) {
      String logicalBaseType = externalBaseType((Types.ExternalType) logicalType);
      return logicalBaseType.equals(physicalBaseType)
          || (("json".equals(logicalBaseType) || "jsonb".equals(logicalBaseType))
              && ("json".equals(physicalBaseType) || "jsonb".equals(physicalBaseType)))
          || (isExternalDecimal(logicalBaseType) && "decimal256".equals(physicalBaseType));
    }
    return false;
  }

  private static boolean requiresStringNormalization(
      Type type, DataType physicalType, String dorisTypeName) {
    String baseType = dorisBaseType(dorisTypeName);
    if (!baseType.isEmpty()) {
      if (isExternalDecimal(baseType) && !(physicalType instanceof DecimalType)) {
        // Connector 26.0.0 cannot construct a Catalyst DecimalType above Spark's precision limit.
        // This includes legacy Doris decimal family names, not only DECIMAL256.
        return true;
      }
      return !isLosslessDirectDorisType(baseType);
    }
    if (type instanceof Type.IntegralType && !((Type.IntegralType) type).signed()) {
      return true;
    }
    if (type instanceof Types.TimestampType
        || type instanceof Types.BinaryType
        || type instanceof Type.ComplexType) {
      return true;
    }
    if (type instanceof Types.ExternalType) {
      String externalType = externalBaseType((Types.ExternalType) type);
      return !isExternalDecimal(externalType) || !(physicalType instanceof DecimalType);
    }
    return false;
  }

  private static String normalizationProjection(
      Type type, String physicalName, String dorisTypeName) {
    String quotedName = DorisReadSchema.quoteIdentifier(physicalName);
    String baseType = dorisBaseType(dorisTypeName);
    String expression;
    if ("binary".equals(baseType)
        || "varbinary".equals(baseType)
        || (baseType.isEmpty() && type instanceof Types.BinaryType)) {
      expression = "TO_BASE64(" + quotedName + ")";
    } else if ("bitmap".equals(baseType)
        || (baseType.isEmpty()
            && type instanceof Types.ExternalType
            && "bitmap".equals(externalBaseType((Types.ExternalType) type)))) {
      expression = "BITMAP_TO_BASE64(" + quotedName + ")";
    } else if ("hll".equals(baseType)
        || (baseType.isEmpty()
            && type instanceof Types.ExternalType
            && "hll".equals(externalBaseType((Types.ExternalType) type)))) {
      expression = "HLL_TO_BASE64(" + quotedName + ")";
    } else {
      // Doris 3.0 cannot CAST ARRAY/MAP/STRUCT directly to STRING. Keep the original Doris value
      // in the SQL projection and let Spark JDBC read it through ResultSet.getString according to
      // the adapter-provided StringType schema.
      expression = quotedName;
    }
    return expression + " AS " + quotedName;
  }

  private static String externalBaseType(Types.ExternalType type) {
    return dorisBaseType(type.catalogString());
  }

  private static void validateExternalDecimal(
      Identifier identifier,
      Column logicalColumn,
      Types.ExternalType logicalType,
      DecimalType physicalType) {
    Matcher matcher = EXTERNAL_DECIMAL_PATTERN.matcher(logicalType.catalogString().trim());
    if (!matcher.matches()) {
      return;
    }

    int logicalPrecision;
    int logicalScale;
    try {
      logicalPrecision = Integer.parseInt(matcher.group(1));
      logicalScale = Integer.parseInt(matcher.group(2));
    } catch (NumberFormatException e) {
      throw incompatible(
          identifier,
          String.format(
              "column %s has invalid external decimal type %s",
              logicalColumn.name(), logicalType.catalogString()));
    }
    if (logicalPrecision != physicalType.precision() || logicalScale != physicalType.scale()) {
      throw incompatible(
          identifier,
          String.format(
              "column %s decimal precision or scale differs: logical=(%d,%d), physical=(%d,%d)",
              logicalColumn.name(),
              logicalPrecision,
              logicalScale,
              physicalType.precision(),
              physicalType.scale()));
    }
  }

  private static void validateNormalizedExternalDecimal(
      Identifier identifier,
      Column logicalColumn,
      Types.ExternalType logicalType,
      String physicalTypeName) {
    Matcher logical = EXTERNAL_DECIMAL_PATTERN.matcher(logicalType.catalogString().trim());
    Matcher physical =
        EXTERNAL_DECIMAL_PATTERN.matcher(physicalTypeName == null ? "" : physicalTypeName.trim());
    // Older compatibility catalogs may retain only the FE base type. The production Spark 3.5
    // adapter includes FE precision and scale, so real reads take the strict branch below.
    if (!logical.matches() || !physical.matches()) {
      return;
    }
    if (!logical.group(1).equals(physical.group(1))
        || !logical.group(2).equals(physical.group(2))) {
      throw incompatible(
          identifier,
          String.format(
              "column %s decimal precision or scale differs: logical=(%s,%s), physical=(%s,%s)",
              logicalColumn.name(),
              logical.group(1),
              logical.group(2),
              physical.group(1),
              physical.group(2)));
    }
  }

  private static String dorisBaseType(String typeName) {
    String catalogString = typeName == null ? "" : typeName.trim().toLowerCase(Locale.ROOT);
    int parameterStart = catalogString.indexOf('(');
    int complexStart = catalogString.indexOf('<');
    int suffixStart;
    if (parameterStart < 0) {
      suffixStart = complexStart;
    } else if (complexStart < 0) {
      suffixStart = parameterStart;
    } else {
      suffixStart = Math.min(parameterStart, complexStart);
    }
    return suffixStart < 0 ? catalogString : catalogString.substring(0, suffixStart).trim();
  }

  private static boolean isLosslessDirectDorisType(String baseType) {
    switch (baseType) {
      case "boolean":
      case "tinyint":
      case "smallint":
      case "int":
      case "integer":
      case "bigint":
      case "float":
      case "double":
      case "decimal":
      case "decimalv2":
      case "decimal32":
      case "decimal64":
      case "decimal128":
      case "date":
      case "datev2":
      case "char":
      case "varchar":
      case "string":
      case "text":
        return true;
      default:
        return false;
    }
  }

  private static boolean isExternalDecimal(String baseType) {
    return "decimal".equals(baseType)
        || "decimalv2".equals(baseType)
        || "decimal32".equals(baseType)
        || "decimal64".equals(baseType)
        || "decimal128".equals(baseType)
        || "decimal256".equals(baseType);
  }

  private static IllegalArgumentException incompatible(Identifier identifier, String detail) {
    return new IllegalArgumentException(
        String.format("Incompatible Doris schema for %s: %s", identifier, detail));
  }
}
