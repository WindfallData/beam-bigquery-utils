package com.windfalldata.beam.bigquery;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.annotations.VisibleForTesting;
import com.windfalldata.beam.bigquery.BigQueryColumnValueExtractor.ExtractionFunction;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static com.windfalldata.beam.bigquery.FieldType.*;
import static org.apache.commons.lang3.StringUtils.isBlank;

public class BigQueryTable<T> {

  @VisibleForTesting static final String MODE_REQUIRED = "REQUIRED";
  @VisibleForTesting static final String MODE_REPEATED = "REPEATED";

  /**
   * Generates a {@link TableSchema} for the provided Class.
   * <p>
   * The class should be annotated with one ore more {@link BigQueryColumn} annotations which will be used as
   * the columns in the schema for the table.
   * </p>
   * <p>
   * <em>NOTE:</em> it is an error to serialize an object (including nested objects within collections) that are
   * not annotated with at least one {@code BigQueryColumn}.
   * </p>
   *
   * @see BigQueryColumn
   * @see BigQueryIOWriterTransform
   */
  @SuppressWarnings("unused")
  public static <T> TableSchema getSchemaForClass(Class<T> clazz) {
    return new BigQueryTable<>(clazz).inferSchema();
  }

  private final Class<T> type;

  private BigQueryTable(Class<T> type) {
    this.type = type;
  }

  /**
   * Infers the TableSchema of the Class type, including parameterized collections.
   */
  private TableSchema inferSchema() {
    final TableSchema schema = new TableSchema();
    List<TableFieldSchema> fields = getRecordFieldMetaListForType(type).stream()
                                                                       .map(RecordFieldMeta::asTableFieldSchema)
                                                                       .collect(Collectors.toList());
    if (fields.isEmpty()) {
      throw new IllegalStateException("No BigQuery schema fields found for type " + type);
    }
    schema.setFields(fields);
    return schema;
  }

  static List<RecordFieldMeta> getRecordFieldMetaListForType(Class<?> recordType) {
    // Check if this is an Avro SpecificRecord type
    if (SpecificRecord.class.isAssignableFrom(recordType)) {
      return getRecordFieldMetaListFromAvroSchema(recordType);
    }

    ArrayList<RecordFieldMeta> list = new ArrayList<>();

    while (recordType.getSuperclass() != null) {
      for (Field field : recordType.getDeclaredFields()) {
        BigQueryColumn column = field.getAnnotation(BigQueryColumn.class);
        if (column != null) {
          list.add(getRecordFieldMeta(column, field, null));
        }
      }

      for (Method method : recordType.getDeclaredMethods()) {
        BigQueryColumn column = method.getAnnotation(BigQueryColumn.class);
        if (column != null) {
          list.add(getRecordFieldMeta(column, method, null));
        }
      }
      recordType = recordType.getSuperclass();
    }

    return list;
  }

  /**
   * Converts an Avro SpecificRecord schema to a list of RecordFieldMeta for BigQuery.
   */
  private static List<RecordFieldMeta> getRecordFieldMetaListFromAvroSchema(Class<?> avroType) {
    try {
      // Get the static SCHEMA$ field from the Avro-generated class
      java.lang.reflect.Field schemaField = avroType.getField("SCHEMA$");
      Schema schema = (Schema) schemaField.get(null);

      ArrayList<RecordFieldMeta> list = new ArrayList<>();
      for (Schema.Field avroField : schema.getFields()) {
        list.add(convertAvroFieldToRecordFieldMeta(avroField));
      }
      return list;
    } catch (Exception e) {
      throw new IllegalStateException("Failed to extract Avro schema from type " + avroType.getName(), e);
    }
  }

  /**
   * Converts a single Avro field to RecordFieldMeta for BigQuery.
   */
  static RecordFieldMeta convertAvroFieldToRecordFieldMeta(Schema.Field avroField) {
    RecordFieldMeta meta = new RecordFieldMeta();
    meta.name = avroField.name();
    meta.description = avroField.doc();

    Schema fieldSchema = avroField.schema();

    // Handle union types (e.g., ["null", "string"])
    if (fieldSchema.getType() == Schema.Type.UNION) {
      // Find the non-null type in the union (BigQuery doesn't support complex unions)
      Schema nonNullSchema = null;
      for (Schema unionType : fieldSchema.getTypes()) {
        if (unionType.getType() != Schema.Type.NULL) {
          if (nonNullSchema != null) {
            throw new IllegalArgumentException(
              "Field '" + avroField.name() + "' has a complex union with multiple non-null types. " +
              "BigQuery only supports [null, type] unions for optional fields.");
          }
          nonNullSchema = unionType;
        }
      }
      if (nonNullSchema != null) {
        fieldSchema = nonNullSchema;
      }
      // Union types are nullable (NULLABLE is the default mode when not specified)
      // meta.mode left as null, which BigQuery interprets as NULLABLE
    } else {
      // Non-union types are required in BigQuery
      meta.mode = MODE_REQUIRED;
    }

    // Convert Avro type to BigQuery type
    switch (fieldSchema.getType()) {
      case STRING:
      case ENUM:
        meta.type = STRING.getTypeName();
        break;
      case INT:
      case LONG:
        // Check for logical types (date, timestamp, etc.)
        if (fieldSchema.getLogicalType() != null) {
          if (fieldSchema.getLogicalType() instanceof LogicalTypes.Date) {
            meta.type = DATE.getTypeName();
          } else if (fieldSchema.getLogicalType() instanceof LogicalTypes.TimestampMillis ||
                     fieldSchema.getLogicalType() instanceof LogicalTypes.TimestampMicros) {
            meta.type = "TIMESTAMP";
          } else {
            meta.type = INTEGER.getTypeName();
          }
        } else {
          meta.type = INTEGER.getTypeName();
        }
        break;
      case FLOAT:
      case DOUBLE:
        meta.type = FLOAT.getTypeName();
        break;
      case BOOLEAN:
        meta.type = BOOLEAN.getTypeName();
        break;
      case BYTES:
      case FIXED:
        meta.type = "BYTES";
        break;
      case ARRAY:
        meta.mode = MODE_REPEATED;
        Schema elementType = fieldSchema.getElementType();
        if (elementType.getType() == Schema.Type.RECORD) {
          meta.type = RECORD.getTypeName();
          meta.fields = new ArrayList<>();
          for (Schema.Field nestedField : elementType.getFields()) {
            meta.fields.add(convertAvroFieldToRecordFieldMeta(nestedField));
          }
        } else {
          meta.type = convertAvroTypeToString(elementType);
        }
        break;
      case RECORD:
        meta.type = RECORD.getTypeName();
        meta.fields = new ArrayList<>();
        for (Schema.Field nestedField : fieldSchema.getFields()) {
          meta.fields.add(convertAvroFieldToRecordFieldMeta(nestedField));
        }
        break;
      default:
        throw new IllegalArgumentException("Unsupported Avro type: " + fieldSchema.getType());
    }

    return meta;
  }

  /**
   * Helper method to convert simple Avro types to BigQuery type strings.
   */
  private static String convertAvroTypeToString(Schema schema) {
    switch (schema.getType()) {
      case STRING:
      case ENUM:
        return STRING.getTypeName();
      case INT:
      case LONG:
        return INTEGER.getTypeName();
      case FLOAT:
      case DOUBLE:
        return FLOAT.getTypeName();
      case BOOLEAN:
        return BOOLEAN.getTypeName();
      case BYTES:
      case FIXED:
        return "BYTES";
      default:
        throw new IllegalArgumentException("Unsupported Avro type: " + schema.getType());
    }
  }

  static RecordFieldMeta getRecordFieldMeta(BigQueryColumn column, Field field, ExtractionFunction fn) {
    return getRecordFieldMeta(column, field.getName(), field.getType(), field.getGenericType(), fn);
  }

  static RecordFieldMeta getRecordFieldMeta(BigQueryColumn column, Method method, ExtractionFunction fn) {
    String name = StringUtils.uncapitalize(method.getName().replaceFirst("^get([A-Z])", "$1"));
    return getRecordFieldMeta(column, name, method.getReturnType(), method.getGenericReturnType(), fn);
  }

  private static RecordFieldMeta getRecordFieldMeta(BigQueryColumn column, String defaultName,
                                                    Class<?> type, Type genericType, @Nullable ExtractionFunction fn) {
    RecordFieldMeta meta = new RecordFieldMeta();
    meta.name = isBlank(column.name()) ? defaultName : column.name();
    meta.description = column.description();
    if (column.required()) {
      if (column.stripToNull()) {
        throw new IllegalArgumentException("Column cannot be marked as required and also \"stripToNull\" = true");
      }
      meta.mode = MODE_REQUIRED;
    }

    if (fn != null) {
      // type not needed unless its a collection, which is handled below
      meta.fn = fn.bindType(null);
    }

    if (column.convertToJson()) {
      // needs to come first so that repeated fields are converted to JSON
      meta.type = deriveConversionType(column.convertTo()).getTypeName();
    } else if (isSupportedCollection(type)) {
      checkState(genericType instanceof ParameterizedType,
                 "BigQueryColumn annotated collection types must be parameterized: Offending %s", meta.name);

      Type[] typeArgs = ((ParameterizedType) genericType).getActualTypeArguments();
      checkState(typeArgs.length == 1, "Found %s type arguments, expected 1", typeArgs.length);

      Class typeArg = (Class) typeArgs[0];
      if (column.isSimpleCollection()) {
        meta.mode = MODE_REPEATED;
        meta.type = deriveType(typeArg, column.convertTo()).getTypeName();
      } else {
        meta.type = RECORD.getTypeName();
        meta.mode = MODE_REPEATED;
        if (fn == null) {
          // this is a recursive call to build the whole tree
          meta.fields = getRecordFieldMetaListForType((Class<?>) typeArgs[0]);
        } else {
          // this is not recursive and binds the parameterized type to the extraction function
          meta.fn = fn.bindType(typeArg);
        }
      }
    } else {
      meta.type = deriveType(type, column.convertTo()).getTypeName();
      if (meta.type.equals(RECORD.getTypeName())) {
        if (fn == null) {
          // this is a recursive call to build the whole tree
          meta.fields = getRecordFieldMetaListForType(type);
        } else {
          // this is not recursive and binds the parameterized type to the extraction function
          meta.fn = fn.bindType(type);
        }
      }
    }
    return meta;
  }

  private static boolean isSupportedCollection(Class c) {
    return List.class.isAssignableFrom(c) || Set.class.isAssignableFrom(c);
  }

  private static FieldType deriveType(Class c, Class conversionTarget) {
    if (String.class.isAssignableFrom(c) || c.isEnum()) {
      return deriveConversionType(conversionTarget);
    } else if (Integer.class.isAssignableFrom(c) || Integer.TYPE.isAssignableFrom(c)) {
      return INTEGER;
    } else if (Long.class.isAssignableFrom(c) || Long.TYPE.isAssignableFrom(c)) {
      return INTEGER;
    } else if (Double.class.isAssignableFrom(c) || Double.TYPE.isAssignableFrom(c)) {
      return FLOAT;
    } else if (Boolean.class.isAssignableFrom(c) || Boolean.TYPE.isAssignableFrom(c)) {
      return BOOLEAN;
    } else if (Character.class.isAssignableFrom(c) || Character.TYPE.isAssignableFrom(c)) {
      return STRING;
    } else if (LocalDate.class.isAssignableFrom(c)) {
      return DATE;
    } else {
      return RECORD;
    }
  }

  private static FieldType deriveConversionType(Class conversionTarget) {
    if (conversionTarget == Void.class) {
      return STRING;
    } else if (conversionTarget == Long.class) {
      return INTEGER;
    } else if (conversionTarget == Double.class) {
      return FLOAT;
    } else if (conversionTarget == Boolean.class) {
      return BOOLEAN;
    } else {
      throw new IllegalStateException("Unsupported BigQuery convertTo type: " + conversionTarget);
    }
  }

}
