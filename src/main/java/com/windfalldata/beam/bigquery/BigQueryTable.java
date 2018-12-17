package com.windfalldata.beam.bigquery;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.annotations.VisibleForTesting;
import com.windfalldata.beam.bigquery.BigQueryColumnValueExtractor.ExtractionFunction;
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
