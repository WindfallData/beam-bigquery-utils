package com.windfalldata.beam.bigquery;

import com.google.api.services.bigquery.model.TableRow;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.beam.sdk.transforms.SimpleFunction;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.windfalldata.beam.bigquery.BigQueryTable.getRecordFieldMeta;
import static java.util.stream.Collectors.toList;

class BigQueryTableObjectConverter<T> extends SimpleFunction<T, TableRow> {

  private final Class<T> type;
  private HashMap<Class, ArrayList<RecordFieldMeta>> fieldMap = new HashMap<>();


  BigQueryTableObjectConverter(Class<T> type) {
    this.type = type;
  }

  @Override
  public TableRow apply(T input) {
    return asTableRow(type, input);
  }

  @VisibleForTesting
  <X> TableRow asTableRow(Class clazz, X input) {
    Preconditions.checkNotNull(clazz, "No class found for input %s.", input);

    ArrayList<RecordFieldMeta> fieldList = fieldMap.get(clazz);
    if (fieldList == null) {
      synchronized (this) {
        fieldList = fieldMap.computeIfAbsent(clazz, this::getMetaForClass);
      }
    }

    TableRow row = new TableRow();
    for (RecordFieldMeta meta : fieldList) {
      row.set(meta.name, meta.fn.extractValue(input));
    }

    return row;
  }

  ArrayList<RecordFieldMeta> getMetaForClass(Class recordType) {
    // Check if this is an Avro SpecificRecord type
    if (SpecificRecord.class.isAssignableFrom(recordType)) {
      return getMetaForAvroClass(recordType);
    }

    ArrayList<RecordFieldMeta> list = new ArrayList<>();

    while (recordType.getSuperclass() != null) {
      for (Field field : recordType.getDeclaredFields()) {
        BigQueryColumn column = field.getAnnotation(BigQueryColumn.class);
        if (column != null) {
          BigQueryColumnValueExtractor valueExtractor = new BigQueryColumnValueExtractor(field, column,
                                                                                         this::asTableRow);
          list.add(getRecordFieldMeta(column, field, (typeArg) -> o -> valueExtractor.extractValue(typeArg, o)));
        }
      }

      for (Method method : recordType.getDeclaredMethods()) {
        BigQueryColumn column = method.getAnnotation(BigQueryColumn.class);
        if (column != null) {
          BigQueryColumnValueExtractor valueExtractor = new BigQueryColumnValueExtractor(method, column,
                                                                                         this::asTableRow);
          list.add(getRecordFieldMeta(column, method, (typeArg) -> o -> valueExtractor.extractValue(typeArg, o)));
        }
      }

      recordType = recordType.getSuperclass();
    }

    return list;
  }

  /**
   * Extracts RecordFieldMeta list for Avro SpecificRecord types.
   * This creates value extraction functions for each field in the Avro schema.
   */
  private ArrayList<RecordFieldMeta> getMetaForAvroClass(Class<?> avroType) {
    try {
      // Get the static SCHEMA$ field from the Avro-generated class
      java.lang.reflect.Field schemaField = avroType.getField("SCHEMA$");
      Schema schema = (Schema) schemaField.get(null);

      ArrayList<RecordFieldMeta> list = new ArrayList<>();
      for (Schema.Field avroField : schema.getFields()) {
        RecordFieldMeta meta = BigQueryTable.convertAvroFieldToRecordFieldMeta(avroField);

        // Set up the value extraction function for this field
        final int fieldPos = avroField.pos();
        final String fieldName = avroField.name();
        meta.fn = (instance) -> {
          if (instance instanceof SpecificRecord) {
            Object value = ((SpecificRecord) instance).get(fieldPos);
            return convertAvroValueForBigQuery(value);
          }
          throw new IllegalArgumentException("Expected SpecificRecord but got: " +
                                            (instance == null ? "null" : instance.getClass()));
        };

        list.add(meta);
      }
      return list;
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new IllegalStateException("Failed to extract Avro schema from type " + avroType.getName(), e);
    }
  }

  /**
   * Recursively converts Avro values to BigQuery-compatible types.
   * This handles nested records, collections, dates, enums, etc.
   */
  private Object convertAvroValueForBigQuery(Object value) {
    if (value == null) {
      return null;
    }

    // Handle nested Avro records - convert to TableRow
    if (value instanceof SpecificRecord) {
      SpecificRecord record = (SpecificRecord) value;
      TableRow tableRow = new TableRow();
      Schema schema = record.getSchema();

      for (Schema.Field field : schema.getFields()) {
        Object fieldValue = record.get(field.pos());
        tableRow.set(field.name(), convertAvroValueForBigQuery(fieldValue));
      }

      return tableRow;
    }

    // Handle collections/arrays
    if (value instanceof Collection) {
      return ((Collection<?>) value).stream()
          .map(this::convertAvroValueForBigQuery)
          .collect(toList());
    }

    // Handle LocalDate
    if (value instanceof LocalDate) {
      return ((LocalDate) value).toString();
    }

    // Handle Enums - use name()
    if (value instanceof Enum) {
      return ((Enum<?>) value).name();
    }

    // Handle CharSequence (Avro's Utf8 class implements this)
    if (value instanceof CharSequence) {
      return value.toString();
    }

    // Primitives and other types pass through
    return value;
  }

}
