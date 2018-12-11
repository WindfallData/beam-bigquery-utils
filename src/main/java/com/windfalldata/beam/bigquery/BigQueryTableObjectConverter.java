package com.windfalldata.beam.bigquery;

import com.google.api.services.bigquery.model.TableRow;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.beam.sdk.transforms.SimpleFunction;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;

import static com.windfalldata.beam.bigquery.BigQueryTable.getRecordFieldMeta;

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

}
