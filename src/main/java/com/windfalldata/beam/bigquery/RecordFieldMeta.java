package com.windfalldata.beam.bigquery;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.windfalldata.beam.bigquery.BigQueryColumnValueExtractor.ValueFunction;

import java.util.List;

import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

class RecordFieldMeta {

  String name;
  String mode;
  String type;
  String description;
  List<RecordFieldMeta> fields;
  ValueFunction fn;

  TableFieldSchema asTableFieldSchema() {
    TableFieldSchema schema = new TableFieldSchema().setName(name).setType(type);
    if (isNotBlank(description)) {
      schema.setDescription(description);
    }
    if (mode != null) {
      schema.setMode(mode);
    }
    if (fields != null) {
      if (fields.isEmpty()) {
        throw new IllegalStateException("Field name \""+name+"\" of type RECORD has an empty schema. " +
                                        "Does this object type have any BigQueryColumn annotated fields/methods?");
      }

      schema.setFields(fields.stream().map(RecordFieldMeta::asTableFieldSchema).collect(toList()));
    }

    return schema;
  }
}
