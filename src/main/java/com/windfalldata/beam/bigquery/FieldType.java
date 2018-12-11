package com.windfalldata.beam.bigquery;

public enum FieldType {

  STRING,
  BOOLEAN,
  FLOAT,
  INTEGER,
  RECORD,
  DATE;

  public String getTypeName() {
    return this.name();
  }

}
