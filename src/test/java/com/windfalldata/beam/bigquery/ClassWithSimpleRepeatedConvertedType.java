package com.windfalldata.beam.bigquery;

import java.util.List;

public class ClassWithSimpleRepeatedConvertedType {

  @BigQueryColumn(isSimpleCollection = true, convertTo = Long.class)
  private List<String> stringList;

  public List<String> getStringList() {
    return stringList;
  }

  public ClassWithSimpleRepeatedConvertedType setStringList(List<String> stringList) {
    this.stringList = stringList;
    return this;
  }
}
