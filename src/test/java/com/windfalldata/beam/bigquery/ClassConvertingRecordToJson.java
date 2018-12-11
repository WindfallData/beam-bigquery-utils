package com.windfalldata.beam.bigquery;

import java.util.ArrayList;
import java.util.List;

public class ClassConvertingRecordToJson {

  @BigQueryColumn(convertToJson = true)
  List<ClassToBeConvertedToJson> members = new ArrayList<>();

  public List<ClassToBeConvertedToJson> getMembers() {
    return members;
  }

  public ClassConvertingRecordToJson setMembers(List<ClassToBeConvertedToJson> members) {
    this.members = members;
    return this;
  }
}
