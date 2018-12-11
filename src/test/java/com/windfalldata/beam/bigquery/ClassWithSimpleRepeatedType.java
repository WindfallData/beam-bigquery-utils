package com.windfalldata.beam.bigquery;

import java.util.ArrayList;
import java.util.List;

public class ClassWithSimpleRepeatedType {

  @BigQueryColumn(isSimpleCollection = true)
  private List<String> theStrings = new ArrayList<>();

  @BigQueryColumn(isSimpleCollection = true)
  private List<Integer> theIntegers = new ArrayList<>();

  public List<String> getTheStrings() {
    return theStrings;
  }

  public ClassWithSimpleRepeatedType setTheStrings(List<String> theStrings) {
    this.theStrings = theStrings;
    return this;
  }

  public List<Integer> getTheIntegers() {
    return theIntegers;
  }

  public ClassWithSimpleRepeatedType setTheIntegers(List<Integer> theIntegers) {
    this.theIntegers = theIntegers;
    return this;
  }
}
