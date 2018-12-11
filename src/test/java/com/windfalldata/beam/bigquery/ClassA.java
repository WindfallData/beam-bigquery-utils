package com.windfalldata.beam.bigquery;

@SuppressWarnings("unused")
class ClassA {

  @BigQueryColumn
  private String foo;
  private String bar;
  @BigQueryColumn(name = "myInt", required = true)
  private int xxx;

  String getFoo() {
    return foo;
  }

  ClassA setFoo(String foo) {
    this.foo = foo;
    return this;
  }

  String getBar() {
    return bar;
  }

  ClassA setBar(String bar) {
    this.bar = bar;
    return this;
  }

  int getXxx() {
    return xxx;
  }

  ClassA setXxx(int xxx) {
    this.xxx = xxx;
    return this;
  }
}
