package com.windfalldata.beam.bigquery;

@SuppressWarnings("unused")
class ClassAPrime {

  private String foo;
  private String bar;
  private int xxx;

  @BigQueryColumn
  String getFoo() {
    return foo;
  }

  ClassAPrime setFoo(String foo) {
    this.foo = foo;
    return this;
  }

  String getBar() {
    return bar;
  }

  ClassAPrime setBar(String bar) {
    this.bar = bar;
    return this;
  }

  @BigQueryColumn(name = "myInt", required = true)
  int getXxx() {
    return xxx;
  }

  ClassAPrime setXxx(int xxx) {
    this.xxx = xxx;
    return this;
  }
}
