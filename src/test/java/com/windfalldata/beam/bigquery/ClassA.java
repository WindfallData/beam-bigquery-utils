package com.windfalldata.beam.bigquery;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

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

  @Override
  public boolean equals(Object o) {
    if (this == o) { return true; }

    if (o == null || getClass() != o.getClass()) { return false; }

    ClassA classA = (ClassA) o;

    return new EqualsBuilder()
            .append(xxx, classA.xxx)
            .append(foo, classA.foo)
            .append(bar, classA.bar)
            .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
            .append(foo)
            .append(bar)
            .append(xxx)
            .toHashCode();
  }
}
