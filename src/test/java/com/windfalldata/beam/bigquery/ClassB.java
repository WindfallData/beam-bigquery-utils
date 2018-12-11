package com.windfalldata.beam.bigquery;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

@SuppressWarnings({"unused", "WeakerAccess", "SameParameterValue"})
class ClassB {

  enum Order {
    ONE, TWO, THREE
  }

  @BigQueryColumn
  private List<ClassA> ass;

  @BigQueryColumn(name = "yep")
  private boolean verdad;

  @BigQueryColumn(convertTo = Double.class)
  private String dollars;

  @BigQueryColumn(convertToJson = true)
  private Map<String, Object> map;

  @BigQueryColumn
  private Order order;

  @BigQueryColumn
  private LocalDate myDate;

  List<ClassA> getAss() {
    return ass;
  }

  ClassB setAss(List<ClassA> ass) {
    this.ass = ass;
    return this;
  }

  boolean isVerdad() {
    return verdad;
  }

  ClassB setVerdad(boolean verdad) {
    this.verdad = verdad;
    return this;
  }

  public String getDollars() {
    return dollars;
  }

  public ClassB setDollars(String dollars) {
    this.dollars = dollars;
    return this;
  }

  public Map<String, Object> getMap() {
    return map;
  }

  public ClassB setMap(Map<String, Object> map) {
    this.map = map;
    return this;
  }

  public Order getOrder() {
    return order;
  }

  public ClassB setOrder(Order order) {
    this.order = order;
    return this;
  }

  public LocalDate getMyDate() {
    return myDate;
  }

  public ClassB setMyDate(LocalDate myDate) {
    this.myDate = myDate;
    return this;
  }
}
