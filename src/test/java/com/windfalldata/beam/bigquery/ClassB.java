package com.windfalldata.beam.bigquery;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

@SuppressWarnings({"unused", "WeakerAccess", "SameParameterValue"})
class ClassB {

  enum Order {
    ONE, TWO, THREE
  }

  @BigQueryColumn
  private List<ClassA> aList;

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

  List<ClassA> getAList() {
    return aList;
  }

  ClassB setAList(List<ClassA> aList) {
    this.aList = aList;
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

  @Override
  public boolean equals(Object o) {
    if (this == o) { return true; }

    if (o == null || getClass() != o.getClass()) { return false; }

    ClassB classB = (ClassB) o;

    return new EqualsBuilder()
            .append(verdad, classB.verdad)
            .append(aList, classB.aList)
            .append(dollars, classB.dollars)
            .append(map, classB.map)
            .append(order, classB.order)
            .append(myDate, classB.myDate)
            .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
            .append(aList)
            .append(verdad)
            .append(dollars)
            .append(map)
            .append(order)
            .append(myDate)
            .toHashCode();
  }
}
