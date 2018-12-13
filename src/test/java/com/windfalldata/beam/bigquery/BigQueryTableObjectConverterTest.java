package com.windfalldata.beam.bigquery;

import com.google.api.services.bigquery.model.TableRow;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.ArrayList;

import static com.google.common.collect.Lists.newArrayList;
import static com.windfalldata.beam.bigquery.ClassB.Order.TWO;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BigQueryTableObjectConverterTest {

  @Test
  void testGetMetaForSimpleClass() {
    BigQueryTableObjectConverter<Void> converter = new BigQueryTableObjectConverter<>(Void.class);
    ArrayList<RecordFieldMeta> meta = converter.getMetaForClass(ClassA.class);

    assertEquals(meta.size(), 2);
    assertTrue(meta.stream().anyMatch(m -> "foo".equals(m.name)));
    assertTrue(meta.stream().anyMatch(m -> "myInt".equals(m.name)));
  }

  @Test
  void testGetMetaForSimpleClassWithAnnotatedMethod() {
    BigQueryTableObjectConverter<Void> converter = new BigQueryTableObjectConverter<>(Void.class);
    ArrayList<RecordFieldMeta> meta = converter.getMetaForClass(ClassAPrime.class);

    assertEquals(meta.size(), 2);
    assertTrue(meta.stream().anyMatch(m -> "foo".equals(m.name)));
    assertTrue(meta.stream().anyMatch(m -> "myInt".equals(m.name)));
  }

  @Test
  void testGetMetaForComplexClass() {
    BigQueryTableObjectConverter<Void> converter = new BigQueryTableObjectConverter<>(Void.class);
    ArrayList<RecordFieldMeta> meta = converter.getMetaForClass(ClassB.class);

    assertEquals(meta.size(), 6);
    assertTrue(meta.stream().anyMatch(m -> "aList".equals(m.name)));
    assertTrue(meta.stream().anyMatch(m -> "yep".equals(m.name)));
    assertTrue(meta.stream().anyMatch(m -> "dollars".equals(m.name)));
    assertTrue(meta.stream().anyMatch(m -> "map".equals(m.name)));
    assertTrue(meta.stream().anyMatch(m -> "order".equals(m.name)));
    assertTrue(meta.stream().anyMatch(m -> "myDate".equals(m.name)));
  }

  @Test
  void testAsTableRowForSimpleClass() {
    ClassA a = new ClassA()
            .setFoo("abc")
            .setBar("xyz")
            .setXxx(42);

    BigQueryTableObjectConverter<Void> converter = new BigQueryTableObjectConverter<>(Void.class);
    TableRow row = converter.asTableRow(ClassA.class, a);
    assertEquals(row, new TableRow().set("foo", "abc").set("myInt", 42));
  }

  @Test
  void testAsTableRowForSimpleClassWithAnnotatedMethod() {
    ClassAPrime a = new ClassAPrime()
            .setFoo("abc")
            .setBar("xyz")
            .setXxx(42);

    BigQueryTableObjectConverter<Void> converter = new BigQueryTableObjectConverter<>(Void.class);
    TableRow row = converter.asTableRow(ClassAPrime.class, a);
    assertEquals(row, new TableRow().set("foo", "abc").set("myInt", 42));
  }

  @Test
  void testAsTableRowForComplexClass() {
    ClassA a1 = new ClassA()
            .setFoo("abc")
            .setBar("xyz")
            .setXxx(42);
    ClassA a2 = new ClassA()
            .setFoo(null)
            .setBar("xyz")
            .setXxx(-1);

    ImmutableMap<String, Object> map = ImmutableMap.of("hey", "sup");
    ClassB b = new ClassB()
            .setAList(newArrayList(a1, a2))
            .setVerdad(false)
            .setDollars("1234.56")
            .setMap(map)
            .setMyDate(LocalDate.parse("1980-07-01"))
            .setOrder(TWO);

    BigQueryTableObjectConverter<Void> converter = new BigQueryTableObjectConverter<>(Void.class);
    TableRow row = converter.asTableRow(ClassB.class, b);
    assertEquals(row, new TableRow()
            .set("yep", false)
            .set("dollars", 1234.56)
            .set("map", new Gson().toJson(map))
            .set("order", "TWO")
            .set("aList", newArrayList(
                    new TableRow().set("foo", "abc").set("myInt", 42),
                    new TableRow().set("foo", null).set("myInt", -1)
            ))
            .set("myDate", "1980-07-01"));
  }

  @Test
  @DisplayName("Test map value for repeated simple type")
  void testMapValueForSimpleType() {
    BigQueryTableObjectConverter<Void> converter = new BigQueryTableObjectConverter<>(Void.class);
    TableRow row = converter.asTableRow(ClassWithSimpleRepeatedType.class, new ClassWithSimpleRepeatedType()
        .setTheStrings(newArrayList("string1")).setTheIntegers(newArrayList(1)));
    assertEquals(row, new TableRow().set("theStrings", newArrayList("string1")).set("theIntegers", newArrayList(1)));
  }

  @Test
  @DisplayName("Test map converted value for repeated simple type")
  void testMapConvertedRepeatedSimpleValue() {
    BigQueryTableObjectConverter<Void> converter = new BigQueryTableObjectConverter<>(Void.class);
    ClassWithSimpleRepeatedConvertedType object = new ClassWithSimpleRepeatedConvertedType()
        .setStringList(newArrayList("25"));
    TableRow row = converter.asTableRow(ClassWithSimpleRepeatedConvertedType.class, object);
    assertEquals(row, new TableRow().set("stringList", newArrayList("25")));
  }

}
