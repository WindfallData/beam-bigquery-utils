package com.windfalldata.beam.bigquery;

import com.google.api.services.bigquery.model.TableRow;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

class MapTableRowTest {

  @Test
  @DisplayName("Test map ClassA")
  void testMapSimpleFields() {
    TableRow row = new TableRow()
        .set("foo", "Foo")
        .set("bar", "Bar")
        .set("xxx", 1)
        ;

    ClassA a = new MapTableRow<>(ClassA.class).map(row);

    assertAll(
        () -> assertEquals(row.get("foo"), a.getFoo()),
        () -> assertEquals(row.get("bar"), a.getBar()),
        () -> assertEquals(row.get("xxx"), a.getXxx())
    );
  }

  @Test
  @DisplayName("Test map ClassB")
  void testMapComplexFields() {
    TableRow row = new TableRow()
            .set("verdad", true)
            .set("dollars", "42.0")
            .set("order", "TWO")
            .set("myDate", "2016-07-11")
            .set("aList", asList(new TableRow().set("foo", "a1"),
                                 new TableRow().set("foo", "a2")))
            ;

    ClassB b = new MapTableRow<>(ClassB.class).map(row);

    assertAll(
        () -> assertEquals(row.get("verdad"), b.isVerdad()),
        () -> assertEquals(row.get("dollars"), b.getDollars()),
        () -> assertEquals(ClassB.Order.TWO, b.getOrder()),
        () -> assertEquals(LocalDate.parse((CharSequence) row.get("myDate")), b.getMyDate()),
        () -> assertEquals(asList(new ClassA().setFoo("a1"), new ClassA().setFoo("a2")),
                           b.getAList())
    );
  }

}
