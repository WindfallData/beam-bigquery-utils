package com.windfalldata.beam.bigquery;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.common.collect.Iterables;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static com.windfalldata.beam.bigquery.BigQueryTable.*;
import static org.junit.jupiter.api.Assertions.*;

class BigQueryTableTest {

  @Test
  void testRecordFieldMetaListForSimpleType() {
    List<TableFieldSchema> list = schema(ClassA.class);
    assertEquals(list, newArrayList(
            new TableFieldSchema().setName("foo").setType("STRING"),
            new TableFieldSchema().setName("myInt").setType("INTEGER").setMode(MODE_REQUIRED)));
  }

  @Test
  void testGetRecordFieldMetaListForSimpleTypeWithAnnotatedMethod() {
    // using a Set instead of a List because the order in which the methods are loaded by the JVM is inconsistent
    // and I'd rather not enforce and order
    Set<TableFieldSchema> set = newHashSet(schema(ClassAPrime.class));
    assertEquals(set, newHashSet(
            new TableFieldSchema().setName("foo").setType("STRING"),
            new TableFieldSchema().setName("myInt").setType("INTEGER").setMode(MODE_REQUIRED)));
  }

  @Test
  void testGetRecordFieldMetaListForNestedType() {
    List<TableFieldSchema> list = schema(ClassB.class);
    assertEquals(list, newArrayList(
            new TableFieldSchema().setName("aList").setType("RECORD").setMode(MODE_REPEATED).setFields(
                    newArrayList(
                            new TableFieldSchema().setName("foo").setType("STRING"),
                            new TableFieldSchema().setName("myInt").setType("INTEGER").setMode(MODE_REQUIRED))),
            new TableFieldSchema().setName("yep").setType("BOOLEAN"),
            new TableFieldSchema().setName("dollars").setType("FLOAT"),
            new TableFieldSchema().setName("map").setType("STRING"),
            new TableFieldSchema().setName("order").setType("STRING"),
            new TableFieldSchema().setName("myDate").setType("DATE")
            ));
  }

  @Test
  void testGetMetaForRepeatedSimpleType() {
    List<TableFieldSchema> schema = schema(ClassWithSimpleRepeatedType.class);
    TableFieldSchema tfsString = schema.stream().filter(s -> s.getName().equals("theStrings")).findFirst().orElseThrow(RuntimeException::new);
    TableFieldSchema tfsInteger = schema.stream().filter(s -> s.getName().equals("theIntegers")).findFirst().orElseThrow(RuntimeException::new);
    assertAll(
        () -> assertEquals(MODE_REPEATED, tfsString.getMode()),
        () -> assertEquals("theStrings", tfsString.getName()),
        () -> assertEquals(FieldType.STRING.getTypeName(), tfsString.getType()),
        () -> assertNull(tfsString.getFields()),
        () -> assertEquals(MODE_REPEATED, tfsInteger.getMode()),
        () -> assertEquals("theIntegers", tfsInteger.getName()),
        () -> assertEquals(FieldType.INTEGER.getTypeName(), tfsInteger.getType()),
        () -> assertNull(tfsInteger.getFields())
    );
  }

  @Test
  @DisplayName("Test convert simple repeated type")
  void testConvertedRepeated() {
    List<TableFieldSchema> schema = schema(ClassWithSimpleRepeatedConvertedType.class);
    TableFieldSchema element = Iterables.getOnlyElement(schema);
    assertAll(
        () -> assertEquals( MODE_REPEATED, element.getMode()),
        () -> assertEquals( "stringList", element.getName()),
        () -> assertEquals( FieldType.INTEGER.getTypeName(), element.getType()),
        () -> assertNull(element.getFields())
    );
  }

  @Test
  @DisplayName("convertToJson() flag on records should produce string output")
  void testConvertRecordFieldToJson() {
    List<TableFieldSchema> schema = schema(ClassConvertingRecordToJson.class);
    TableFieldSchema element = Iterables.getOnlyElement(schema);
    assertAll(
        () -> assertNull(element.getMode()),
        () -> assertEquals("members", element.getName()),
        () -> assertEquals(FieldType.STRING.getTypeName(), element.getType()),
        () -> assertNull(element.getFields())
    );
  }

  @Test
  @DisplayName("Tests throws exception if nested object record has no declared columns")
  void testThrowsExceptionIfRecordHasNoFields() {
    assertThrows(IllegalStateException.class,
                 () -> BigQueryTable.getSchemaForClass(ClassWithInvalidNestedRecord.class));
  }

  private List<TableFieldSchema> schema(Class x) {
    return getRecordFieldMetaListForType(x).stream()
                                           .map(RecordFieldMeta::asTableFieldSchema)
                                           .collect(Collectors.toList());
  }

  private static class ClassWithInvalidNestedRecord {
    @BigQueryColumn
    private ClassWithNoAnnotations field;
  }

  private static class ClassWithNoAnnotations {
    public String value;
  }
}
