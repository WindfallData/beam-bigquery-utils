# BigQuery Utilities for Apache Beam

A small library of utilities for making it simpler to read from, write to, and generally interact with 
BigQuery within your Apache Beam pipeline.

Requirements:
* Java 1.8+
* Apache Beam 2.x

## Importing to Your Project

Releases are published to [Maven Central](https://search.maven.org/search?q=a:beam-bigquery-utils).  

Maven projects would simply add a dependency like so:

```
<dependency>
  <groupId>com.windfalldata</groupId>
  <artifactId>beam-bigquery-utils</artifactId>
  <version>${version}</version>
</dependency>
``` 
 
## Transforming `TableRow`s

If you commonly read from BigQuery as an input source, then you have likely faced the process of transforming
`PCollection`s of `TableRow`s into a data object that is a little more user friendly.  Behind the scenes,
`TableRow` objects are really just JSON.  We use that fact to utilize the robust Jackson JSON parsing framework to
handle the transformations for us.

An example: 
```
Pipeline p = Pipeline.create(options);

String bqQuery = ...;
PCollection<TableRow> tableRows = p.apply(BigQueryIO.read().fromQuery(bqQuery)); 
PCollection<MyObject> mappedObjects = tableRows.apply(new MapTableRow<MyObject>(MyObject.class) {});
```

In the example above, we are using the `MapTableRow` transform to convert the `PCollection<TableRow>` into a typed
PCollection of our own object type, `MyObject`.

The `MapTableRow` transform is already configured to handle mappings to the newer Java8 types including `LocalDate`,
`LocalDateTime`, `Optional`s, and more.

_Warning_: Make sure your Coder library supports the types you are serializing into.  For example, the default
`AvroCoder` does not handle `LocalDate`s. 

## Writing to BigQuery

Writing to BigQuery, and particularly maintaining the schema definitions, can be a challenge in rapidly evolving 
code bases or that have complicated table row structures. We made a transformation that will automatically create
the BigQuery `TableSchema` for you based off of the Class type of the `PCollection`.  

An example:
```
Pipeline p = Pipeline.create(options);
...

PCollection<MyObject> collection = p.apply(...);

// Writes the PCollection to the table provided in the options.
collection.apply(new BigQueryIOWriterTransform("gcp-project-id",
                                               "target_dataset",
                                               "target_table",
                                               MyObject.class));
```

In the above example, we are using the `BigQueryIOWriterTransform` to write to the BigQuery table 
`target_dataset.target_table` without defining the table schema or mapping to `TableRow` objects.

The `BigQueryIOWriterTransform` uses a mechanism to infer the table schema of your source object by
looking for fields or methods annotated with the `@BigQueryColumn` annotation.   

### Annotating your Classes

The `@BigQueryColumn` annotation is required in order to use the `BigQueryIOWriterTransform`. It provides
the mechanism by which the transform is able to infer the table schema and also perform the mappings to
table row objects.

Features of the annotation: 
* Can be applied to either fields or methods.
  - the field/method does not need to be visible. 
* It works on all java types, including typed collections.
  - Collections will created as nested repeated columns.
* Column descriptions can be added to the table schema
* Required columns can be declared
* Conversions of values from String types to Doubles, Longs, or Booleans
* Serialize to json within a column

#### Examples

A simple class:
```
Java Class:
public class MyClass {
  @BigQueryColumn(required=true) private String value;
  @BigQueryColumn private long aNumber;
  @BigQueryColumn private boolean aBoolean;
}

Schema:
{
  "fields":[
    { "name":"value", "type": "STRING", "REQUIRED":true }, 
    { "name":"aNumber", "type": "INTEGER" }, 
    { "name":"aBoolean", "type": "BOOLEAN" }, 
  ]
}
```

Methods or Getters:
```
Java Class:
public class MyClass {
  private String value;
  
  @BigQueryColumn
  public String getValue() { return value; }

  @BigQueryColumn
  public long aNumber() { return 42; } 
}

Schema:
{ 
  "fields": [
    { "name":"value", "type": "STRING" }, 
    { "name":"aNumber", "type": "INTEGER" } 
  ]
}
```

Documenting Columns:
```
Java Class:
public class MyClass {
  @BigQueryColumn(name="str_value", description="String value column") 
  private String value;
}

Schema:
{
  "fields": [
    { "name":"str_value", "type": "STRING", "description":"String value column" }
  ]
}
```

Simple Collections:
```
Java Class:
public class MyClass {
  
  @BigQueryColumn(name="my_str_list", isSimpleCollection = true)
  private List<String> list;
  
  @BigQueryColumn(name="my_int_list", isSimpleCollection = true)
  private List<Integer> list2;
}

Schema:
{
  "fields": [
    { "name":"my_str_list", "type":"STRING", mode:"REPEATED" }, 
    { "name":"my_int_list", "type":"INTEGER", mode:"REPEATED" } 
  ]
}
```

Nested Objects and Collections:
```
Java Class:
public class MyClass {
  
  @BigQueryColumn
  private MyOtherClass other;
  
  @BigQueryColumn
  private List<MyOtherClass> others;
  
  public static class MyOtherClass {
    @BigQueryColumn
    private String value;
  }
}

Schema:
{
  "fields": [
    { 
      "name":"other", 
      "type":"RECORD", 
      "fields":[
        { "name":"value", "type":"STRING" }
      ]
    },
    { 
      "name":"others",
      "type":"RECORD",
      "fields":[
        { "name":"value", "type":"STRING" }
      ],
      "mode":"REPEATED"
    }
  ]
}
```

Converting Columns to Other Types
```
Java Class:
public class MyClass {
  @BigQueryColumn(convertTo=Long.class)
  private String myNumbers;
  
  @BigQueryColumn(convertToJson=true)
  private List<Integer> jsonNumbers;
  
  @BigQueryColumn(convertToJson=true)
  private List<SomeOtherClass> jsonRecordList;
}

Schema:
{
  "fields": [
    { "name":"myNumbers", "type":"INTEGER" },
    { "name":"jsonNumbers", "type":"STRING" },
    { "name":"jsonRecordList", "type":"STRING" }
  ]
}
```
