# BigQuery Utilities for Apache Beam

A small library of utilities for making it simpler to read from, write to, and generally interact with 
BigQuery within your Apache Beam pipeline.

Requirements:
* Java 1.8+
* Apache Beam 2.x
 
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


