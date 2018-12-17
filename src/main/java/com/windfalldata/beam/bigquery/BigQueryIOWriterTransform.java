package com.windfalldata.beam.bigquery;


import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

import javax.annotation.Nonnull;

/**
 * Transformation that takes an object having fields or methods annotated with one or
 * more {@link BigQueryColumn} annotations and writes those objects as rows in a
 * BigQuery table.
 * <p>
 * By default, the target BigQuery table will be replaced if it exists. This can be changed
 * by calling the {@link #withWriteDisposition(BigQueryIO.Write.WriteDisposition)} method.
 * </p>
 * <p>
 * Example usage:
 * </p>
 * <pre>
 * Pipeline p = Pipeline.create(options);
 * ...
 *
 * PCollection&lt;MyObject&gt; collection = p.apply(...);
 * collection.apply(new BigQueryIOWriterTransform(options.getProject(),
 *                                                options.getDataset(),
 *                                                options.getTableName(),
 *                                                MyObject.class));
 * </pre>
 *
 * @param <T> the object type to serialize and write to BigQuery
 * @see BigQueryColumn
 * @see BigQueryTable#getSchemaForClass(Class)
 */
@SuppressWarnings("unused")
public class BigQueryIOWriterTransform<T> extends PTransform<PCollection<T>, WriteResult> {

  private final ValueProvider<String> project;
  private final ValueProvider<String> dataset;
  private final ValueProvider<String> table;
  private final Class<T> type;

  private BigQueryIO.Write.WriteDisposition writeDisposition = BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE;
  private BigQueryIO.Write.CreateDisposition createDisposition = BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;

  public BigQueryIOWriterTransform(ValueProvider<String> project,
                                   ValueProvider<String> dataset,
                                   ValueProvider<String> table,
                                   Class<T> type) {
    super();
    this.project = project;
    this.dataset = dataset;
    this.table = table;
    this.type = type;
  }

  public BigQueryIOWriterTransform(ValueProvider<String> project,
                                   ValueProvider<String> dataset,
                                   String table,
                                   Class<T> type) {
    this(project, dataset, StaticValueProvider.of(table), type);
  }

  public BigQueryIOWriterTransform(@Nonnull String project,
                                   @Nonnull String dataset,
                                   @Nonnull String table,
                                   @Nonnull Class<T> type) {
    this(StaticValueProvider.of(project), StaticValueProvider.of(dataset), table, type);
  }

  public BigQueryIO.Write.CreateDisposition getCreateDisposition() {
    return createDisposition;
  }

  public BigQueryIOWriterTransform<T> withCreateDisposition(BigQueryIO.Write.CreateDisposition createDisposition) {
    this.createDisposition = createDisposition;
    return this;
  }

  public BigQueryIO.Write.WriteDisposition getWriteDisposition() {
    return writeDisposition;
  }

  public BigQueryIOWriterTransform<T> withWriteDisposition(BigQueryIO.Write.WriteDisposition writeDisposition) {
    this.writeDisposition = writeDisposition;
    return this;
  }


  @Override
  public WriteResult expand(PCollection<T> input) {
    BigQueryTableObjectConverter<T> converter = new BigQueryTableObjectConverter<>(type);

    return input.apply(MapElements.into(TypeDescriptor.of(TableRow.class))
                                  .via(converter::apply))
                .apply(BigQueryIO.writeTableRows()
                                 .to(new TableSpecValueProvider(project, dataset, table))
                                 .withSchema(BigQueryTable.getSchemaForClass(type))
                                 .withCreateDisposition(createDisposition)
                                 .withWriteDisposition(writeDisposition));
  }

}
