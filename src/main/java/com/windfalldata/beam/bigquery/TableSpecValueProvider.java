package com.windfalldata.beam.bigquery;

import org.apache.beam.sdk.options.ValueProvider;

import static java.lang.String.format;

public class TableSpecValueProvider implements ValueProvider<String> {

  private final ValueProvider<String> project;
  private final ValueProvider<String> dataset;
  private final ValueProvider<String> table;

  public TableSpecValueProvider(ValueProvider<String> project, ValueProvider<String> dataset, ValueProvider<String> table) {
    this.project = project;
    this.dataset = dataset;
    this.table = table;
  }

  public TableSpecValueProvider(ValueProvider<String> project, ValueProvider<String> dataset, String table) {
    this(project, dataset, ValueProvider.StaticValueProvider.of(table));
  }

  public TableSpecValueProvider(ValueProvider<String> project, String dataset, String table) {
    this(project, ValueProvider.StaticValueProvider.of(dataset), ValueProvider.StaticValueProvider.of(table));
  }

  @Override
  public String get() {
    return format("%s:%s.%s", project.get(), dataset.get(), table.get());
  }

  @Override
  public boolean isAccessible() {
    return project.isAccessible() && dataset.isAccessible() && table.isAccessible();
  }
}
