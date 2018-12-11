package com.windfalldata.beam.bigquery;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.fasterxml.jackson.databind.MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES;

public class MapTableRow<T> extends PTransform<PCollection<TableRow>, PCollection<T>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(MapTableRow.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().registerModule(new ParameterNamesModule())
                                                                      .registerModule(new Jdk8Module())
                                                                      .registerModule(new JavaTimeModule())
                                                                      .configure(ACCEPT_CASE_INSENSITIVE_PROPERTIES,
                                                                                 true);

  private final Class<T> type;

  public MapTableRow(Class<T> type) {
    this.type = type;
  }

  @Override
  public PCollection<T> expand(PCollection<TableRow> input) {

    return input.apply(ParDo.of(new DoFn<TableRow, T>() {

      // https://stackoverflow.com/questions/32591914/making-transformations-in-dataflow-generic
      @Override
      public TypeDescriptor<T> getOutputTypeDescriptor() {
        return new TypeDescriptor<T>(MapTableRow.this.getClass()) {
        };
      }

      @ProcessElement
      public void processElement(ProcessContext c) {
        c.output(map(c.element()));
      }
    }));
  }

  public T map(TableRow row) {
    try {
      return OBJECT_MAPPER.readValue(OBJECT_MAPPER.writeValueAsString(row), type);
    } catch (IOException e) {
      LOGGER.error("Invalid json: {}", row, e);
      throw new RuntimeException(e);
    }
  }

}
