package com.windfalldata.beam.bigquery;

import org.apache.avro.AvroTypeException;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SchemaAndRecordConverter<T> implements SerializableFunction<SchemaAndRecord, T> {

  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaAndRecordConverter.class);

  private final Class<T> type;

  public SchemaAndRecordConverter(Class<T> type) {
    this.type = type;
  }

  @Override
  public T apply(SchemaAndRecord input) {
    final GenericRecord record = input.getRecord();
    final GenericDatumReader<T> reader = new GenericDatumReader<>(record.getSchema());

    final String data = record.toString();
    try {
      return reader.read(type.newInstance(), DecoderFactory.get().jsonDecoder(record.getSchema(), data));
    } catch (IOException | IllegalAccessException | InstantiationException e) {
      throw new RuntimeException(e);
    } catch (AvroTypeException e) {
      LOGGER.error("Failed to read object: {}", data, e);
      throw new RuntimeException(e);
    }
  }
}
