package com.windfalldata.beam.bigquery;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Specifies that fields or method with this annotation should be added to the BigQuery schema for the object
 * in which they are contained.
 *
 * @see BigQueryIOWriterTransform
 */
@Target({ElementType.FIELD, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface BigQueryColumn {

  /**
   * The name of the column.
   * If not set will use the value of the field or the method.
   */
  String name() default "";

  /**
   * Whether the BigQuery column should be marked as required
   */
  boolean required() default false;

  /**
   * The description of the column.
   */
  String description() default "";

  /**
   * String types only!
   * <p>
   * If true, will strip the String value to null. Mutually exclusive of {@code required}.
   *
   * @see org.apache.commons.lang3.StringUtils#stripToNull(String)
   */
  boolean stripToNull() default false;

  /**
   * Converts the object to its JSON string format.
   */
  boolean convertToJson() default false;

  /**
   * String types only!
   * <p>
   * Will attempt to convert the value to the specified type.
   * Currently supported values are: Double, Long, and Boolean.
   */
  Class convertTo() default Void.class;

  /**
   * Annotation to use for repeated columns with simple types (repeated STRING fields for example)
   */
  boolean isSimpleCollection() default false;
}
