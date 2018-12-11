package com.windfalldata.beam.bigquery;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.time.LocalDate;
import java.util.Collection;

import static java.util.stream.Collectors.toList;

class BigQueryColumnValueExtractor {

  private static final Logger LOGGER = LoggerFactory.getLogger(BigQueryColumnValueExtractor.class);

  @FunctionalInterface
  interface CollectionValuesMapper {

    /**
     * Function to map from one instance type to another.
     *
     * @param collectionType The parameterized type of the collection
     * @param instance       An instance value of the collection
     * @return the mapped value
     */
    Object mapValue(Class collectionType, Object instance);
  }

  @FunctionalInterface
  interface ExtractionFunction {

    /**
     * Function to bind the parameterized type of a collection to the generated ValueFunction
     *
     * @param type the parameterized type of the collection
     * @return A ValueFunction bound to this type
     */
    ValueFunction bindType(Class type);
  }

  @FunctionalInterface
  interface ValueFunction {

    /**
     * Function to extract the value from the provided instance Object.
     *
     * @param fromInstance the instance
     * @return the extracted value
     * @see #extractValue(Class, Object)
     */
    Object extractValue(Object fromInstance);
  }

  private final Member member;
  private final BigQueryColumn columnDefinition;
  private final CollectionValuesMapper fn;

  BigQueryColumnValueExtractor(@Nonnull Member member,
                               @Nonnull BigQueryColumn columnDefinition,
                               @Nonnull CollectionValuesMapper recursiveCollectionFn) {
    this.member = member;
    this.columnDefinition = columnDefinition;
    this.fn = recursiveCollectionFn;
  }

  /**
   * Extracts the value from the provided instance and returns the value.
   *
   * @param type     the parameterized type of the instance, in case of a Collection
   * @param instance the instance object
   * @return the value
   */
  @SuppressWarnings("WeakerAccess")
  Object extractValue(Class<?> type, Object instance) {
    Object value = getValue(member, instance);
    if (value != null && columnDefinition.convertToJson()) {
      return new Gson().toJson(value);
    }

    if (columnDefinition.isSimpleCollection()) {
      Preconditions.checkState(value instanceof Collection, "Value %s of type %s is not a simple collection");
      return value;
    }

    if (value instanceof Collection) {
      //noinspection unchecked
      return ((Collection) value).stream().map(x -> fn.mapValue(type, x)).collect(toList());
    }

    if (value instanceof LocalDate) {
      return ((LocalDate) value).toString();
    }

    if (value instanceof String || value instanceof Enum || value instanceof Character) {
      String s = value.toString();
      if (columnDefinition.stripToNull()) {
        s = StringUtils.stripToNull(s);
      }

      if (s != null && Void.class != columnDefinition.convertTo()) {
        if (Long.class == columnDefinition.convertTo()) {
          // use double here in case it is "42.00" and we still want longs
          return Double.valueOf(s).longValue();
        } else if (Double.class == columnDefinition.convertTo()) {
          return Double.valueOf(s);
        } else if (Boolean.class == columnDefinition.convertTo()) {
          return Boolean.valueOf(s);
        } else {
          throw new IllegalStateException("Unexpected convertTo value: " + columnDefinition.convertTo());
        }
      }

      return s;
    }
    return value;
  }

  private Object getValue(Member member, Object instance) {
    if (member instanceof Field) {
      return getValue(((Field) member), instance);
    } else if (member instanceof Method) {
      return getValue(((Method) member), instance);
    }

    throw new IllegalArgumentException("Invalid Member type: " + member);
  }

  private Object getValue(Field field, Object instance) {
    boolean accessible = field.isAccessible();
    if (!accessible) {
      field.setAccessible(true);
    }

    try {
      return field.get(instance);
    } catch (IllegalAccessException e) {
      LOGGER.error("Failed to get field {} value from instance {}", field, instance);
      throw new IllegalStateException(e);
    } finally {
      if (!accessible) {
        field.setAccessible(false);
      }
    }
  }

  private Object getValue(Method method, Object instance) {
    boolean accessible = method.isAccessible();
    if (!accessible) {
      method.setAccessible(true);
    }

    try {
      return method.invoke(instance);
    } catch (IllegalAccessException | InvocationTargetException e) {
      LOGGER.error("Failed to get method {} value from instance {}", method, instance);
      throw new IllegalStateException(e);
    } finally {
      if (!accessible) {
        method.setAccessible(false);
      }
    }
  }

}
