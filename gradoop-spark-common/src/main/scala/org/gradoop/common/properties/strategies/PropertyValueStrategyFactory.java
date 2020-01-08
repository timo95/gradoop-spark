/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.common.properties.strategies;

import org.gradoop.common.properties.PrimitiveType;
import org.gradoop.common.properties.PropertyValue;
import org.gradoop.common.properties.Type;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Factory class responsible for instantiating strategy classes that manage every kind of access to
 * the current {@link PropertyValue} value.
 */
public class PropertyValueStrategyFactory {

  /**
   * {@link PropertyValueStrategyFactory} instance
   */
  private static PropertyValueStrategyFactory INSTANCE = new PropertyValueStrategyFactory();
  /**
   * Map which links a data type to a strategy class
   */
  private final Map<Byte, PropertyValueStrategy> strategyMap;
  /**
   * Strategy for {@code null}-value properties
   */
  private final NullStrategy nullStrategy;

  /**
   * Constructs an {@link PropertyValueStrategyFactory} with type - strategy mappings as defined in
   * {@code initClassStrategyMap}.
   * Only one instance of this class is needed.
   */
  private PropertyValueStrategyFactory() {
    nullStrategy = new NullStrategy();
    strategyMap = initStrategyMap();
  }

  /**
   * Returns value which is represented by the the provided byte array. Assumes that the value is
   * serialized according to the {@link PropertyValue} standard.
   *
   * @param bytes byte array of raw bytes.
   * @return Object which is the result of the deserialization.
   */
  public static Object fromBytes(byte[] bytes) {
    PropertyValueStrategy strategy = INSTANCE.strategyMap.get(bytes[0]);
    try {
      return strategy == null ? null : strategy.get(bytes);
    } catch (IOException e) {
      throw new RuntimeException("Error while deserializing object.", e);
    }
  }

  /**
   * Compares two values.<br>
   * {@link PropertyValue#NULL_VALUE} is considered to be less than all other properties.
   * <p>
   * If {@code other} is not comparable to {@code value}, the used {@link PropertyValueStrategy} will throw an
   * {@code IllegalArgumentException}. This behavior violates the requirements of
   * {@link Comparable#compareTo}.
   *
   * @param value first value.
   * @param other second value.
   * @return a negative integer, zero, or a positive integer as {@code value} is less than, equal
   * to, or greater than {@code other}.
   */
  public static int compare(Object value, Object other) {
    if (value == null || other == null) {
      return INSTANCE.nullStrategy.compare(value, other);
    } else {
      PropertyValueStrategy strategy = get(PrimitiveType.of(value).getTypeByte());
      return strategy.compare(value, other);
    }
  }

  /**
   * Get byte array representation of the provided object. The object is serialized according to the
   * {@link PropertyValue} standard.
   * If the given type is not supported, an {@code UnsupportedTypeException} will be thrown.
   *
   * @param value to be serialized.
   * @return byte array representation of the provided object.
   */
  @SuppressWarnings("unchecked")
  public static byte[] getBytes(Object value) {
    if (value != null) {
      try {
        PropertyValueStrategy strategy = get(PrimitiveType.of(value).getTypeByte());
        if (!strategy.is(value)) {
          throw new IllegalArgumentException(String.format("Type %s is not supported by %s",
            value.getClass().getSimpleName(), strategy.getClass().getSimpleName()));
        }
        return strategy.getBytes(value);
      } catch (IOException e) {
        throw new RuntimeException("Error while serializing object.", e);
      }
    }
    return new byte[] {Type.NULL$.MODULE$.getTypeByte()};
  }

  /**
   * Returns strategy mapping to the provided byte value.
   *
   * @param value representing a data type.
   * @return strategy class.
   * @throws IllegalArgumentException when there is no matching strategy for a given type byte.
   */
  public static PropertyValueStrategy get(byte value) throws IllegalArgumentException {
    PropertyValueStrategy strategy = INSTANCE.strategyMap.get(value);
    if (strategy == null) {
      throw new IllegalArgumentException("No strategy for type byte " + value);
    }

    return strategy;
  }

  /**
   * Initializes class-strategy mapping.
   *
   * @return Map of supported class-strategy associations.
   */
  private Map<Byte, PropertyValueStrategy> initStrategyMap() {
    Map<Byte, PropertyValueStrategy> classMapping = new HashMap<>();
    classMapping.put(Type.NULL$.MODULE$.getTypeByte(), new NullStrategy());
    classMapping.put(Type.BOOLEAN$.MODULE$.getTypeByte(), new BooleanStrategy());
    classMapping.put(Type.SET$.MODULE$.getTypeByte(), new SetStrategy());
    classMapping.put(Type.INTEGER$.MODULE$.getTypeByte(), new IntegerStrategy());
    classMapping.put(Type.LONG$.MODULE$.getTypeByte(), new LongStrategy());
    classMapping.put(Type.FLOAT$.MODULE$.getTypeByte(), new FloatStrategy());
    classMapping.put(Type.DOUBLE$.MODULE$.getTypeByte(), new DoubleStrategy());
    classMapping.put(Type.SHORT$.MODULE$.getTypeByte(), new ShortStrategy());
    classMapping.put(Type.BIG_DECIMAL$.MODULE$.getTypeByte(), new BigDecimalStrategy());
    classMapping.put(Type.DATE$.MODULE$.getTypeByte(), new DateStrategy());
    classMapping.put(Type.TIME$.MODULE$.getTypeByte(), new TimeStrategy());
    classMapping.put(Type.DATE_TIME$.MODULE$.getTypeByte(), new DateTimeStrategy());
    classMapping.put(Type.GRADOOP_ID$.MODULE$.getTypeByte(), new GradoopIdStrategy());
    classMapping.put(Type.STRING$.MODULE$.getTypeByte(), new StringStrategy());
    classMapping.put(Type.LIST$.MODULE$.getTypeByte(), new ListStrategy());
    classMapping.put(Type.MAP$.MODULE$.getTypeByte(), new MapStrategy());

    return Collections.unmodifiableMap(classMapping);
  }
}
