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

import org.gradoop.common.properties.PropertyValue;
import org.gradoop.common.properties.Type;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Interface defining the methods necessary to handle the (de-) serialization of a
 * {@link PropertyValue}'s value.
 *
 * @param <T> The property value type.
 */
public interface PropertyValueStrategy<T> {

  /**
   * Writes the given value to the provided {@link DataOutputStream}.
   * The argument {@code value} can not be {@code null}.
   *
   * @param value      to be written to the {@link DataOutputStream}.
   * @param outputStream that the value is written to.
   * @throws IOException if write process fails.
   */
  void write(T value, DataOutputStream outputStream) throws IOException;

  /**
   * Reads raw bytes from the given {@link DataInputStream} and deserializes the contained object.
   *
   * @param inputStream containing serialized object.
   * @param typeByte  byte needed to indicate whether serialized object has a variable length.
   * @return deserialized object.
   * @throws IOException when reading or deserialization of the object fails.
   */
  T read(DataInputStream inputStream, byte typeByte) throws IOException;

  byte[] readBytes(DataInputStream inputStream, byte typeByte) throws IOException;

  /**
   * Compares two objects.
   *
   * @param value first object.
   * @param other second object.
   * @return a negative integer, zero, or a positive integer as first object is less than, equal to,
   * or greater than the second object.
   * @throws IllegalArgumentException when {@code other} is not comparable to {@code value}.
   */
  int compare(T value, Object other);

  /**
   * Checks if given object is an instance of the data type the specific strategy handles.
   *
   * @param value to be checked.
   * @return true if {@code value} is an instance of the data type this strategy handles.
   * False otherwise.
   */
  boolean is(Object value);

  /**
   * Returns the type handled by this strategy. For compound types only the primitive is returned.
   *
   * @return type class.
   */
  Type getType();

  /**
   * Returns the type of the given object. Compound types include their subtypes.
   *
   * @param bytes serialized object.
   * @return type class.
   */
  default Type getExactType(byte[] bytes) {
    return getType();
  }

  /**
   * Deserializes an object from the provided byte array.
   *
   * @param bytes representing a serialized object.
   * @return an instance of the type handled by this strategy.
   * @throws IOException on failure
   */
  T get(byte[] bytes) throws IOException;

  /**
   * Serializes the given object.
   * The argument {@code value} can not be {@code null}.
   *
   * @param value the object to be serialized.
   * @return byte array representation of the provided object.
   * @throws IOException on failure
   */
  byte[] getBytes(T value) throws IOException;
}
