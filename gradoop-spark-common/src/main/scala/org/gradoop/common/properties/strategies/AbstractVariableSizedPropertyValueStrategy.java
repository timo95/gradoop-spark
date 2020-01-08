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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Abstract class that provides generic methods for {@code PropertyValueStrategy} classes that
 * handle data types with a variable size.
 *
 * @param <T> Type with a variable length.
 */
public abstract class AbstractVariableSizedPropertyValueStrategy<T> implements PropertyValueStrategy<T> {

  @Override
  public void write(T value, DataOutputStream outputStream) throws IOException {
    byte[] rawBytes = getBytes(value);
    byte type = rawBytes[0];

    if (rawBytes.length > PropertyValue.LARGE_PROPERTY_THRESHOLD()) {
      type |= PropertyValue.FLAG_LARGE();
    }
    outputStream.writeByte(type);

    // Write length as an int if the "large" flag is set.
    if ((type & PropertyValue.FLAG_LARGE()) == PropertyValue.FLAG_LARGE()) {
      outputStream.writeInt(rawBytes.length - PropertyValue.OFFSET());
    } else {
      outputStream.writeShort(rawBytes.length - PropertyValue.OFFSET());
    }

    outputStream.write(rawBytes, PropertyValue.OFFSET(), rawBytes.length - PropertyValue.OFFSET());
  }

  /**
   * Reads data of variable size from a data input view. The size of the data is determined by the
   * type byte and {@see org.gradoop.common.model.impl.properties.PropertyValue#FLAG_LARGE}.
   *
   * @param inputStream Data input view to read from
   * @param typeByte Byte indicating the type of the serialized data
   * @return The serialized data in the data input view
   * @throws IOException when reading a byte goes wrong
   */
  @Override
  public byte[] readBytes(DataInputStream inputStream, byte typeByte) throws IOException {
    int length;
    // read length
    if ((typeByte & PropertyValue.FLAG_LARGE()) == PropertyValue.FLAG_LARGE()) {
      length = inputStream.readInt();
    } else {
      length = inputStream.readShort();
    }
    // init new array
    byte[] rawBytes = new byte[length];

    for (int i = 0; i < rawBytes.length; i++) {
      rawBytes[i] = inputStream.readByte();
    }

    return rawBytes;
  }

  /**
   * Creates an instance of {@link DataInputStream} from a byte array.
   *
   * @param bytes input byte array
   * @return A DataInputStreamStreamWrapper
   */
  DataInputStream createInputStream(byte[] bytes) {
    ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
    DataInputStream inputStream = new DataInputStream(byteStream);
    return new DataInputStream(inputStream);
  }
}
