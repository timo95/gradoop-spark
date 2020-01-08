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
import org.gradoop.common.properties.bytes.Bytes;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * Strategy class for handling {@code PropertyValue} operations with a value of the type
 * {@code String}.
 */
public class StringStrategy extends AbstractVariableSizedPropertyValueStrategy<String> {

  @Override
  public String read(DataInputStream inputStream, byte typeByte) throws IOException {
    return Bytes.toString(readBytes(inputStream, typeByte));
  }

  @Override
  public int compare(String value, Object other) {
    if (other instanceof String) {
      return value.compareTo((String) other);
    }
    throw new IllegalArgumentException(String.format(
      "Incompatible types: %s, %s", value.getClass(), other.getClass()));
  }

  @Override
  public boolean is(Object value) {
    return value instanceof String;
  }

  @Override
  public Type getType() {
    return Type.STRING$.MODULE$;
  }

  @Override
  public String get(byte[] bytes) {
    return Bytes.toString(bytes, PropertyValue.OFFSET(), bytes.length - PropertyValue.OFFSET());
  }

  @Override
  public byte[] getBytes(String value) {
    byte[] valueBytes = Bytes.toBytes(value);
    byte[] rawBytes = new byte[PropertyValue.OFFSET() + valueBytes.length];
    rawBytes[0] = getType().getTypeByte();
    Bytes.putBytes(rawBytes, PropertyValue.OFFSET(), valueBytes, 0, valueBytes.length);
    return rawBytes;
  }
}
