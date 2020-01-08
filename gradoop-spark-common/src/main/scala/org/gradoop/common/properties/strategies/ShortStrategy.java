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
 * {@code Short}.
 */
public class ShortStrategy extends AbstractFixSizedPropertyValueStrategy<Short> {

  @Override
  public Short read(DataInputStream inputStream, byte typeByte) throws IOException {
    return Bytes.toShort(readBytes(inputStream, typeByte));
  }

  @Override
  public int compare(Short value, Object other) {
    return PropertyValueStrategyUtils.compareNumerical(value, other);
  }

  @Override
  public boolean is(Object value) {
    return value instanceof Short;
  }

  @Override
  int getSize() {
    return Bytes.SIZEOF_SHORT;
  }

  @Override
  public Type getType() {
    return Type.SHORT$.MODULE$;
  }

  @Override
  public Short get(byte[] bytes) {
    return Bytes.toShort(bytes, PropertyValue.OFFSET());
  }

  @Override
  public byte[] getBytes(Short value) {
    byte[] rawBytes = new byte[PropertyValue.OFFSET() + Bytes.SIZEOF_SHORT];
    rawBytes[0] = getType().getTypeByte();
    Bytes.putShort(rawBytes, PropertyValue.OFFSET(), value);
    return rawBytes;
  }
}
