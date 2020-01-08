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

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.properties.PropertyValue;
import org.gradoop.common.properties.Type;
import org.gradoop.common.properties.bytes.Bytes;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;

/**
 * Strategy class for handling {@code PropertyValue} operations with a value of the type
 * {@code GradoopIdStrategy}.
 */
public class GradoopIdStrategy extends AbstractFixSizedPropertyValueStrategy<GradoopId> {

  @Override
  public GradoopId read(DataInputStream inputStream, byte typeByte) throws IOException {
    return GradoopId.apply(readBytes(inputStream, typeByte));
  }

  @Override
  public int compare(GradoopId value, Object other) {
    if (other instanceof GradoopId) {
      return value.compareTo((GradoopId) other);
    }
    throw new IllegalArgumentException(String.format(
      "Incompatible types: %s, %s", value.getClass(), other.getClass()));
  }

  @Override
  public boolean is(Object value) {
    return value instanceof GradoopId;
  }

  @Override
  int getSize() {
    return GradoopId.ID_SIZE();
  }

  @Override
  public Type getType() {
    return Type.GRADOOP_ID$.MODULE$;
  }

  @Override
  public GradoopId get(byte[] bytes) {
    return GradoopId.apply(
      Arrays.copyOfRange(
        bytes, PropertyValue.OFFSET(), GradoopId.ID_SIZE() + PropertyValue.OFFSET()
      ));
  }

  @Override
  public byte[] getBytes(GradoopId value) {
    byte[] valueBytes = value.bytes();
    byte[] rawBytes = new byte[PropertyValue.OFFSET() + GradoopId.ID_SIZE()];
    rawBytes[0] = getType().getTypeByte();
    Bytes.putBytes(rawBytes, PropertyValue.OFFSET(), valueBytes, 0, valueBytes.length);
    return rawBytes;
  }
}
