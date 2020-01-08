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

import org.gradoop.common.properties.DateTimeSerializer;
import org.gradoop.common.properties.PropertyValue;
import org.gradoop.common.properties.Type;
import org.gradoop.common.properties.bytes.Bytes;

import java.io.DataInputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Arrays;

/**
 * Strategy class for handling {@code PropertyValue} operations with a value of the type
 * {@code LocalDateTime}.
 */
public class DateTimeStrategy extends AbstractFixSizedPropertyValueStrategy<LocalDateTime> {

  @Override
  public LocalDateTime read(DataInputStream inputStream, byte typeByte) throws IOException {
    return DateTimeSerializer.deserializeDateTime(readBytes(inputStream, typeByte));
  }

  @Override
  public int compare(LocalDateTime value, Object other) {
    if (other instanceof LocalDateTime) {
      return value.compareTo((LocalDateTime) other);
    }
    throw new IllegalArgumentException(String.format(
      "Incompatible types: %s, %s", value.getClass(), other.getClass()));
  }

  @Override
  public boolean is(Object value) {
    return value instanceof LocalDateTime;
  }

  @Override
  int getSize() {
    return DateTimeSerializer.SIZEOF_DATETIME;
  }

  @Override
  public Type getType() {
    return Type.DATE_TIME$.MODULE$;
  }

  @Override
  public LocalDateTime get(byte[] bytes) {
    return DateTimeSerializer.deserializeDateTime(
      Arrays.copyOfRange(
        bytes, PropertyValue.OFFSET(), DateTimeSerializer.SIZEOF_DATETIME + PropertyValue.OFFSET()
      ));
  }

  @Override
  public byte[] getBytes(LocalDateTime value) {
    byte[] valueBytes = DateTimeSerializer.serializeDateTime(value);
    byte[] rawBytes = new byte[PropertyValue.OFFSET() + DateTimeSerializer.SIZEOF_DATETIME];
    rawBytes[0] = getType().getTypeByte();
    Bytes.putBytes(rawBytes, PropertyValue.OFFSET(), valueBytes, 0, valueBytes.length);
    return rawBytes;
  }
}
