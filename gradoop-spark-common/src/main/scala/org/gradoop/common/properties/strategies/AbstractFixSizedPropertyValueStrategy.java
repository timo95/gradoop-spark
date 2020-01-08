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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Abstract class that provides generic methods for {@code PropertyValueStrategy} classes that
 * handle data types with a fixed size.
 *
 * @param <T> Type with a fixed length.
 */
public abstract class AbstractFixSizedPropertyValueStrategy<T> implements PropertyValueStrategy<T> {

  abstract int getSize();

  @Override
  public byte[] readBytes(DataInputStream inputStream, byte typeByte) throws IOException {
    byte[] rawBytes = new byte[getSize()];

    for (int i = 0; i < rawBytes.length; i++) {
      rawBytes[i] = inputStream.readByte();
    }

    return rawBytes;
  }

  @Override
  public void write(T value, DataOutputStream outputStream) throws IOException {
    outputStream.write(getBytes(value));
  }
}
