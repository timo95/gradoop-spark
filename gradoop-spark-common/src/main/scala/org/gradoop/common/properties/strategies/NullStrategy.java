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

import org.gradoop.common.properties.Type;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Strategy class for handling {@code PropertyValue} operations when the value is {@code null}.
 */
public class NullStrategy implements PropertyValueStrategy {

  @Override
  public void write(Object value, DataOutputStream outputStream) throws IOException {
    outputStream.write(new byte[]{getType().getTypeByte()});
  }

  @Override
  public Object read(DataInputStream inputStream, byte typeByte) {
    return null;
  }

  @Override
  public byte[] readBytes(DataInputStream inputStream, byte typeByte) {
    return new byte[0];
  }

  @Override
  public int compare(Object value, Object other) {
    if (value == null && other == null) {
      return 0;
    } else if (value == null) {
      return -1;
    }
    return 1;
  }

  @Override
  public boolean is(Object value) {
    return false;
  }

  @Override
  public Type getType() {
    return Type.NULL$.MODULE$;
  }

  @Override
  public Object get(byte[] bytes) {
    return null;
  }

  @Override
  public byte[] getBytes(Object value) {
    return new byte[] {getType().getTypeByte()};
  }
}
