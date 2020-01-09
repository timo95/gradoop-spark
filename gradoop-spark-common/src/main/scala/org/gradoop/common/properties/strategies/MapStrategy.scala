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
 *//*
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
package org.gradoop.common.properties.strategies

import java.io.{ByteArrayOutputStream, DataInputStream, DataOutputStream, IOException}

import org.gradoop.common.properties.{PropertyValue, Type}

import scala.collection.mutable

/**
 * Strategy class for handling {@code PropertyValue} operations with a value of the type {@code Map}.
 */
class MapStrategy extends AbstractVariableSizedPropertyValueStrategy[Map[PropertyValue, PropertyValue]] {
  @throws[IOException]
  override def read(inputStream: DataInputStream, typeByte: Byte): Map[PropertyValue, PropertyValue] = {
    val rawBytes = readBytes(inputStream, typeByte)
    val internalInputStream = createInputStream(rawBytes)
    createMap(internalInputStream)
  }

  override def compare(value: Map[PropertyValue, PropertyValue], other: Any): Int = {
    throw new UnsupportedOperationException("Method compare() is not supported for Map.")
  }

  override def is(value: Any): Boolean = {
    value.isInstanceOf[Map[_, _]] && value.asInstanceOf[Map[_, _]]
      .forall(e => e._1.isInstanceOf[PropertyValue] && e._2.isInstanceOf[PropertyValue])
  }

  override def getType: Type = Type.MAP

  override def getExactType(bytes: Array[Byte]): Type = {
    val value = get(bytes)
    if(value.isEmpty) Type.TYPED_MAP(Type.NULL, Type.NULL)
    else Type.TYPED_MAP(value.head._1.getExactType, value.head._2.getExactType)
  }

  @throws[IOException]
  override def get(bytes: Array[Byte]): Map[PropertyValue, PropertyValue] = {
    val inputStream = createInputStream(bytes)
    try {
      if (inputStream.skipBytes(PropertyValue.OFFSET) != PropertyValue.OFFSET) {
        throw new IOException("Malformed entry in PropertyValue Map.")
      }
      createMap(inputStream)
    } catch {
      case e: IOException => throw new IOException("Error while processing DataInputStream.", e)
    }
  }

  @throws[IOException]
  override def getBytes(value: Map[PropertyValue, PropertyValue]): Array[Byte] = {
    val size = value.keySet.map(_.value.length).sum + value.values.map(_.value.length).sum + PropertyValue.OFFSET

    try {
      val byteStream = new ByteArrayOutputStream(size)
      val outputStream = new DataOutputStream(byteStream)
      try {
        outputStream.write(getType.byte)
        for (entry <- value) {
          entry._1.write(outputStream)
          entry._2.write(outputStream)
        }
        byteStream.toByteArray
      } catch {
        case e: IOException =>
          throw new IOException("Error writing PropertyValue with MapStrategy.", e)
      } finally {
        if (byteStream != null) byteStream.close()
        if (outputStream != null) outputStream.close()
      }
    } catch {
      case e: IOException => throw new IOException("Error writing PropertyValue with MapStrategy.", e)
    }
  }

  /**
   * Creates a map with data read from an {@link DataInputStream}.
   *
   * @param inputStream { @link DataInputStream} containing data
   * @return a map containing the deserialized data
   * @throws IOException on failure to read input view
   */
  @throws[IOException]
  private def createMap(inputStream: DataInputStream): Map[PropertyValue, PropertyValue] = {
    val map = new mutable.HashMap[PropertyValue, PropertyValue]
    try while (inputStream.available > 0) {
      map.put(PropertyValue.read(inputStream), PropertyValue.read(inputStream))
    }
    catch {
      case e: IOException => throw new IOException("Error reading PropertyValue with MapStrategy.", e)
    }
    map.toMap
  }
}
