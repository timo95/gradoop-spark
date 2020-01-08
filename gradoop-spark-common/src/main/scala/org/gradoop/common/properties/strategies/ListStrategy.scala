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

import java.io._

import org.gradoop.common.properties.{PropertyValue, Type}

import scala.collection.mutable

/**
 * Strategy class for handling {@code PropertyValue} operations with a value of the type {@code List}.
 */
class ListStrategy extends AbstractVariableSizedPropertyValueStrategy[List[PropertyValue]] {

  @throws[IOException]
  override def read(inputStream: DataInputStream, typeByte: Byte): List[PropertyValue] = {
    val rawBytes = readBytes(inputStream, typeByte)
    val list = mutable.MutableList.empty[PropertyValue]
    val internalInputStream = createInputStream(rawBytes)
    try while (internalInputStream.available > 0) list += PropertyValue.read(internalInputStream)
    catch {
      case e: IOException =>
        throw new IOException("Error reading PropertyValue with ListStrategy.", e)
    }
    list.toList
  }

  override def compare(value: List[PropertyValue], other: Any): Int = {
    throw new UnsupportedOperationException("Method compare() is not supported for List.")
  }

  override def is(value: Any): Boolean = {
    value.isInstanceOf[List[_]] && value.asInstanceOf[List[_]].map(_.isInstanceOf[PropertyValue]).reduce(_&&_)
  }

  override def getType: Type = Type.LIST

  override def getExactType(bytes: Array[Byte]): Type = {
    val value = get(bytes)
    Type.TYPED_LIST(if(value.isEmpty) Type.NULL else value.head.getExactType)
  }

  @throws[IOException]
  override def get(bytes: Array[Byte]): List[PropertyValue] = {
    val list = mutable.MutableList.empty[PropertyValue]
    try {
      val byteStream = new ByteArrayInputStream(bytes)
      val inputStream = new DataInputStream(byteStream)
      try {
        if (inputStream.skipBytes(PropertyValue.OFFSET) != PropertyValue.OFFSET) {
          throw new IOException("Malformed entry in PropertyValue List.")
        }
        while (inputStream.available > 0) list += PropertyValue.read(inputStream)
      } catch {
        case e: IOException => throw new IOException("Error reading PropertyValue with ListStrategy.", e)
      } finally {
        if (byteStream != null) byteStream.close()
        if (inputStream != null) inputStream.close()
      }
    } catch {
      case e: IOException => throw new IOException("Error reading PropertyValue with ListStrategy.", e)
    }
    list.toList
  }

  @throws[IOException]
  override def getBytes(value: List[PropertyValue]): Array[Byte] = {
    val size = value.map(_.value.length).sum + PropertyValue.OFFSET
    try {
      val byteStream = new ByteArrayOutputStream(size)
      val outputStream = new DataOutputStream(byteStream)
      try {
        outputStream.write(getType.byte)
        for (entry <- value) {
          entry.write(outputStream)
        }
        byteStream.toByteArray
      } catch {
        case e: IOException => throw new IOException("Error writing PropertyValue with ListStrategy.", e)
      } finally {
        if (byteStream != null) byteStream.close()
        if (outputStream != null) outputStream.close()
      }
    } catch {
      case e: IOException => throw new IOException("Error writing PropertyValue with ListStrategy.", e)
    }
  }
}
