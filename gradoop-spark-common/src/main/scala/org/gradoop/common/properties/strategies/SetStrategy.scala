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
import org.gradoop.common.properties.bytes.Bytes

import scala.collection.mutable

/**
 * Strategy class for handling {@code PropertyValue} operations with a value of the type {@code Set}.
 */
class SetStrategy extends AbstractVariableSizedPropertyValueStrategy[Set[PropertyValue]] {

  @throws[IOException]
  override def read(inputStream: DataInputStream, typeByte: Byte): Set[PropertyValue] = {
    val rawBytes = readBytes(inputStream, typeByte)
    val set = Set.empty[PropertyValue]
    val internalInputStream = createInputStream(rawBytes)
    try while (internalInputStream.available > 0) set + PropertyValue.read(internalInputStream)
    catch {
      case e: IOException => throw new IOException("Error reading PropertyValue with SetStrategy.", e)
    }
    set
  }

  override def compare(value: Set[PropertyValue], other: Any): Int = {
    throw new UnsupportedOperationException("Method compare() is not supported for Set.")
  }

  override def is(value: Any): Boolean = {
    value.isInstanceOf[Set[_]] && value.asInstanceOf[Set[_]].map(_.isInstanceOf[PropertyValue]).reduce(_&&_)
  }

  override def getType: Type = Type.SET

  override def getExactType(bytes: Array[Byte]): Type = {
    val value = get(bytes)
    Type.TYPED_SET(if(value.isEmpty) Type.NULL else value.head.getExactType)
  }

  @throws[IOException]
  override def get(bytes: Array[Byte]): Set[PropertyValue] = {
    val set = mutable.Set.empty[PropertyValue]
    try {
      val byteStream = new ByteArrayInputStream(bytes)
      val inputStream = new DataInputStream(byteStream)
      try {
        inputStream.skipBytes(1)
        while (inputStream.available > 0) set.add(PropertyValue.read(inputStream))
      } catch {
        case e: IOException => throw new IOException("Error reading PropertyValue with SetStrategy.", e)
      } finally {
        if (byteStream != null) byteStream.close()
        if (inputStream != null) inputStream.close()
      }
    } catch {
      case e: IOException => throw new IOException("Error writing PropertyValue with SetStrategy.", e)
    }
    set.toSet
  }

  @throws[IOException]
  override def getBytes(value: Set[PropertyValue]): Array[Byte] = {
    val size = value.map(_.value.length).sum + PropertyValue.OFFSET + Bytes.SIZEOF_SHORT
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
        case e: IOException => throw new IOException("Error writing PropertyValue with SetStrategy.", e)
      } finally {
        if (byteStream != null) byteStream.close()
        if (outputStream != null) outputStream.close()
      }
    } catch {
      case e: IOException => throw new IOException("Error writing PropertyValue with SetStrategy.", e)
    }
  }
}
