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

import org.gradoop.common.properties.PropertyValue
import org.gradoop.common.properties.Type
import org.gradoop.common.properties.bytes.Bytes
import scala.math.BigDecimal
import java.io.DataInputStream
import java.io.IOException

/**
 * Strategy class for handling {@link PropertyValue} operations with a value of the type {@link BigDecimal}.
 */
class BigDecimalStrategy extends AbstractVariableSizedPropertyValueStrategy[BigDecimal] {
  @throws[IOException]
  override def read(inputStream: DataInputStream, typeByte: Byte): BigDecimal = {
    BigDecimal(Bytes.toBigDecimal(readBytes(inputStream, typeByte)))
  }

  override def compare(value: BigDecimal, other: Any): Int = {
    other match {
      case num: Number => PropertyValueStrategyUtils.compareNumerical(value, num)
      case _ => throw new IllegalArgumentException("Incompatible types: %s, %s".format(value.getClass, other.getClass))
    }
  }

  override def is(value: Any): Boolean = value.isInstanceOf[BigDecimal]

  override def getType: Type = Type.BIG_DECIMAL

  override def get(bytes: Array[Byte]): BigDecimal = Bytes.toBigDecimal(bytes, PropertyValue.OFFSET, bytes.length - PropertyValue.OFFSET)

  override def getBytes(value: BigDecimal): Array[Byte] = {
    val valueBytes = Bytes.toBytes(value.bigDecimal)
    val rawBytes = new Array[Byte](PropertyValue.OFFSET + valueBytes.length)
    rawBytes(0) = getType.byte
    Bytes.putBytes(rawBytes, PropertyValue.OFFSET, valueBytes, 0, valueBytes.length)
    rawBytes
  }
}
