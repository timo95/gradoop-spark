package org.gradoop.common.model.impl.id

import java.net.{NetworkInterface, SocketException}
import java.nio.{BufferUnderflowException, ByteBuffer}
import java.security.SecureRandom
import java.util.Date
import java.util.concurrent.atomic.AtomicInteger

case class GradoopId(val bytes: Array[Byte]) extends Ordered[GradoopId] {

  /** Checks if the specified object is equal to the current id.
   *
   * @param o the object to be compared
   * @return true, iff the specified id is equal to this id
   */
  override def equals(o: Any): Boolean = {
    if (super.equals(o)) return true
    if (o == null || (getClass ne o.getClass)) return false
    val firstBytes = this.bytes
    val secondBytes = o.asInstanceOf[GradoopId].bytes
    for (i <- 0 until GradoopId.ID_SIZE) {
      if (firstBytes(i) != secondBytes(i)) return false
    }
    true
  }

  /** Returns the hash code of this GradoopId.
   * <p>
   * Note: Implementation taken from org.bson.types.ObjectId
   *
   * @return hash code
   */
  override def hashCode: Int = {
    var result = getTimeStamp
    result = 31 * result + getMachineIdentifier
    result = 31 * result + getProcessIdentifier.toInt
    result = 31 * result + getCounter
    result
  }

  override def compare(that: GradoopId): Int = {
    for (i <- 0 until GradoopId.ID_SIZE) {
      if (this.bytes(i) != that.bytes(i)) return if ((this.bytes(i) & 0xff) < (that.bytes(i) & 0xff)) -1
      else 1
    }
    0
  }

  /** Returns hex string representation of a GradoopId.
   * <p>
   * Note: Implementation taken from org.bson.types.ObjectId
   *
   * @return GradoopId string representation.
   */
  override def toString: String = {
    val chars = new Array[Char](GradoopId.ID_SIZE * 2)
    for(i <- 0 until GradoopId.ID_SIZE) {
      chars(i * 2) = GradoopId.HEX_CHARS(bytes(i) >> 4 & 0xF)
      chars(i * 2 + 1) = GradoopId.HEX_CHARS(bytes(i) & 0xF)
    }
    String.valueOf(chars)
  }

  //------------------------------------------------------------------------------------------------
  // private little helpers
  //------------------------------------------------------------------------------------------------

  /** Returns the timestamp component of the id.
   *
   * @return the timestamp
   */
  private def getTimeStamp = GradoopId.makeInt(bytes(0), bytes(1), bytes(2), bytes(3))

  /** Returns the machine identifier component of the id.
   *
   * @return the machine identifier
   */
  private def getMachineIdentifier = GradoopId.makeInt(0.toByte, bytes(4), bytes(5), bytes(6))

  /** Returns the process identifier component of the id.
   *
   * @return the process identifier
   */
  private def getProcessIdentifier = GradoopId.makeInt(0.toByte, 0.toByte, bytes(7), bytes(8)).toShort

  /** Returns the counter component of the id.
   *
   * @return the counter
   */
  private def getCounter = GradoopId.makeInt(0.toByte, bytes(9), bytes(10), bytes(11))
}

object GradoopId {
  /** Number of bytes to represent an id internally. */
  val ID_SIZE = 12

  /** Represents a null id. */
  val NULL_VALUE: GradoopId = GradoopId(0, 0, 0.toShort, 0, checkCounter = true)

  /** Integer containing a counter that is increased whenever a new id is created */
  private val NEXT_COUNTER = new AtomicInteger(new SecureRandom().nextInt)

  /** Bit mask used to extract the lowest three bytes of four */
  private val LOW_ORDER_THREE_BYTES = 0x00ffffff

  /** Bit mask used to extract the highest byte of four */
  private val HIGH_ORDER_ONE_BYTE = 0xff000000

  /** Required for {@link GradoopId#toString()} */
  private val HEX_CHARS = Array[Char]('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f')

  private val MACHINE_IDENTIFIER: Int = createMachineIdentifier
  private val PROCESS_IDENTIFIER: Short = createProcessIdentifier

  /** Creates the machine identifier from the network interface.
   * <p>
   * Note: Implementation taken from org.bson.types.ObjectId
   *
   * @return a short representing the process
   */
  private def createMachineIdentifier: Int = {
    var machinePiece: Int = 0
    try {
      val sb: StringBuilder = new StringBuilder
      import scala.collection.JavaConverters._
      for(interface <- NetworkInterface.getNetworkInterfaces.asScala) {
        sb.append(interface.toString)
        val mac: Array[Byte] = interface.getHardwareAddress
        if (mac != null) {
          val bb: ByteBuffer = ByteBuffer.wrap(mac)
          try {
            sb.append(bb.getChar)
            sb.append(bb.getChar)
            sb.append(bb.getChar)
          } catch {
            case _: BufferUnderflowException =>
            // mac with less than 6 bytes. continue
          }
        }
      }
      machinePiece = sb.toString.hashCode
    } catch {
      case _: SocketException => machinePiece = new SecureRandom().nextInt
    }
    machinePiece = machinePiece & LOW_ORDER_THREE_BYTES
    machinePiece
  }

  /** Creates the process identifier. This does not have to be unique per class loader because
   * NEXT_COUNTER will provide the uniqueness.
   * <p>
   * Note: Implementation taken from org.bson.types.ObjectId
   *
   * @return a short representing the process
   */
  private def createProcessIdentifier: Short = {
    var processId: Short = 0
    val processName: String = java.lang.management.ManagementFactory.getRuntimeMXBean.getName
    if (processName.contains("@")) processId = processName.substring(0, processName.indexOf('@')).toInt.toShort
    else processId = java.lang.management.ManagementFactory.getRuntimeMXBean.getName.hashCode.toShort
    processId
  }

  /** Converts a date into the seconds since unix epoch.
   *
   * @param time a time
   * @return int representing the seconds between unix epoch and the given time
   */
  private def dateToTimestampSeconds(time: Date) = (time.getTime / 1000).toInt

  /** Creates a GradoopId using the given time, machine identifier, process identifier, and counter.
   *
   * Note: Implementation taken from org.bson.types.ObjectId
   *
   * @param timestamp         the time in seconds
   * @param machineIdentifier the machine identifier
   * @param processIdentifier the process identifier
   * @param counter           the counter
   * @param checkCounter      if the constructor should test if the counter is between 0 and 16777215
   */
  private def apply(timestamp: Int, machineIdentifier: Int, processIdentifier: Short, counter: Int, checkCounter: Boolean): GradoopId = {
    if ((machineIdentifier & HIGH_ORDER_ONE_BYTE) != 0) throw new IllegalArgumentException("The machine identifier must be between 0" + " and 16777215 (it must fit in three bytes).")
    if (checkCounter && ((counter & HIGH_ORDER_ONE_BYTE) != 0)) throw new IllegalArgumentException("The counter must be between 0" + " and 16777215 (it must fit in three bytes).")

    val buffer = ByteBuffer.allocate(12)

    buffer.put((timestamp >> 24).toByte)
    buffer.put((timestamp >> 16).toByte)
    buffer.put((timestamp >> 8).toByte)
    buffer.put(timestamp.toByte)

    buffer.put((machineIdentifier >> 16).toByte)
    buffer.put((machineIdentifier >> 8).toByte)
    buffer.put(machineIdentifier.toByte)

    buffer.put((processIdentifier >> 8).toByte)
    buffer.put(processIdentifier.toByte)

    buffer.put((counter >> 16).toByte)
    buffer.put((counter >> 8).toByte)
    buffer.put(counter.toByte)

    new GradoopId(buffer.array)
  }


  def get: GradoopId = apply(dateToTimestampSeconds(new Date), MACHINE_IDENTIFIER, PROCESS_IDENTIFIER, NEXT_COUNTER.getAndIncrement, false)

  /** Returns the Gradoop ID represented by a specified hexadecimal string.
   *
   * Note: Implementation taken from org.bson.types.ObjectId
   *
   * @param string hexadecimal GradoopId representation
   * @return GradoopId
   */
  def fromString(string: String): GradoopId = {
    if (!GradoopId.isValid(string))
      throw new IllegalArgumentException("invalid hexadecimal representation of a GradoopId: [" + string + "]")
    val bytes = (0 to string.length-2 by 2)
      .map(i => Integer.parseInt(string.substring(i, i + 2), 16).toByte)
      .toArray
    new GradoopId(bytes)
  }

  /** Checks if a string can be transformed into a GradoopId.
   *
   * Note: Implementation taken from org.bson.types.ObjectId
   *
   * @param hexString a potential GradoopId as a String.
   * @return whether the string could be an object id
   */
  def isValid(hexString: String): Boolean = {
    if (hexString.length != 2 * ID_SIZE) return false
    hexString.foreach(c =>
      if(!(c >= '0' && c <= '9') && !(c >= 'a' && c <= 'f') && !(c >= 'A' && c <= 'F')) return false)
    true
  }

  //------------------------------------------------------------------------------------------------
  // helper functions
  //------------------------------------------------------------------------------------------------

  /** Compares the given GradoopIds and returns the smaller one. It both are equal, the first argument is returned.
   *
   * @param first  first GradoopId
   * @param second second GradoopId
   * @return smaller GradoopId or first if equal
   */
  def min(first: GradoopId, second: GradoopId): GradoopId = {
    val comparison = first.compareTo(second)
    if (comparison == 0) first
    else if (comparison < 0) first
    else second
  }

  /** Returns a primitive int represented by the given 4 bytes.
   *
   * @param b3 byte 3
   * @param b2 byte 2
   * @param b1 byte 1
   * @param b0 byte 0
   * @return int value
   */
  private def makeInt(b3: Byte, b2: Byte, b1: Byte, b0: Byte) =
    (b3 << 24) | ((b2 & 0xff) << 16) | ((b1 & 0xff) << 8) | b0 & 0xff
}