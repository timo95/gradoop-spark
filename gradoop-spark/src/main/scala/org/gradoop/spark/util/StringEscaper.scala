package org.gradoop.spark.util

import java.util

import com.google.common.collect.ImmutableBiMap

object StringEscaper {

  /** Escape character. */
  private val ESCAPE_CHARACTER = '\\'

  /** Custom escape sequences to avoid disruptive behavior of the file reader (e.g. newline). */
  private val CUSTOM_ESCAPE_SEQUENCES = new ImmutableBiMap.Builder[Char, CharSequence]()
    .put('\t', s"${ESCAPE_CHARACTER}t")
    .put('\b', s"${ESCAPE_CHARACTER}b")
    .put('\n', s"${ESCAPE_CHARACTER}n")
    .put('\r', s"${ESCAPE_CHARACTER}r")
    .put('\f', s"${ESCAPE_CHARACTER}f")
    .build

  /** Escapes the {@code escapedCharacters} in a string.
   *
   * @param string            string to be escaped
   * @param escapedCharacters characters to be escaped
   * @return escaped string
   */
  def escape(string: String, escapedCharacters: Set[Char]): String = {
    val sb = new StringBuilder
    for (c <- string.toCharArray) {
      if (escapedCharacters.contains(c)) sb.append(escapeCharacter(c))
      else sb.append(c)
    }
    sb.toString
  }

  /** Unescapes the escaped characters in a string.
   *
   * @param escapedString string to be unescaped
   * @return unescaped string
   */
  def unescape(escapedString: String): String = {
    val sb = new StringBuilder
    var escaped = false
    var i = 0
    while ( {
      i < escapedString.length
    }) {
      if (escaped) {
        escaped = false
        sb.append(unescapeSequence(escapedString.subSequence(i - 1, i + 1)))
      }
      else if (escapedString.charAt(i) == ESCAPE_CHARACTER) escaped = true
      else sb.append(escapedString.charAt(i))

      {
        i += 1; i - 1
      }
    }
    sb.toString
  }

  /** Splits an escaped string while ignoring escaped delimiters. Does not unescape the tokens.
   *
   * @param escapedString escaped string to be split
   * @param delimiter     delimiter char
   * @return string array with still escaped strings split by the delimiter
   */
  def split(escapedString: String, delimiter: Char): Array[String] = split(escapedString, delimiter.toString, 0)

  /** Splits an escaped string while ignoring escaped delimiters. Does not unescape the tokens.
   *
   * @param escapedString escaped string to be split
   * @param delimiter     delimiter char
   * @param limit         limits the size of the output array
   * @return string array with still escaped strings split by the delimiter
   */
  def split(escapedString: String, delimiter: Char, limit: Int): Array[String] = split(escapedString, delimiter.toString, limit)

  /** Splits an escaped string while ignoring escaped delimiters. Does not unescape the tokens.
   *
   * @param escapedString escaped string to be split
   * @param delimiter     delimiter string
   * @return string array with still escaped strings split by the delimiter
   * @throws IllegalArgumentException if the delimiter contains the escape character
   */
  @throws[IllegalArgumentException]
  def split(escapedString: String, delimiter: String): Array[String] = split(escapedString, delimiter, 0)

  /** Splits an escaped string while ignoring escaped delimiters. Does not unescape the tokens.
   *
   * @param escapedString escaped string to be split
   * @param delimiter     delimiter string
   * @param limit         limits the size of the output array
   * @return string array with still escaped strings split by the delimiter
   * @throws IllegalArgumentException if the delimiter contains the escape character
   */
  @throws[IllegalArgumentException]
  def split(escapedString: String, delimiter: String, limit: Int): Array[String] = {
    if (delimiter.contains(Character.toString(ESCAPE_CHARACTER)))
      throw new IllegalArgumentException(s"Delimiter must not contain the escape character: '${ESCAPE_CHARACTER}'")
    val realLimit = if (limit <= 0) escapedString.length + 1 else limit
    val tokens = new util.ArrayList[String]
    val sb = new StringBuilder
    var escaped = false
    var delimiterIndex = 0
    for (c <- escapedString.toCharArray) { // parse and match delimiter
      if (!escaped && c == delimiter.charAt(delimiterIndex)) {
        delimiterIndex += 1
        if (delimiterIndex == delimiter.length) {
          if (tokens.size < realLimit - 1) {
            tokens.add(sb.toString)
            sb.setLength(0)
          }
          else sb.append(delimiter.substring(0, delimiterIndex))
          delimiterIndex = 0
        }
      }
      else { // reset delimiter parsing
        sb.append(delimiter.substring(0, delimiterIndex))
        delimiterIndex = 0
        // escape
        if (escaped) escaped = false
        else if (c == ESCAPE_CHARACTER) escaped = true
        sb.append(c)
      }
    }
    sb.append(delimiter.substring(0, delimiterIndex))
    tokens.add(sb.toString)
    tokens.toArray(new Array[String](0))
  }

  /** Returns the escape sequence of a given character.
   *
   * @param character character to be escaped
   * @return escape sequence
   */
  private def escapeCharacter(character: Char): CharSequence = {
    if (CUSTOM_ESCAPE_SEQUENCES.containsKey(character)) return CUSTOM_ESCAPE_SEQUENCES.get(character)
    s"${ESCAPE_CHARACTER}$character"
  }

  /** Returns the character of a given escape sequence.
   *
   * @param sequence escape sequence
   * @return escaped character
   */
  private def unescapeSequence(sequence: CharSequence): Char = {
    if (CUSTOM_ESCAPE_SEQUENCES.containsValue(sequence)) return CUSTOM_ESCAPE_SEQUENCES.inverse.get(sequence)
    sequence.charAt(1)
  }
}
