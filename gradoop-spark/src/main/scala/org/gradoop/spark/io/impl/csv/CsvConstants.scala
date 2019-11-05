package org.gradoop.spark.io.impl.csv

object CsvConstants {

  /** A function that parses a string and adds the parsed value to the object.
   *
   * @tparam T type
   */
  type ParseFunction[T] = (Option[T], String) => Option[T]

  /** A function that composes a string from a given object.
   *
   * @tparam T type
   */
  type ComposeFunction[T] = (T) => String

  // CSV file structure

  /** System constant file separator. */
  val DIRECTORY_SEPARATOR: String = System.getProperty("file.separator")

  /** Broadcast set identifier for meta data. */
  val BC_METADATA = "metadata"

  /** File ending for CSV files. */
  val CSV_FILE_SUFFIX = ".csv"

  /** Path for indexed vertices */
  val VERTEX_PATH = "vertices"

  /** CSV file for vertices. */
  val VERTEX_FILE: String = "vertices" + CSV_FILE_SUFFIX

  /** Path for indexed graph heads. */
  val GRAPH_HEAD_PATH = "graphs"

  /** CSV file containing the graph heads. */
  val GRAPH_HEAD_FILE: String = "graphs" + CSV_FILE_SUFFIX

  /** Path for indexed edges */
  val EDGE_PATH = "edges"

  /** CSV file for edgesCSV Base constants. */
  val EDGE_FILE: String = "edges" + CSV_FILE_SUFFIX

  /** CSV file for meta data. */
  val METADATA_FILE: String = "metadata" + CSV_FILE_SUFFIX

  // Indexed CSV file structure

  /** File name for indexed data. */
  val SIMPLE_FILE = "data.csv"

  /** Directory to store empty labels with indexed CSV. */
  val DEFAULT_DIRECTORY = '_'

  // CSV string format

  /** Used to separate the tokens (id, label, values) in the CSV files. */
  val TOKEN_DELIMITER = ";"

  /** Used to separate the property values in the CSV files. */
  val VALUE_DELIMITER = "|"

  /** Used to separate lines in the output CSV files. */
  val ROW_DELIMITER: String = System.getProperty("line.separator")

  /** Used to separate entries of list types in the CSV files. */
  val LIST_DELIMITER = ","

  /** Used to separate key and value of maps in the CSV files. */
  val MAP_SEPARATOR = '='

  /** Characters to be escaped in csv strings. */
  val ESCAPED_CHARS: Set[Char] = Set[Char]('\\', ';', ',', '|', ':', '\n', '=')
}
