/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.spark.connector.hive.read.orc

import java.util.Locale

import org.apache.spark.sql.catalyst.util.{quoteIfNeeded, CaseInsensitiveMap}
import org.apache.spark.sql.sources.{And, Filter}
import org.apache.spark.sql.types._

/**
 * Methods that can be shared when upgrading the built-in Hive.
 */
trait OrcFiltersBase {

  def buildTree(filters: Seq[Filter]): Option[Filter] = {
    filters match {
      case Seq() => None
      case Seq(filter) => Some(filter)
      case Seq(filter1, filter2) => Some(And(filter1, filter2))
      case _ => // length > 2
        val (left, right) = filters.splitAt(filters.length / 2)
        Some(And(buildTree(left).get, buildTree(right).get))
    }
  }

  case class OrcPrimitiveField(fieldName: String, fieldType: DataType)

  /**
   * This method returns a map which contains ORC field name and data type. Each key
   * represents a column; `dots` are used as separators for nested columns. If any part
   * of the names contains `dots`, it is quoted to avoid confusion. See
   * `org.apache.spark.sql.connector.catalog.quoted` for implementation details.
   *
   * BinaryType, UserDefinedType, ArrayType and MapType are ignored.
   */
  def getSearchableTypeMap(
      schema: StructType,
      caseSensitive: Boolean): Map[String, OrcPrimitiveField] = {

    def getPrimitiveFields(
        fields: Seq[StructField],
        parentFieldNames: Seq[String] = Seq.empty): Seq[(String, OrcPrimitiveField)] = {
      fields.flatMap { f =>
        f.dataType match {
          case st: StructType =>
            getPrimitiveFields(st.fields, parentFieldNames :+ f.name)
          case BinaryType => None
          case VarcharType(_) => None
          case _: BooleanType | IntegerType | FloatType | DoubleType | TimestampNTZType
              | TimestampType | DateType | StringType | VarcharType(_) | CharType(_)
              | DecimalType() | YearMonthIntervalType(_, _) | DayTimeIntervalType(_, _) =>
            val fieldName = (parentFieldNames :+ f.name).map(quoteIfNeeded).mkString(".")
            val orcField = OrcPrimitiveField(fieldName, f.dataType)
            Some((fieldName, orcField))
          case _ => None
        }
      }
    }

    val primitiveFields = getPrimitiveFields(schema.fields)
    if (caseSensitive) {
      primitiveFields.toMap
    } else {
      // Don't consider ambiguity here, i.e. more than one field are matched in case insensitive
      // mode, just skip pushdown for these fields, they will trigger Exception when reading,
      // See: SPARK-25175.
      val dedupPrimitiveFields = primitiveFields
        .groupBy(_._1.toLowerCase(Locale.ROOT))
        .filter(_._2.size == 1)
        .transform((_, v) => v.head._2)
      CaseInsensitiveMap(dedupPrimitiveFields)
    }
  }
}
