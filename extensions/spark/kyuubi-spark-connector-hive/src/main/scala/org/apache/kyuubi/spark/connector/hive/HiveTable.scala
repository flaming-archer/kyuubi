/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.spark.connector.hive

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.catalog.TableCapability.{BATCH_READ, BATCH_WRITE, OVERWRITE_BY_FILTER, OVERWRITE_DYNAMIC}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.hive.kyuubi.connector.HiveBridgeHelper.{BucketSpecHelper, LogicalExpressions}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.kyuubi.spark.connector.hive.read.{HiveCatalogFileIndex, HiveScanBuilder}
import org.apache.kyuubi.spark.connector.hive.write.HiveWriteBuilder

case class HiveTable(
    sparkSession: SparkSession,
    catalogTable: CatalogTable,
    hiveTableCatalog: HiveTableCatalog)
  extends Table with SupportsRead with SupportsWrite with Logging {

  lazy val dataSchema: StructType = catalogTable.dataSchema

  lazy val partitionSchema: StructType = catalogTable.partitionSchema

  def rootPaths: Seq[Path] = catalogTable.storage.locationUri.map(new Path(_)).toSeq

  lazy val fileIndex: HiveCatalogFileIndex = {
    val defaultTableSize = sparkSession.sessionState.conf.defaultSizeInBytes
    new HiveCatalogFileIndex(
      sparkSession,
      catalogTable,
      hiveTableCatalog,
      catalogTable.stats.map(_.sizeInBytes.toLong).getOrElse(defaultTableSize))
  }

  override def name(): String = catalogTable.identifier.unquotedString

  override def schema(): StructType = catalogTable.schema

  override def properties(): util.Map[String, String] = catalogTable.properties.asJava

  override def partitioning: Array[Transform] = {
    val partitions = new mutable.ArrayBuffer[Transform]()
    catalogTable.partitionColumnNames.foreach { col =>
      partitions += LogicalExpressions.identity(LogicalExpressions.reference(Seq(col)))
    }
    catalogTable.bucketSpec.foreach { spec =>
      partitions += spec.asTransform
    }
    partitions.toArray
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    HiveScanBuilder(sparkSession, fileIndex, schema, dataSchema, catalogTable)
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    HiveWriteBuilder(sparkSession, catalogTable, info, hiveTableCatalog)
  }

  override def capabilities(): util.Set[TableCapability] = {
    util.EnumSet.of(BATCH_READ, BATCH_WRITE, OVERWRITE_BY_FILTER, OVERWRITE_DYNAMIC)
  }
}
