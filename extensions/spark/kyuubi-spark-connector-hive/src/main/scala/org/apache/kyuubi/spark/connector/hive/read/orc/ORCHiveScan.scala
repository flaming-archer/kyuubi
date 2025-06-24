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

package org.apache.kyuubi.spark.connector.hive.read.orc

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTablePartition}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.hive.kyuubi.connector.HiveBridgeHelper.HiveClientImpl
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration
import scala.collection.mutable

import org.apache.kyuubi.spark.connector.hive.read.{AbstractHiveScan, HiveCatalogFileIndex, HiveReader}

case class ORCHiveScan(
                        sparkSession: SparkSession,
                        fileIndex: HiveCatalogFileIndex,
                        catalogTable: CatalogTable,
                        dataSchema: StructType,
                        readDataSchema: StructType,
                        readPartitionSchema: StructType,
                        pushedFilters: Array[Filter] = Array.empty,
                        partitionFilters: Seq[Expression] = Seq.empty,
                        dataFilters: Seq[Expression] = Seq.empty)
  extends AbstractHiveScan(
    sparkSession,
    fileIndex,
    catalogTable,
    dataSchema,
    readDataSchema,
    readPartitionSchema,
    pushedFilters,
    partitionFilters,
    dataFilters
  ) {

  private val isCaseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis

  private val partFileToHivePartMap: mutable.Map[PartitionedFile, CatalogTablePartition] =
    mutable.Map()

  override def isSplitable(path: Path): Boolean = true

  override def createReaderFactory(): PartitionReaderFactory = {
    val hiveConf = new Configuration(fileIndex.hiveCatalog.hadoopConfiguration())
    addCatalogTableConfToConf(hiveConf, catalogTable)

    val table = HiveClientImpl.toHiveTable(catalogTable)
    HiveReader.initializeHiveConf(table, hiveConf, dataSchema, readDataSchema)
    val broadcastHiveConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hiveConf))

    ORCHivePartitionReaderFactory(
      sparkSession.sessionState.conf.clone(),
      broadcastHiveConf,
      table,
      readDataSchema,
      readPartitionSchema,
      partFileToHivePartMap.toMap,
      pushedFilters = pushedFilters,
      isCaseSensitive = isCaseSensitive,
      dataSchema)
  }

  private def addCatalogTableConfToConf(hiveConf: Configuration, table: CatalogTable): Unit = {
    table.properties.foreach {
      case (key, value) =>
        hiveConf.set(key, value)
    }
  }

}
