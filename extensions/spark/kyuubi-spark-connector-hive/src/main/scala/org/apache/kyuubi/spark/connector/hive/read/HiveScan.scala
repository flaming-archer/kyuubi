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

package org.apache.kyuubi.spark.connector.hive.read

import java.util.Locale

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.hive.kyuubi.connector.HiveBridgeHelper.HiveClientImpl
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

case class HiveScan(
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
    dataFilters) {

  override def isSplitable(path: Path): Boolean = {
    catalogTable.provider.map(_.toUpperCase(Locale.ROOT)).exists {
      case "PARQUET" => true
      case "ORC" => true
      case "HIVE" => isHiveOrcOrParquet(catalogTable.storage)
      case _ => super.isSplitable(path)
    }
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    val hiveConf = new Configuration(fileIndex.hiveCatalog.hadoopConfiguration())
    addCatalogTableConfToConf(hiveConf, catalogTable)

    val table = HiveClientImpl.toHiveTable(catalogTable)
    HiveReader.initializeHiveConf(table, hiveConf, dataSchema, readDataSchema)
    val broadcastHiveConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hiveConf))

    HivePartitionReaderFactory(
      sparkSession.sessionState.conf.clone(),
      broadcastHiveConf,
      table,
      dataSchema,
      readDataSchema,
      readPartitionSchema,
      partFileToHivePartMap.toMap,
      pushedFilters = pushedFilters,
      isCaseSensitive = isCaseSensitive)
  }

  private def addCatalogTableConfToConf(hiveConf: Configuration, table: CatalogTable): Unit = {
    table.properties.foreach {
      case (key, value) =>
        hiveConf.set(key, value)
    }
  }

  private def isHiveOrcOrParquet(storage: CatalogStorageFormat): Boolean = {
    val serde = storage.serde.getOrElse("").toLowerCase(Locale.ROOT)
    serde.contains("parquet") || serde.contains("orc")
  }

}
