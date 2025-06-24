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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.read.{Scan, SupportsPushDownAggregates}
import org.apache.spark.sql.execution.datasources.AggregatePushDownUtils
import org.apache.spark.sql.execution.datasources.v2.FileScanBuilder
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

import org.apache.kyuubi.spark.connector.hive.read.orc.{OrcFilters, ORCHiveScan}

case class HiveScanBuilder(
    sparkSession: SparkSession,
    fileIndex: HiveCatalogFileIndex,
    schema: StructType,
    dataSchema: StructType,
    table: CatalogTable)
  extends FileScanBuilder(sparkSession, fileIndex, dataSchema) with SupportsPushDownAggregates {

  private var pushedAggregations = Option.empty[Aggregation]
  private var finalSchema = new StructType()
  private var finalTableType = Option.empty[String]

  override def pushDataFilters(dataFilters: Array[Filter]): Array[Filter] = {
    if (finalTableType.isEmpty) {
      finalTableType = tableType()
    }

    finalTableType match {
      case Some("ORC") if sparkSession.sessionState.conf.orcFilterPushDown =>
        val dataTypeMap = OrcFilters.getSearchableTypeMap(
          readDataSchema(),
          sparkSession.sessionState.conf.caseSensitiveAnalysis)
        OrcFilters.convertibleFilters(dataTypeMap, dataFilters).toArray
      case _ => Array.empty[Filter]
    }
  }

  def tableType(): Option[String] = {
    val serde = table.storage.serde.getOrElse("").toLowerCase(Locale.ROOT)
    val parquet = serde.contains("parquet")
    val orc = serde.contains("orc")
    val provider = table.provider.map(_.toUpperCase(Locale.ROOT))
    if (orc | provider.contains("ORC")) {
      return Some("ORC")
    }

    if (parquet | provider.contains("PARQUET")) {
      return Some("PARQUET")
    }

    None
  }

  override def build(): Scan = {
    // the `finalSchema` is either pruned in pushAggregation (if aggregates are
    // pushed down), or pruned in readDataSchema() (in regular column pruning). These
    // two are mutual exclusive.
    if (pushedAggregations.isEmpty) {
      finalSchema = readDataSchema()
    }

    if (finalTableType.isEmpty) {
      finalTableType = tableType()
    }

    finalTableType match {
      case Some("ORC") => ORCHiveScan(
          sparkSession = sparkSession,
          fileIndex = fileIndex,
          catalogTable = table,
          dataSchema = dataSchema,
          readDataSchema = finalSchema,
          readPartitionSchema = readPartitionSchema(),
          pushedFilters = pushedDataFilters,
          partitionFilters = partitionFilters,
          dataFilters = dataFilters)
      case _ =>
        HiveScan(
          sparkSession = sparkSession,
          fileIndex = fileIndex,
          catalogTable = table,
          dataSchema = dataSchema,
          readDataSchema = finalSchema,
          readPartitionSchema = readPartitionSchema(),
          pushedFilters = pushedDataFilters,
          partitionFilters = partitionFilters,
          dataFilters = dataFilters)
    }
  }

  override def pushAggregation(aggregation: Aggregation): Boolean = {
    if (!sparkSession.sessionState.conf.orcAggregatePushDown) {
      return false
    }

    AggregatePushDownUtils.getSchemaForPushedAggregation(
      aggregation,
      schema,
      partitionNameSet,
      dataFilters) match {

      case Some(schema) =>
        finalSchema = schema
        this.pushedAggregations = Some(aggregation)
        true
      case _ => false
    }
  }
}
