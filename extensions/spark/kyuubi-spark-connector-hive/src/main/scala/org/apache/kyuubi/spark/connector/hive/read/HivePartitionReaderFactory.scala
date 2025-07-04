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

import org.apache.hadoop.hive.ql.metadata.{Table => HiveTable}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.catalog.CatalogTablePartition
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration

case class HivePartitionReaderFactory(
    sqlConf: SQLConf,
    broadcastHiveConf: Broadcast[SerializableConfiguration],
    hiveTable: HiveTable,
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType,
    partFileToHivePart: Map[PartitionedFile, CatalogTablePartition],
    pushedFilters: Array[Filter] = Array.empty,
    isCaseSensitive: Boolean)
  extends AbstractHivePartitionReaderFactory(
    sqlConf,
    broadcastHiveConf,
    hiveTable,
    readDataSchema,
    partitionSchema,
    partFileToHivePart,
    pushedFilters,
    isCaseSensitive) {}
