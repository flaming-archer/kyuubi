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

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output
import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument
import org.apache.hadoop.hive.ql.metadata.{Table => HiveTable}
import org.apache.orc.OrcConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.catalog.CatalogTablePartition
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

import org.apache.kyuubi.spark.connector.hive.read._

case class ORCHivePartitionReaderFactory(
    sqlConf: SQLConf,
    broadcastHiveConf: Broadcast[SerializableConfiguration],
    hiveTable: HiveTable,
    readDataSchema: StructType,
    partitionSchema: StructType,
    partFileToHivePart: Map[PartitionedFile, CatalogTablePartition],
    pushedFilters: Array[Filter] = Array.empty,
    isCaseSensitive: Boolean,
    dataSchema: StructType)
  extends AbstractHivePartitionReaderFactory(
    sqlConf,
    broadcastHiveConf,
    hiveTable,
    readDataSchema,
    partitionSchema,
    partFileToHivePart,
    pushedFilters,
    isCaseSensitive) {

  override def setPushFilters(
      readDataSchema: StructType,
      pushedFilters: Array[Filter] = Array.empty,
      isCaseSensitive: Boolean,
      broadcastHiveConf: Broadcast[SerializableConfiguration]): Unit = {
    // Apply pushed filters to Hive configuration
    OrcFilters.createFilter(readDataSchema, pushedFilters, isCaseSensitive).foreach { f =>
      // Sets pushed predicates
      broadcastHiveConf.value.value.set("sarg.pushdown", toKryo(f))
      broadcastHiveConf.value.value.setBoolean(OrcConf.ALLOW_SARG_TO_FILTER.getAttribute, true)
    }
  }

  // HIVE-11253 moved `toKryo` from `SearchArgument` to `storage-api` module.
  // This is copied from Hive 1.2's SearchArgumentImpl.toKryo().
  private def toKryo(sarg: SearchArgument): String = {
    val kryo = new Kryo()
    val out = new Output(4 * 1024, 10 * 1024 * 1024)
    kryo.writeObject(out, sarg)
    out.close()
    Base64.encodeBase64String(out.toBytes)
  }

}
