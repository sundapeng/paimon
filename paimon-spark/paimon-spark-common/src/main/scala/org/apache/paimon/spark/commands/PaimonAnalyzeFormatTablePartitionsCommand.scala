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

package org.apache.paimon.spark.commands

import org.apache.paimon.spark.format.PaimonFormatTable
import org.apache.paimon.spark.leafnode.PaimonLeafRunnableCommand
import org.apache.paimon.spark.util.OptionUtils
import org.apache.paimon.table.format.FormatTablePartitionStatsCollector

import org.apache.spark.sql.{Row, SparkSession}

import java.util.{Map => JMap}

import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap

/**
 * Recomputes the catalog statistics of a Format Table with catalog-managed partitions, backing
 * `ANALYZE TABLE t [PARTITION(...)] COMPUTE STATISTICS [NOSCAN]`.
 *
 * The partitions are measured from storage and the result replaces what the catalog holds, so this
 * is how a table drifts back into agreement after writers the catalog never saw. `NOSCAN` stops at
 * what a directory listing gives — file count, byte size, last file creation time — while a full
 * ANALYZE also reads each file footer for its row count. Formats that carry no footer (CSV, TEXT,
 * JSON) keep an unknown row count either way rather than a guessed one.
 *
 * Analyzing is not a way to add or remove partitions: it measures the ones registered at the time
 * of the listing and re-registers exactly those. There is no lock between the listing and the
 * write, so a partition dropped concurrently can be re-registered with its last measurement — the
 * same last-writer-wins window every lock-free partition operation on these tables has. A
 * `PARTITION(...)` clause selects a leading subset of them.
 */
case class PaimonAnalyzeFormatTablePartitionsCommand(
    v2Table: PaimonFormatTable,
    partitionSpec: Map[String, Option[String]],
    noScan: Boolean)
  extends PaimonLeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val prefix = leadingPrefix(sparkSession)
    val partitions = v2Table.partitionManager
      .listPartitions(prefix.asJava, null)
      .asScala
      .map(_.spec().asInstanceOf[JMap[String, String]])
      .toList

    if (partitions.isEmpty && prefix.nonEmpty) {
      throw new IllegalArgumentException(
        s"Partition ${prefix.map { case (k, v) => s"$k=$v" }.mkString("(", ", ", ")")} of table " +
          s"${v2Table.name()} does not exist, so there is nothing to measure.")
    }

    if (partitions.nonEmpty) {
      val collector = new FormatTablePartitionStatsCollector(
        v2Table.table,
        !noScan,
        OptionUtils.formatTableStatisticsParallelism())
      val statistics = collector.collect(partitions.asJava)
      v2Table.partitionManager
        .createPartitions(partitions.asJava, true, statistics, true)
    }
    Seq.empty[Row]
  }

  /**
   * The values the `PARTITION(...)` clause fixes, as a leading prefix of the partition keys — the
   * shape the catalog can select on.
   *
   * This follows what Spark does with the same clause on a metastore table: column names resolve
   * under the session's case sensitivity, a column named without a value means every value of it,
   * and the columns that do carry a value have to be a leading run. `PARTITION (dt = 'x', hour)`
   * therefore selects every hour of that day and `PARTITION (dt, hour)` selects everything, while
   * `PARTITION (hour = '00')` is rejected: the catalog cannot select on a non-leading key, and
   * quietly widening it would measure more partitions than were asked for.
   */
  private def leadingPrefix(sparkSession: SparkSession): Map[String, String] = {
    if (partitionSpec.isEmpty) {
      return Map.empty
    }
    val resolver = sparkSession.sessionState.conf.resolver
    val partitionKeys = v2Table.table.partitionKeys().asScala.toSeq
    val normalized = partitionSpec.map {
      case (key, value) =>
        val resolved = partitionKeys
          .find(partitionKey => resolver(partitionKey, key))
          .getOrElse(
            throw new IllegalArgumentException(
              s"$key is not a partition column of ${v2Table.name()}, whose partition columns are " +
                partitionKeys.mkString("[", ", ", "]")))
        resolved -> value
    }
    val valueByKey = partitionKeys.map(key => key -> normalized.get(key).flatten)
    val prefix = valueByKey.takeWhile(_._2.isDefined)
    if (valueByKey.drop(prefix.size).exists(_._2.isDefined)) {
      throw new IllegalArgumentException(
        s"ANALYZE TABLE ${v2Table.name()} PARTITION must give values for a leading run of its " +
          s"partition columns ${partitionKeys.mkString("[", ", ", "]")}, but got " +
          partitionSpec.keys.mkString("[", ", ", "]"))
    }
    // Kept in partition-key order, so a message built from it reads in that order too.
    ListMap(prefix.map { case (key, value) => key -> value.get }: _*)
  }
}
