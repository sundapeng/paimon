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

package org.apache.paimon.spark.sql

import org.apache.paimon.CoreOptions
import org.apache.paimon.catalog.Identifier
import org.apache.paimon.fs.Path
import org.apache.paimon.spark.PaimonSparkTestWithRestCatalogBase
import org.apache.paimon.table.FormatTable

/**
 * SQL-level regression for the Format Table full-scan guard: with
 * `format-table.scan.allow-full-scan` disabled, a query without a partition filter fails before any
 * partition is enumerated, while default behavior and partition-metadata commands stay unchanged.
 * Mirrors the incident where a lost WHERE clause made the driver enumerate every historical
 * partition.
 */
class CatalogManagedFullScanGuardTest extends PaimonSparkTestWithRestCatalogBase {

  test("unfiltered query fails fast when the table disallows full scans") {
    val tableName = "full_scan_guard_table_property"
    withTable(tableName) {
      createGuardedTable(tableName, guarded = true)
      writeCsvPartition(tableName, "20260101", "00", 1)
      writeCsvPartition(tableName, "20260102", "01", 2)
      repair(tableName)

      val error = intercept[Exception] {
        sql(s"SELECT * FROM ${qualified(tableName)}").collect()
      }
      assert(causeMessages(error).contains("Full scan of partitioned Format Table"))
      assert(causeMessages(error).contains(CoreOptions.FORMAT_TABLE_SCAN_ALLOW_FULL_SCAN.key()))

      // Partition-metadata commands must not trip the guard.
      assert(sql(s"SHOW PARTITIONS ${qualified(tableName)}").collect().length == 2)
    }
  }

  test("filtered query succeeds when the table disallows full scans") {
    val tableName = "full_scan_guard_filtered"
    withTable(tableName) {
      createGuardedTable(tableName, guarded = true)
      writeCsvPartition(tableName, "20260101", "00", 1)
      writeCsvPartition(tableName, "20260102", "01", 2)
      repair(tableName)

      val rows = sql(s"SELECT id FROM ${qualified(tableName)} WHERE dt = '20260101'").collect()

      assert(rows.map(_.getInt(0)).toSet == Set(1))
    }
  }

  test("unfiltered query is allowed by default") {
    val tableName = "full_scan_guard_default"
    withTable(tableName) {
      createGuardedTable(tableName, guarded = false)
      writeCsvPartition(tableName, "20260101", "00", 1)
      writeCsvPartition(tableName, "20260102", "01", 2)
      repair(tableName)

      val rows = sql(s"SELECT id FROM ${qualified(tableName)}").collect()

      assert(rows.map(_.getInt(0)).toSet == Set(1, 2))
    }
  }

  test("full scans can be disallowed from the session configuration") {
    val tableName = "full_scan_guard_session"
    withTable(tableName) {
      createGuardedTable(tableName, guarded = false)
      writeCsvPartition(tableName, "20260101", "00", 1)
      repair(tableName)

      spark.conf.set(
        s"spark.paimon.${CoreOptions.FORMAT_TABLE_SCAN_ALLOW_FULL_SCAN.key()}",
        "false")
      val error = intercept[Exception] {
        sql(s"SELECT * FROM ${qualified(tableName)}").collect()
      }
      assert(causeMessages(error).contains("Full scan of partitioned Format Table"))

      spark.conf.unset(s"spark.paimon.${CoreOptions.FORMAT_TABLE_SCAN_ALLOW_FULL_SCAN.key()}")
      spark.catalog.refreshTable(qualified(tableName))
      val rows = sql(s"SELECT id FROM ${qualified(tableName)}").collect()
      assert(rows.map(_.getInt(0)).toSet == Set(1))
    }
  }

  private def createGuardedTable(tableName: String, guarded: Boolean): Unit = {
    val guardProperty =
      if (guarded) s"'${CoreOptions.FORMAT_TABLE_SCAN_ALLOW_FULL_SCAN.key()}' = 'false',"
      else ""
    sql(s"""CREATE TABLE $tableName (id INT, payload STRING, dt STRING, hour STRING)
           |USING CSV
           |PARTITIONED BY (dt, hour)
           |TBLPROPERTIES (
           |  'format-table.implementation' = 'paimon',
           |  $guardProperty
           |  'metastore.partitioned-table' = 'true')
           |""".stripMargin)
  }

  private def repair(tableName: String): Unit =
    sql(s"MSCK REPAIR TABLE ${qualified(tableName)}").collect()

  private def qualified(tableName: String): String = s"paimon.$dbName0.$tableName"

  private def formatTable(tableName: String): FormatTable =
    paimonCatalog.getTable(Identifier.create(dbName0, tableName)).asInstanceOf[FormatTable]

  private def writeCsvPartition(tableName: String, dt: String, hour: String, id: Int): Path = {
    val table = formatTable(tableName)
    val partitionPath = new Path(table.location(), s"dt=$dt/hour=$hour")
    table.fileIO().mkdirs(partitionPath)
    table
      .fileIO()
      .writeFile(new Path(partitionPath, f"part-$id%05d.csv"), s"$id,payload-$id\n", false)
    partitionPath
  }

  private def causeMessages(error: Throwable): String =
    Iterator
      .iterate(error)(_.getCause)
      .takeWhile(_ != null)
      .map(e => String.valueOf(e.getMessage))
      .mkString(" | ")
}
