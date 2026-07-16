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

package org.apache.paimon.spark.table

import org.apache.paimon.catalog.Identifier
import org.apache.paimon.fs.Path
import org.apache.paimon.spark.{FormatTableScanBuilder, PaimonFormatTableScan, PaimonRecordReaderIterator, PaimonSparkTestWithRestCatalogBase}
import org.apache.paimon.table.FormatTable

import org.apache.spark.sql.PaimonUtils.translateFilterV2
import org.apache.spark.sql.catalyst.plans.logical.Filter

class PaimonFormatTableLimitPushdownTest extends PaimonSparkTestWithRestCatalogBase {

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    sql("USE paimon")
    sql("CREATE DATABASE IF NOT EXISTS test_db")
    sql("USE test_db")
  }

  test("PaimonFormatTable: pushed limit must not truncate splits before data filtering") {
    val tableName = "paimon_format_limit_after_filter"
    withTable(tableName) {
      sql(
        s"CREATE TABLE $tableName (id INT) USING CSV TBLPROPERTIES (" +
          "'format-table.implementation'='paimon', 'file.compression'='none', " +
          "'source.split.target-size'='1b', 'source.split.open-file-cost'='0b')")
      val table =
        paimonCatalog.getTable(Identifier.create("test_db", tableName)).asInstanceOf[FormatTable]

      // File-name ordering makes the predicate-empty split come first.
      table.fileIO().writeFile(new Path(table.location(), "part-00000.csv"), "0", false)
      table.fileIO().writeFile(new Path(table.location(), "part-00001.csv"), "1", false)

      val condition =
        sql(s"SELECT * FROM $tableName WHERE id = 1").queryExecution.optimizedPlan.collectFirst {
          case filter: Filter => filter.condition
        }.get
      val dataPredicate = translateFilterV2(condition).get

      def buildScan(pushLimit: Boolean): PaimonFormatTableScan = {
        val builder = FormatTableScanBuilder(table)
        builder.pushPredicates(Array(dataPredicate))
        if (pushLimit) {
          assert(!builder.pushLimit(1))
        }
        builder.build()
      }

      def readFiltered(scan: PaimonFormatTableScan): Seq[Int] = {
        val read = scan.readBuilder.newRead().executeFilter()
        scan.inputSplits.flatMap {
          split =>
            val iterator = PaimonRecordReaderIterator(read.createReader(split), Seq.empty, split)
            try {
              val rows = scala.collection.mutable.ArrayBuffer.empty[Int]
              while (iterator.hasNext) {
                rows += iterator.next().getInt(0)
              }
              rows
            } finally {
              iterator.close()
            }
        }.toSeq
      }

      val globalLimitOracle = readFiltered(buildScan(pushLimit = false)).take(1)
      val pushedLimitScan = buildScan(pushLimit = true)

      assert(globalLimitOracle == Seq(1))
      assert(pushedLimitScan.pushedLimit.isEmpty)
      assert(pushedLimitScan.inputSplits.length == 2)
      assert(readFiltered(pushedLimitScan).take(1) == globalLimitOracle)
    }
  }
}
