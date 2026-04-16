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

package org.apache.paimon.mergetree.compact.clustering;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;

import org.junit.jupiter.api.Test;

import static org.apache.paimon.stats.SimpleStats.EMPTY_STATS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ClusteringFilesTest {

    private static DataFileMeta newFile(String name, int level) {
        return DataFileMeta.create(
                name,
                100,
                10,
                BinaryRow.EMPTY_ROW,
                BinaryRow.EMPTY_ROW,
                EMPTY_STATS,
                EMPTY_STATS,
                0,
                0,
                0,
                level,
                null,
                null,
                null,
                null,
                null,
                null);
    }

    @Test
    void testRemoveSortedFileCleansUpMaps() {
        ClusteringFiles files = new ClusteringFiles();
        DataFileMeta sorted = newFile("sorted-1.parquet", 1);
        files.addNewFile(sorted);

        int fileId = files.getFileIdByName("sorted-1.parquet");
        assertThat(files.getFileById(fileId)).isEqualTo(sorted);

        files.removeFile(sorted);

        assertThat(files.getFileById(fileId)).isNull();
        assertThat(files.sortedFiles()).isEmpty();
    }

    @Test
    void testRemoveSortedFileGetFileIdByNameThrows() {
        ClusteringFiles files = new ClusteringFiles();
        DataFileMeta sorted = newFile("sorted-2.parquet", 1);
        files.addNewFile(sorted);
        files.removeFile(sorted);

        assertThatThrownBy(() -> files.getFileIdByName("sorted-2.parquet"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("sorted-2.parquet");
    }

    @Test
    void testRemoveUnsortedFileDoesNotAffectMaps() {
        ClusteringFiles files = new ClusteringFiles();
        DataFileMeta unsorted = newFile("unsorted-1.parquet", 0);
        DataFileMeta sorted = newFile("sorted-3.parquet", 1);
        files.addNewFile(unsorted);
        files.addNewFile(sorted);

        files.removeFile(unsorted);

        assertThat(files.unsortedFiles()).isEmpty();
        int fileId = files.getFileIdByName("sorted-3.parquet");
        assertThat(files.getFileById(fileId)).isEqualTo(sorted);
    }

    @Test
    void testMultipleSortedFilesRemoval() {
        ClusteringFiles files = new ClusteringFiles();
        DataFileMeta f1 = newFile("file-a.parquet", 1);
        DataFileMeta f2 = newFile("file-b.parquet", 1);
        DataFileMeta f3 = newFile("file-c.parquet", 1);
        files.addNewFile(f1);
        files.addNewFile(f2);
        files.addNewFile(f3);

        int idB = files.getFileIdByName("file-b.parquet");
        files.removeFile(f2);

        assertThat(files.getFileById(idB)).isNull();
        assertThat(files.sortedFiles()).containsExactly(f1, f3);
        assertThat(files.getFileById(files.getFileIdByName("file-a.parquet"))).isEqualTo(f1);
        assertThat(files.getFileById(files.getFileIdByName("file-c.parquet"))).isEqualTo(f3);
    }
}
