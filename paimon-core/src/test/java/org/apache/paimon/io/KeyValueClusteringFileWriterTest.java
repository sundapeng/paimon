/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.io;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SupportsDirectWrite;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.Collections;

import static org.apache.paimon.io.DataFilePathFactory.dataFileToFileIndexPath;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Test for {@link KeyValueClusteringFileWriter}. */
class KeyValueClusteringFileWriterTest {

    private static final Path PATH = new Path("file:/tmp/data-file");
    private static final RowType VALUE_TYPE =
            RowType.builder().field("id", DataTypes.INT()).build();
    // Key field ids must be offset from the value field ids, see keyStatMapping in the writer.
    private static final RowType KEY_TYPE =
            new RowType(
                    Collections.singletonList(
                            new DataField(
                                    SpecialFields.KEY_FIELD_ID_START, "k", DataTypes.INT())));

    @TempDir java.nio.file.Path tempDir;

    @Test
    void testIndexCloseFailureStillClosesAndAbortsDataFile() throws Exception {
        FileIO fileIO = mock(FileIO.class);
        IOException failure = new IOException("index spill failed");
        when(fileIO.newOutputStream(dataFileToFileIndexPath(PATH), true)).thenThrow(failure);
        TestingFormatWriter formatWriter = new TestingFormatWriter();
        KeyValueClusteringFileWriter writer =
                new KeyValueClusteringFileWriter(
                        fileIO,
                        new FileWriterContext(
                                new TestingFormatWriterFactory(formatWriter),
                                SimpleStatsProducer.disabledProducer(),
                                "none"),
                        PATH,
                        KEY_TYPE,
                        VALUE_TYPE,
                        1L,
                        1,
                        false,
                        clusteringOptions(),
                        indexOptions(),
                        false);

        writer.write(
                new KeyValue()
                        .replace(GenericRow.of(1), 1L, RowKind.INSERT, GenericRow.of(10)));

        assertThatThrownBy(writer::close).isSameAs(failure);
        assertThat(formatWriter.closeCalls).isEqualTo(1);
        verify(fileIO).deleteQuietly(PATH);
    }

    private static CoreOptions clusteringOptions() {
        Options options = new Options();
        options.set("clustering.columns", "id");
        return new CoreOptions(options);
    }

    private static FileIndexOptions indexOptions() {
        Options options = new Options();
        options.set("file-index.bitmap.columns", "id");
        options.set("file-index.in-manifest-threshold", "0B");
        return new FileIndexOptions(new CoreOptions(options));
    }

    private static class TestingFormatWriterFactory
            implements FormatWriterFactory, SupportsDirectWrite {

        private final FormatWriter writer;

        private TestingFormatWriterFactory(FormatWriter writer) {
            this.writer = writer;
        }

        @Override
        public FormatWriter create(PositionOutputStream out, String compression) {
            return writer;
        }

        @Override
        public FormatWriter create(FileIO fileIO, Path path, String compression) {
            return writer;
        }
    }

    private static class TestingFormatWriter implements FormatWriter {

        int closeCalls;

        @Override
        public void addElement(org.apache.paimon.data.InternalRow element) {}

        @Override
        public boolean reachTargetSize(boolean suggestedCheck, long targetSize) {
            return false;
        }

        @Override
        public void close() {
            closeCalls++;
        }
    }
}
