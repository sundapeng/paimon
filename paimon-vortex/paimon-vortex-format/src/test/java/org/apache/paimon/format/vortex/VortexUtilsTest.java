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

package org.apache.paimon.format.vortex;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PluginFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.Pair;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VortexUtilsTest {

    private static class TestFileIO extends PluginFileIO {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean isObjectStore() {
            return true;
        }

        @Override
        protected FileIO createFileIO(Path path) {
            throw new UnsupportedOperationException("Not used in tests");
        }

        @Override
        protected ClassLoader pluginClassLoader() {
            return Thread.currentThread().getContextClassLoader();
        }

        void setOptions(Options opts) {
            this.options = opts;
        }
    }

    @Test
    void testNativeLocationWithOssOptions() {
        Path path = new Path("oss://test-bucket/db-name.db/table-name/bucket-0/file.vortex");
        Options options = new Options();
        options.set("fs.oss.endpoint", "oss-example-region.example.com");
        options.set("fs.oss.accessKeyId", "test-key");
        options.set("fs.oss.accessKeySecret", "test-secret");

        TestFileIO fileIO = new TestFileIO();
        fileIO.setOptions(options);

        Pair<String, Map<String, String>> result = VortexUtils.nativeLocation(fileIO, path, true);

        assertEquals("s3://test-bucket/db-name.db/table-name/bucket-0/file.vortex", result.getLeft());
        Map<String, String> storageOptions = result.getValue();
        assertEquals(
                "https://test-bucket.oss-example-region.example.com",
                storageOptions.get("endpoint"));
        assertEquals("test-key", storageOptions.get("access_key_id"));
        assertEquals("test-secret", storageOptions.get("secret_access_key"));
        assertEquals("true", storageOptions.get("virtual_hosted_style_request"));
    }

    @Test
    void testNativeLocationWithoutOssOptionsFailsWithClearError() {
        Path path = new Path("oss://test-bucket/db-name.db/table-name/bucket-0/file.vortex");
        TestFileIO fileIO = new TestFileIO();
        fileIO.setOptions(new Options());

        IllegalArgumentException error =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> VortexUtils.nativeLocation(fileIO, path, true));
        assertTrue(
                error.getMessage().contains("fs.oss.endpoint"),
                "Error should name the missing option but was: " + error.getMessage());
    }

    @Test
    void testNativeLocationPartialOssOptionsFailsWithClearError() {
        Path path = new Path("oss://test-bucket/db-name.db/table-name/bucket-0/file.vortex");
        Options options = new Options();
        options.set("fs.oss.endpoint", "oss-example-region.example.com");
        TestFileIO fileIO = new TestFileIO();
        fileIO.setOptions(options);

        IllegalArgumentException error =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> VortexUtils.nativeLocation(fileIO, path, false));
        assertTrue(
                error.getMessage().contains("fs.oss.accessKeyId"),
                "Error should name the missing option but was: " + error.getMessage());
    }

    @Test
    void testNativeLocationNonOssReturnsNull() {
        Path path = new Path("hdfs://test-bucket/db-name.db/table-name/bucket-0/file.vortex");
        TestFileIO fileIO = new TestFileIO();
        fileIO.setOptions(new Options());

        assertNull(VortexUtils.nativeLocation(fileIO, path, true));
    }
}
