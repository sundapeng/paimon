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

package org.apache.paimon.utils;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileIOLoader;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BlobDescriptorUtils}. */
class BlobDescriptorUtilsTest {

    @Test
    void testValidateTableRoot() {
        Path tableRoot = new Path("oss://bucket/table");

        assertThatCode(
                        () ->
                                BlobDescriptorUtils.validateTableRoot(
                                        tableRoot,
                                        new BlobDescriptor(
                                                "oss://bucket/table/bucket-0/data.blob", 0, 1)))
                .doesNotThrowAnyException();

        assertInvalid(tableRoot, "oss://bucket/table-copy/data.blob");
        assertInvalid(tableRoot, "oss://bucket/table/../other/data.blob");
        assertInvalid(tableRoot, "oss://other/table/data.blob");
        assertInvalid(tableRoot, "s3://bucket/table/data.blob");
    }

    @Test
    void testDescriptorOptionsMergeIntoCurrentContext() {
        Options tableOptions = new Options();
        tableOptions.set("blob-descriptor.fs.oss.endpoint", "bbb");
        tableOptions.set("blob-descriptor.fs.oss.credentials.provider", "descriptor-provider");

        Options currentOptions = new Options();
        currentOptions.set("fs.oss.endpoint", "aaa");
        currentOptions.set("fs.oss.credentials.provider", "table-provider");
        currentOptions.set("warehouse", "oss://bucket/warehouse");
        FileIOLoader preferIO = new ConstantSchemeLoader("prefer");
        FileIOLoader fallbackIO = new ConstantSchemeLoader("fallback");
        Configuration hadoopConf = new Configuration();
        hadoopConf.set("fs.defaultFS", "hdfs://localhost:9000");
        CatalogContext currentContext =
                CatalogContext.create(currentOptions, hadoopConf, preferIO, fallbackIO);

        CatalogContext descriptorContext =
                BlobDescriptorUtils.getCatalogContext(currentContext, tableOptions);

        // Descriptor-specific options take precedence over the table's own options.
        assertThat(descriptorContext.options().get("fs.oss.endpoint")).isEqualTo("bbb");
        assertThat(descriptorContext.options().get("fs.oss.credentials.provider"))
                .isEqualTo("descriptor-provider");
        // Non-overridden options, loaders and the Hadoop configuration are inherited.
        assertThat(descriptorContext.options().get("warehouse"))
                .isEqualTo("oss://bucket/warehouse");
        assertThat(descriptorContext.preferIO()).isSameAs(preferIO);
        assertThat(descriptorContext.fallbackIO()).isSameAs(fallbackIO);
        assertThat(descriptorContext.hadoopConf().get("fs.defaultFS"))
                .isEqualTo("hdfs://localhost:9000");
    }

    @Test
    void testNoDescriptorOptionsReturnsCurrentContext() {
        CatalogContext currentContext = CatalogContext.create(new Options());

        assertThat(BlobDescriptorUtils.getCatalogContext(currentContext, new Options()))
                .isSameAs(currentContext);
    }

    @Test
    void testDescriptorOptionsWithoutCurrentContextUseDescriptorOptionsOnly() {
        Options tableOptions = new Options();
        tableOptions.set("blob-descriptor.fs.oss.endpoint", "bbb");

        CatalogContext descriptorContext =
                BlobDescriptorUtils.getCatalogContext(null, tableOptions);

        assertThat(descriptorContext.options().toMap()).containsOnlyKeys("fs.oss.endpoint");
        assertThat(descriptorContext.options().get("fs.oss.endpoint")).isEqualTo("bbb");
        assertThat(descriptorContext.preferIO()).isNull();
        assertThat(descriptorContext.fallbackIO()).isNull();
    }

    private static void assertInvalid(Path tableRoot, String uri) {
        assertThatThrownBy(
                        () ->
                                BlobDescriptorUtils.validateTableRoot(
                                        tableRoot, new BlobDescriptor(uri, 0, 1)))
                .isInstanceOf(IOException.class);
    }

    private static class ConstantSchemeLoader implements FileIOLoader {

        private final String scheme;

        private ConstantSchemeLoader(String scheme) {
            this.scheme = scheme;
        }

        @Override
        public String getScheme() {
            return scheme;
        }

        @Override
        public FileIO load(Path path) {
            throw new UnsupportedOperationException();
        }
    }
}
