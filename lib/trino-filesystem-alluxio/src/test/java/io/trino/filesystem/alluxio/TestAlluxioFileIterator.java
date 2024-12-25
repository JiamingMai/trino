/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.filesystem.alluxio;

import alluxio.client.file.URIStatus;
import alluxio.wire.FileInfo;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.Location;
import org.junit.Rule;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

final class TestAlluxioFileIterator
{
    private final String FILE_NAME = "000000_0";
    private final String FILE_PATH = "/s3a/tables/sales/000000_0";
    private final String UFS_FILE_PATH = "s3a://test-bucket/tables/sales/000000_0";

    @Test
    void testNext() throws IOException {
        String alluxioDirPath = "alluxio://master:8020/s3a/tables/sales";
        FileInfo fileInfo = new FileInfo();
        fileInfo.setName(FILE_NAME);
        fileInfo.setPath(FILE_PATH);
        fileInfo.setUfsPath(UFS_FILE_PATH);
        URIStatus fileStatus = new URIStatus(fileInfo);
        AlluxioFileIterator iterator = new AlluxioFileIterator(
                List.of(fileStatus),
                Location.of(alluxioDirPath));
        FileEntry fileEntry = iterator.next();
        assertThat(fileEntry.location().toString())
                .isEqualTo(alluxioDirPath + "/" + FILE_NAME);

        alluxioDirPath = "alluxio:/s3a/tables/sales";
        fileInfo = new FileInfo();
        fileInfo.setName(FILE_NAME);
        fileInfo.setPath(FILE_PATH);
        fileInfo.setUfsPath(UFS_FILE_PATH);
        fileStatus = new URIStatus(fileInfo);
        iterator = new AlluxioFileIterator(
                List.of(fileStatus),
                Location.of(alluxioDirPath));
        fileEntry = iterator.next();
        assertThat(fileEntry.location().toString())
                .isEqualTo(alluxioDirPath + "/" + FILE_NAME);
    }
}
