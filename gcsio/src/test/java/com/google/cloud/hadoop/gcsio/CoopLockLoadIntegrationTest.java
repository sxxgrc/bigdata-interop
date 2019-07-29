/*
 * Copyright 2019 Google LLC. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.gcsio;

import static com.google.cloud.hadoop.gcsio.cooplock.CoopLockRecordsDao.LOCK_DIRECTORY;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.truth.Truth.assertThat;
import static java.util.stream.Collectors.toList;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper;
import com.google.cloud.hadoop.gcsio.testing.TestConfiguration;
import com.google.cloud.hadoop.util.RetryHttpInitializer;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration tests for GoogleCloudStorageFileSystem class. */
@RunWith(JUnit4.class)
public class CoopLockLoadIntegrationTest {

  private static GoogleCloudStorageOptions gcsOptions;
  private static RetryHttpInitializer httpRequestInitializer;
  private static GoogleCloudStorageFileSystemIntegrationHelper gcsfsIHelper;

  @BeforeClass
  public static void before() throws Throwable {
    String projectId =
        checkNotNull(TestConfiguration.getInstance().getProjectId(), "projectId can not be null");
    String appName = GoogleCloudStorageIntegrationHelper.APP_NAME;
    Credential credential =
        checkNotNull(GoogleCloudStorageTestHelper.getCredential(), "credential must not be null");

    gcsOptions =
        GoogleCloudStorageOptions.builder().setAppName(appName).setProjectId(projectId).build();
    httpRequestInitializer =
        new RetryHttpInitializer(
            credential,
            gcsOptions.getAppName(),
            gcsOptions.getMaxHttpRequestRetries(),
            gcsOptions.getHttpRequestConnectTimeout(),
            gcsOptions.getHttpRequestReadTimeout());

    GoogleCloudStorageFileSystem gcsfs =
        new GoogleCloudStorageFileSystem(
            credential,
            GoogleCloudStorageFileSystemOptions.builder()
                .setBucketDeleteEnabled(true)
                .setCloudStorageOptions(gcsOptions)
                .build());

    gcsfsIHelper = new GoogleCloudStorageFileSystemIntegrationHelper(gcsfs);
    gcsfsIHelper.beforeAllTests();
  }

  @AfterClass
  public static void afterClass() throws Throwable {
    gcsfsIHelper.afterAllTests();
    GoogleCloudStorageFileSystem gcsFs = gcsfsIHelper.gcsfs;
    assertThat(gcsFs.exists(new URI("gs://" + gcsfsIHelper.sharedBucketName1))).isFalse();
    assertThat(gcsFs.exists(new URI("gs://" + gcsfsIHelper.sharedBucketName2))).isFalse();
  }

  @Test
  public void moveDirectory_loadTest() throws Exception {
    GoogleCloudStorageFileSystemOptions gcsFsOptions =
        newGcsFsOptions().toBuilder()
            .setCloudStorageOptions(gcsOptions.toBuilder().setMaxHttpRequestRetries(0).build())
            .build();
    GoogleCloudStorageFileSystem gcsFs = newGcsFs(gcsFsOptions, httpRequestInitializer);

    String bucketName = gcsfsIHelper.createUniqueBucket("coop-load");
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String dirName = "rename_" + UUID.randomUUID();
    String fileNamePrefix = "file_";
    URI srcDirUri = bucketUri.resolve(dirName + "_src/");
    URI dstDirUri = bucketUri.resolve(dirName + "_dst/");

    int iterations = 10;

    // create file to rename
    for (int i = 0; i < iterations; i++) {
      gcsfsIHelper.writeTextFile(
          bucketName, srcDirUri.resolve(fileNamePrefix + i).getPath(), "file_content_" + i);
    }

    ExecutorService moveExecutor = Executors.newFixedThreadPool(iterations * 3);
    List<Future<?>> futures = new ArrayList<>(iterations * 3);
    for (int i = 0; i < iterations; i++) {
      futures.add(moveExecutor.submit(() -> renameUnchecked(gcsFs, srcDirUri, dstDirUri)));
      URI srcDir = bucketUri.resolve(dirName + "_src");
      URI dstDir = bucketUri.resolve(dirName + "_dst");
      futures.add(moveExecutor.submit(() -> renameUnchecked(gcsFs, dstDir, srcDir)));
    }
    moveExecutor.shutdown();
    moveExecutor.awaitTermination(10, TimeUnit.MINUTES);
    assertThat(moveExecutor.isTerminated()).isTrue();

    for (Future<?> f : futures) {
      f.get();
    }

    assertThat(gcsFs.exists(srcDirUri)).isTrue();
    assertThat(gcsFs.exists(dstDirUri)).isFalse();
    for (int i = 0; i < iterations; i++) {
      assertThat(gcsFs.exists(srcDirUri.resolve(fileNamePrefix + i))).isTrue();
    }

    // Validate lock files
    List<URI> lockFiles =
        gcsFs.listFileInfo(bucketUri.resolve(LOCK_DIRECTORY)).stream()
            .map(FileInfo::getPath)
            .collect(toList());

    assertThat(lockFiles).hasSize(iterations * 6);
  }

  private static void renameUnchecked(GoogleCloudStorageFileSystem gcsFs, URI srcDir, URI dstDir) {
    do {
      try {
        gcsFs.rename(srcDir, dstDir);
        break;
      } catch (FileNotFoundException e) {
        assertThat(e).hasMessageThat().isEqualTo("Item not found: " + srcDir);
      } catch (IOException e) {
        assertThat(e)
            .hasMessageThat()
            .isEqualTo("Cannot rename because path does not exist: " + srcDir);
      }
    } while (true);
  }

  private static GoogleCloudStorageFileSystemOptions newGcsFsOptions() {
    return newGcsFsOptions(CoopLockLoadIntegrationTest.gcsOptions);
  }

  private static GoogleCloudStorageFileSystemOptions newGcsFsOptions(
      GoogleCloudStorageOptions gcsOptions) {
    return GoogleCloudStorageFileSystemOptions.builder()
        .setCloudStorageOptions(gcsOptions)
        .setCooperativeLockingEnabled(true)
        .build();
  }

  private static GoogleCloudStorageFileSystem newGcsFs(
      GoogleCloudStorageFileSystemOptions gcsFsOptions, HttpRequestInitializer requestInitializer)
      throws IOException {
    GoogleCloudStorageImpl gcs =
        new GoogleCloudStorageImpl(gcsFsOptions.getCloudStorageOptions(), requestInitializer);
    return new GoogleCloudStorageFileSystem(gcs, gcsFsOptions);
  }
}
