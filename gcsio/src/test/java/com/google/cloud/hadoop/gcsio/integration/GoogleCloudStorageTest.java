/**
 * Copyright 2014 Google Inc. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.gcsio.integration;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.fail;

import com.google.api.client.util.Clock;
import com.google.cloud.hadoop.gcsio.CacheSupplementedGoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.CreateFileOptions;
import com.google.cloud.hadoop.gcsio.CreateObjectOptions;
import com.google.cloud.hadoop.gcsio.FileSystemBackedDirectoryListCache;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageItemInfo;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageWriteChannel;
import com.google.cloud.hadoop.gcsio.InMemoryDirectoryListCache;
import com.google.cloud.hadoop.gcsio.LaggedGoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.LaggedGoogleCloudStorage.ListVisibilityCalculator;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.cloud.hadoop.gcsio.UpdatableItemInfo;
import com.google.cloud.hadoop.gcsio.VerificationAttributes;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.TestBucketHelper;
import com.google.cloud.hadoop.gcsio.testing.InMemoryGoogleCloudStorage;
import com.google.common.base.Equivalence;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Ints;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class GoogleCloudStorageTest {

  // This string is used to prefix all bucket names that are created for GCS IO integration testing
  private static final String BUCKET_NAME_PREFIX = "gcsio-it";

  private static final Supplier<TestBucketHelper> BUCKET_HELPER =
      Suppliers.memoize(
          new Supplier<TestBucketHelper>() {
            @Override
            public TestBucketHelper get() {
              return new TestBucketHelper(BUCKET_NAME_PREFIX);
            }
          });

  private static final LoadingCache<GoogleCloudStorage, String> SHARED_BUCKETS =
      CacheBuilder.newBuilder()
          .build(
              new CacheLoader<GoogleCloudStorage, String>() {
                @Override
                public String load(GoogleCloudStorage gcs) throws Exception {
                  return createUniqueBucket(gcs, "shared");
                }
              });

  private static String createUniqueBucket(GoogleCloudStorage gcs, String suffix)
      throws IOException {
    String bucketName = getUniqueBucketName(suffix) + "_" + gcs.hashCode();
    gcs.create(bucketName);
    return bucketName;
  }

  private static String getUniqueBucketName(String suffix) {
    return BUCKET_HELPER.get().getUniqueBucketName(suffix);
  }

  /** An Equivalence for byte arrays. */
  public static final Equivalence<byte[]> BYTE_ARRAY_EQUIVALENCE = new Equivalence<byte[]>() {
    @Override
    protected boolean doEquivalent(byte[] bytes, byte[] bytes2) {
      return Arrays.equals(bytes, bytes2);
    }

    @Override
    protected int doHash(byte[] bytes) {
      return Arrays.hashCode(bytes);
    }
  };

  /**
   * Static instance which we can use inside long-lived GoogleCloudStorage instances, but still
   * may reconfigure to point to new temporary directories in each test case.
   */
  protected static FileSystemBackedDirectoryListCache fileBackedCache =
      FileSystemBackedDirectoryListCache.getUninitializedInstanceForTest();

  // Test classes using JUnit4 runner must have only a single constructor. Since we
  // want to be able to pass in dependencies, we'll maintain this base class as
  // @Parameterized with @Parameters.
  @Parameters
  public static Collection<Object[]> getConstructorArguments() throws IOException {
    GoogleCloudStorage gcs =
        new InMemoryGoogleCloudStorage();
    GoogleCloudStorage zeroLaggedGcs =
        new LaggedGoogleCloudStorage(
            new InMemoryGoogleCloudStorage(),
            Clock.SYSTEM,
            ListVisibilityCalculator.IMMEDIATELY_VISIBLE);
    GoogleCloudStorage cachedGcs =
        new CacheSupplementedGoogleCloudStorage(
            new InMemoryGoogleCloudStorage(),
            InMemoryDirectoryListCache.getInstance());
    GoogleCloudStorage cachedLaggedGcs =
        new CacheSupplementedGoogleCloudStorage(
          new LaggedGoogleCloudStorage(
              new InMemoryGoogleCloudStorage(),
              Clock.SYSTEM,
              ListVisibilityCalculator.DEFAULT_LAGGED),
          InMemoryDirectoryListCache.getInstance());
    GoogleCloudStorage cachedFilebackedLaggedGcs =
        new CacheSupplementedGoogleCloudStorage(
          new LaggedGoogleCloudStorage(
              new InMemoryGoogleCloudStorage(),
              Clock.SYSTEM,
              ListVisibilityCalculator.DEFAULT_LAGGED),
          fileBackedCache);
    return Arrays.asList(new Object[][]{
        {gcs},
        {zeroLaggedGcs},
        {cachedGcs},
        {cachedLaggedGcs},
        {cachedFilebackedLaggedGcs}
    });
  }

  @Rule
  public TemporaryFolder tempDirectoryProvider = new TemporaryFolder();

  private final GoogleCloudStorage rawStorage;

  public GoogleCloudStorageTest(GoogleCloudStorage rawStorage) {
    this.rawStorage = rawStorage;
  }

  @Before
  public void setUp() throws IOException {
    // Point the shared static cache instance at a new temp directory.
    fileBackedCache.setBasePath(tempDirectoryProvider.newFolder("gcs-metadata").toString());
  }

  @AfterClass
  public static void cleanupBuckets() throws IOException {
    // Use any GCS object (from tested ones) for clean up
    BUCKET_HELPER.get().cleanup(Iterables.getLast(SHARED_BUCKETS.asMap().keySet()));
  }

  private String createUniqueBucket(String suffix) throws IOException {
    return createUniqueBucket(rawStorage, suffix);
  }

  private String getSharedBucketName() {
    return SHARED_BUCKETS.getUnchecked(rawStorage);
  }

  @Test
  public void testCreateSuccessfulBucket() throws IOException {
    String bucketName = createUniqueBucket("create_successful");

    // Verify the bucket exist by creating an object
    StorageResourceId objectToCreate = new StorageResourceId(bucketName, "CreateTestObject");
    rawStorage.createEmptyObject(objectToCreate);
  }

  @Test
  public void testCreateExistingBucket() throws IOException {
    String bucketName = createUniqueBucket("create_existing");

    try {
      rawStorage.create(bucketName);
      fail();
    } catch (IOException e) {
      // expected
    }
  }

  @Test
  public void testCreateInvalidBucket() throws IOException {
    // Buckets must start with a letter or number
    String bucketName = "--" + getUniqueBucketName("create_invalid");

    try {
      rawStorage.create(bucketName);
      fail();
    } catch (IOException e) {
      // expected
    }
  }

  @Test
  public void testCreateObject() throws IOException {
    String bucketName = getSharedBucketName();

    byte[] bytesToWrite = "SomeText".getBytes(StandardCharsets.UTF_8);
    // Verify the bucket exist by creating an object
    StorageResourceId objectToCreate =
        new StorageResourceId(bucketName, "testCreateObject_CreateTestObject");
    try (WritableByteChannel channel = rawStorage.create(objectToCreate)) {
      channel.write(ByteBuffer.wrap(bytesToWrite));
    }

    GoogleCloudStorageTestHelper.assertObjectContent(rawStorage, objectToCreate, bytesToWrite);
  }

  @Test
  public void testCreateInvalidObject() throws IOException {
    String bucketName = getSharedBucketName();

    byte[] bytesToWrite = "SomeText".getBytes(StandardCharsets.UTF_8);
    // Verify the bucket exist by creating an object
    StorageResourceId objectToCreate =
        new StorageResourceId(bucketName, "testCreateObject_CreateInvalidTestObject\n");

    try {
      try (WritableByteChannel channel = rawStorage.create(objectToCreate)) {
        channel.write(ByteBuffer.wrap(bytesToWrite));
      }
      fail();
    } catch (IOException e) {
      // expected
    }
  }

  @Test
  public void testCreateZeroLengthObjectUsingCreate() throws IOException {
    String bucketName = getSharedBucketName();

    byte[] bytesToWrite = new byte[0];

    StorageResourceId objectToCreate =
        new StorageResourceId(
            bucketName, "testCreateZeroLengthObjectUsingCreate_CreateEmptyTestObject");
    try (WritableByteChannel channel = rawStorage.create(objectToCreate)) {
      channel.write(ByteBuffer.wrap(bytesToWrite));
    }

    GoogleCloudStorageTestHelper.assertObjectContent(rawStorage, objectToCreate, bytesToWrite);
  }

  @Test
  public void testCreate1PageLengthObjectUsingCreate() throws IOException {
    String bucketName = getSharedBucketName();
    byte[] bytesToWrite = new byte[GoogleCloudStorageWriteChannel.UPLOAD_PIPE_BUFFER_SIZE_DEFAULT];
    GoogleCloudStorageTestHelper.fillBytes(bytesToWrite);

    StorageResourceId objectToCreate =
        new StorageResourceId(
            bucketName, "testCreate1PageLengthObjectUsingCreate_Create1PageTestObject");
    try (WritableByteChannel channel = rawStorage.create(objectToCreate)) {
      channel.write(ByteBuffer.wrap(bytesToWrite));
    }

    GoogleCloudStorageTestHelper.assertObjectContent(rawStorage, objectToCreate, bytesToWrite);
  }

  @Test
  public void testCreate1PageLengthPlus1byteObjectUsingCreate() throws IOException {
    String bucketName = getSharedBucketName();
    byte[] bytesToWrite =
        new byte[GoogleCloudStorageWriteChannel.UPLOAD_PIPE_BUFFER_SIZE_DEFAULT + 1];
    GoogleCloudStorageTestHelper.fillBytes(bytesToWrite);

    StorageResourceId objectToCreate =
        new StorageResourceId(
            bucketName,
            "testCreate1PageLengthPlus1byteObjectUsingCreate_Create1PagePlusOneTestObject");

    try (WritableByteChannel channel = rawStorage.create(objectToCreate)) {
      channel.write(ByteBuffer.wrap(bytesToWrite));
    }

    GoogleCloudStorageTestHelper.assertObjectContent(rawStorage, objectToCreate, bytesToWrite);
  }

  @Test
  public void testCreateExistingObject() throws IOException {
    String bucketName = getSharedBucketName();
    byte[] bytesToWrite = "SomeText".getBytes(StandardCharsets.UTF_8);

    StorageResourceId objectToCreate =
        new StorageResourceId(bucketName, "testCreateExistingObject_CreateExistingObject");

    try (WritableByteChannel channel = rawStorage.create(objectToCreate)) {
      channel.write(ByteBuffer.wrap(bytesToWrite));
    }

    byte[] overwriteBytesToWrite = "OverwriteText".getBytes(StandardCharsets.UTF_8);

    // We need to write data and close to trigger an IOException.
    try (WritableByteChannel channel2 = rawStorage.create(objectToCreate)) {
      channel2.write(ByteBuffer.wrap(overwriteBytesToWrite));
    }

    GoogleCloudStorageTestHelper.assertObjectContent(
        rawStorage, objectToCreate, overwriteBytesToWrite);
  }

  @Test
  public void testCreateEmptyObject() throws IOException {
    String bucketName = getSharedBucketName();

    StorageResourceId objectToCreate =
        new StorageResourceId(bucketName, "testCreateEmptyObject_CreateEmptyObject");

    rawStorage.createEmptyObject(objectToCreate);

    GoogleCloudStorageItemInfo itemInfo = rawStorage.getItemInfo(objectToCreate);

    assertThat(itemInfo.exists()).isTrue();
    assertThat(itemInfo.getSize()).isEqualTo(0);
  }

  @Test
  public void testCreateEmptyExistingObject() throws IOException {
    String bucketName = getSharedBucketName();

    StorageResourceId objectToCreate =
        new StorageResourceId(
            bucketName, "testCreateEmptyExistingObject_CreateEmptyExistingObject");

    rawStorage.createEmptyObject(objectToCreate);

    GoogleCloudStorageItemInfo itemInfo = rawStorage.getItemInfo(objectToCreate);

    assertThat(itemInfo.exists()).isTrue();
    assertThat(itemInfo.getSize()).isEqualTo(0);

    rawStorage.createEmptyObject(objectToCreate);

    GoogleCloudStorageItemInfo secondItemInfo = rawStorage.getItemInfo(objectToCreate);

    assertThat(secondItemInfo.exists()).isTrue();
    assertThat(secondItemInfo.getSize()).isEqualTo(0);
    assertThat(secondItemInfo.getCreationTime()).isNotSameAs(itemInfo.getCreationTime());
  }

  @Test
  public void testGetSingleItemInfo() throws IOException {
    String bucketName = getSharedBucketName();

    StorageResourceId objectToCreate =
        new StorageResourceId(bucketName, "testGetSingleItemInfo_GetSingleItemInfoObject");

    rawStorage.createEmptyObject(objectToCreate);

    GoogleCloudStorageItemInfo itemInfo = rawStorage.getItemInfo(objectToCreate);

    assertThat(itemInfo.exists()).isTrue();
    assertThat(itemInfo.getSize()).isEqualTo(0);

    StorageResourceId secondObjectToCreate =
        new StorageResourceId(bucketName, "testGetSingleItemInfo_GetSingleItemInfoObject2");

    byte[] bytesToWrite = new byte[100];
    GoogleCloudStorageTestHelper.fillBytes(bytesToWrite);

    try (WritableByteChannel channel = rawStorage.create(secondObjectToCreate)) {
      channel.write(ByteBuffer.wrap(bytesToWrite));
    }

    GoogleCloudStorageItemInfo secondItemInfo = rawStorage.getItemInfo(secondObjectToCreate);

    assertThat(secondItemInfo.exists()).isTrue();
    assertThat(secondItemInfo.getSize()).isEqualTo(100);
    assertThat(secondItemInfo.isBucket()).isFalse();
    assertThat(secondItemInfo.isRoot()).isFalse();

    GoogleCloudStorageItemInfo nonExistentItemInfo =
        rawStorage.getItemInfo(
            new StorageResourceId(bucketName, "testGetSingleItemInfo_SomethingThatDoesntExist"));

    assertThat(nonExistentItemInfo.exists()).isFalse();
    assertThat(nonExistentItemInfo.isBucket()).isFalse();
    assertThat(nonExistentItemInfo.isRoot()).isFalse();

    // Test bucket get item info
    GoogleCloudStorageItemInfo bucketInfo =
        rawStorage.getItemInfo(new StorageResourceId(bucketName));
    assertThat(bucketInfo.exists()).isTrue();
    assertThat(bucketInfo.isBucket()).isTrue();

    GoogleCloudStorageItemInfo rootInfo = rawStorage.getItemInfo(StorageResourceId.ROOT);
    assertThat(rootInfo.exists()).isTrue();
    assertThat(rootInfo.isRoot()).isTrue();
  }

  @Test
  public void testGetMultipleItemInfo() throws IOException {
    String bucketName = getSharedBucketName();

    List<StorageResourceId> objectsCreated = new ArrayList<>();

    for (int i = 0; i < 3; i++) {
      StorageResourceId objectToCreate =
          new StorageResourceId(
              bucketName, String.format("testGetMultipleItemInfo_GetMultiItemInfoObject_%s", i));
      rawStorage.createEmptyObject(objectToCreate);
      objectsCreated.add(objectToCreate);
    }

    StorageResourceId bucketResourceId = new StorageResourceId(bucketName);
    StorageResourceId nonExistentResourceId =
        new StorageResourceId(bucketName, "testGetMultipleItemInfo_IDontExist");

    List<StorageResourceId> allResources = Lists.newArrayList();
    allResources.addAll(objectsCreated);
    allResources.add(nonExistentResourceId);
    allResources.add(bucketResourceId);

    List<GoogleCloudStorageItemInfo> allInfo = rawStorage.getItemInfos(allResources);

    for (int i = 0; i < objectsCreated.size(); i++) {
      StorageResourceId resourceId = objectsCreated.get(i);
      GoogleCloudStorageItemInfo info = allInfo.get(i);

      assertThat(info.getResourceId()).isEqualTo(resourceId);
      assertThat(info.getSize()).isEqualTo(0);
      assertWithMessage("Item should exist").that(info.exists()).isTrue();
      assertThat(info.getCreationTime()).isNotEqualTo(0);
      assertThat(info.isBucket()).isFalse();
    }

    GoogleCloudStorageItemInfo nonExistentItemInfo = allInfo.get(allInfo.size() - 2);
    assertThat(nonExistentItemInfo.exists()).isFalse();

    GoogleCloudStorageItemInfo bucketInfo = allInfo.get(allInfo.size() - 1);
    assertThat(bucketInfo.exists()).isTrue();
    assertThat(bucketInfo.isBucket()).isTrue();
  }

  // TODO(user): Re-enable once a new method of inducing errors is devised.
  @Test @Ignore
  public void testGetMultipleItemInfoWithSomeInvalid() throws IOException {
    String bucketName = getSharedBucketName();

    List<StorageResourceId> resourceIdList = new ArrayList<>();
    StorageResourceId newObject =
        new StorageResourceId(bucketName, "testGetMultipleItemInfoWithSomeInvalid_CreatedObject");
    resourceIdList.add(newObject);
    rawStorage.createEmptyObject(newObject);

    StorageResourceId invalidObject =
        new StorageResourceId(bucketName, "testGetMultipleItemInfoWithSomeInvalid_InvalidObject\n");
    resourceIdList.add(invalidObject);

    try {
      rawStorage.getItemInfos(resourceIdList);
      fail();
    } catch (IOException e) {
      assertThat(e).hasMessageThat().isEqualTo("Error getting StorageObject");
    }
  }

  // TODO(user): Re-enable once a new method of inducing errors is devised.
  @Test @Ignore
  public void testOneInvalidGetItemInfo() throws IOException {
    String bucketName = getSharedBucketName();

    try {
      rawStorage.getItemInfo(
          new StorageResourceId(bucketName, "testOneInvalidGetItemInfo_InvalidObject\n"));
      fail();
    } catch (IOException e) {
      assertThat(e).hasMessageThat().isEqualTo("Error accessing");
    }
  }

  @Test
  public void testSingleObjectDelete() throws IOException {
    String bucketName = getSharedBucketName();

    StorageResourceId resource =
        new StorageResourceId(bucketName, "testSingleObjectDelete_SomeItem");
    rawStorage.createEmptyObject(resource);

    GoogleCloudStorageItemInfo info = rawStorage.getItemInfo(resource);
    assertThat(info.exists()).isTrue();

    rawStorage.deleteObjects(ImmutableList.of(resource));

    GoogleCloudStorageItemInfo deletedInfo = rawStorage.getItemInfo(resource);
    assertThat(deletedInfo.exists()).isFalse();
  }

  @Test
  public void testMultipleObjectDelete() throws IOException {
    String bucketName = getSharedBucketName();

    StorageResourceId resource =
        new StorageResourceId(bucketName, "testMultipleObjectDelete_MultiDeleteSomeItem1");
    rawStorage.createEmptyObject(resource);

    StorageResourceId secondResource =
        new StorageResourceId(bucketName, "testMultipleObjectDelete_MultiDeleteSecondItem");
    rawStorage.createEmptyObject(secondResource);

    assertThat(rawStorage.getItemInfo(resource).exists()).isTrue();
    assertThat(rawStorage.getItemInfo(secondResource).exists()).isTrue();

    rawStorage.deleteObjects(ImmutableList.of(resource, secondResource));

    assertThat(rawStorage.getItemInfo(resource).exists()).isFalse();
    assertThat(rawStorage.getItemInfo(secondResource).exists()).isFalse();
  }

  // TODO(user): Re-enable once a new method of inducing errors is devised.
  @Test @Ignore
  public void testSomeInvalidObjectsDelete() throws IOException {
    String bucketName = getSharedBucketName();

    StorageResourceId resource =
        new StorageResourceId(bucketName, "testSomeInvalidObjectsDelete_SomeItem");
    rawStorage.createEmptyObject(resource);

    // Don't actually create a GCS object for this resource.
    StorageResourceId secondResource =
        new StorageResourceId(bucketName, "testSomeInvalidObjectsDelete_DoesntExit");
    StorageResourceId invalidName =
        new StorageResourceId(bucketName, "testSomeInvalidObjectsDelete_InvalidObject\n");

    assertThat(rawStorage.getItemInfo(resource).exists()).isTrue();
    assertThat(rawStorage.getItemInfo(secondResource).exists()).isFalse();

    try {
      rawStorage.deleteObjects(ImmutableList.of(resource, secondResource, invalidName));
      fail();
    } catch (IOException e) {
      assertThat(e).hasMessageThat().isEqualTo("Error deleting");
    }
  }

  @Test
  public void testDeleteNonExistingObject() throws IOException {
    String bucketName = getSharedBucketName();

    StorageResourceId resource =
        new StorageResourceId(bucketName, "testDeleteNonExistingObject_DeleteNonExistantItemTest");

    rawStorage.deleteObjects(ImmutableList.of(resource));
  }

  @Test
  public void testDeleteNonExistingBucket() throws IOException {
    // Composite exception thrown, not a FileNotFoundException.
    String bucketName = getUniqueBucketName("delete_ne_bucket");

    try {
      rawStorage.deleteBuckets(ImmutableList.of(bucketName));
      fail();
    } catch (IOException e) {
      // expected
    }
  }

  @Test
  public void testSingleDeleteBucket() throws IOException {
    String bucketName = createUniqueBucket("delete_1_bucket");

    rawStorage.deleteBuckets(ImmutableList.of(bucketName));

    GoogleCloudStorageItemInfo info = rawStorage.getItemInfo(new StorageResourceId(bucketName));
    assertThat(info.exists()).isFalse();

    // Create the bucket again to assure that the previous one was deleted...
    rawStorage.create(bucketName);
  }

  @Test
  public void testMultipleDeleteBucket() throws IOException {
    String bucketName = createUniqueBucket("delete_multi_bucket");
    String bucketName2 = createUniqueBucket("delete_multi_bucket2");

    rawStorage.deleteBuckets(ImmutableList.of(bucketName, bucketName2));

    List<GoogleCloudStorageItemInfo> infoList = rawStorage.getItemInfos(ImmutableList.of(
        new StorageResourceId(bucketName), new StorageResourceId(bucketName2)));

    for (GoogleCloudStorageItemInfo info : infoList) {
      assertThat(info.exists()).isFalse();
    }
  }

  @Test
  public void testSomeInvalidDeleteBucket() throws IOException {
    String bucketName = createUniqueBucket("delete_multi_bucket");
    String bucketName2 = createUniqueBucket("delete_multi_bucket2");
    String invalidBucketName = "--invalid_delete_multi_bucket";

    try {
      rawStorage.deleteBuckets(ImmutableList.of(bucketName, bucketName2, invalidBucketName));
      // Expected exception would be a bit more awkward than Assert.fail() with a catch here...
      fail("Delete buckets with an invalid bucket should throw.");
    } catch (IOException ioe) {
      // Expected.
    }

    List<GoogleCloudStorageItemInfo> infoList = rawStorage.getItemInfos(ImmutableList.of(
        new StorageResourceId(bucketName), new StorageResourceId(bucketName2)));

    for (GoogleCloudStorageItemInfo info : infoList) {
      assertThat(info.exists()).isFalse();
    }
  }

  @Test
  public void testListBucketInfo() throws IOException {
    String bucketName = getSharedBucketName();

    // This has potential to become flaky...
    List<GoogleCloudStorageItemInfo> infoList = rawStorage.listBucketInfo();

    assertWithMessage("At least one bucket should exist").that(infoList).isNotEmpty();
    boolean bucketListed = false;
    for (GoogleCloudStorageItemInfo info : infoList) {
      assertThat(info.exists()).isTrue();
      assertThat(info.isBucket()).isTrue();
      assertThat(info.isRoot()).isFalse();
      bucketListed |= info.getBucketName().equals(bucketName);
    }
    assertThat(bucketListed).isTrue();
  }

  @Test
  public void testListBucketNames() throws IOException {
    String bucketName = getSharedBucketName();

    // This has potential to become flaky...
    List<String> bucketNames = rawStorage.listBucketNames();

    assertWithMessage("Bucket names should not be empty").that(bucketNames).isNotEmpty();
    assertThat(bucketNames).contains(bucketName);
  }

  @Test
  public void testListObjectNamesLimited() throws IOException {
    String bucketName = createUniqueBucket("list_object_names_limited");

    String[] names = {"a", "b", "c", "d"};
    for (String name : names) {
      StorageResourceId id = new StorageResourceId(bucketName, name);
      rawStorage.createEmptyObject(id);
    }

    List<String> gcsNames = rawStorage.listObjectNames(bucketName, null, "/", 2);

    assertThat(gcsNames).hasSize(2);
  }

  @Test
  public void testListObjectInfoLimited() throws IOException {
    String bucketName = createUniqueBucket("list_object_info_limited");

    String[] names = {"x", "y", "z"};
    for (String name : names) {
      StorageResourceId id = new StorageResourceId(bucketName, name);
      rawStorage.createEmptyObject(id);
    }

    List<GoogleCloudStorageItemInfo> info = rawStorage.listObjectInfo(bucketName, null, "/", 2);

    assertThat(info).hasSize(2);
  }

  @Test
  public void testListObjectInfoWithDirectoryRepair() throws IOException {
    String bucketName = createUniqueBucket("list_repair");

    StorageResourceId d1 = new StorageResourceId(bucketName, "d1/");
    rawStorage.createEmptyObject(d1);

    StorageResourceId o1 = new StorageResourceId(bucketName, "d1/o1");
    rawStorage.createEmptyObject(o1);

    // No empty d2/ prefix:
    StorageResourceId d3 = new StorageResourceId(bucketName, "d2/d3/");
    rawStorage.createEmptyObject(d3);

    StorageResourceId o2 = new StorageResourceId(bucketName, "d2/d3/o2");
    rawStorage.createEmptyObject(o2);

    GoogleCloudStorageItemInfo itemInfo =
        rawStorage.getItemInfo(new StorageResourceId(bucketName, "d2/"));
    assertThat(itemInfo.exists()).isFalse();

    List<GoogleCloudStorageItemInfo> rootInfo =
        rawStorage.listObjectInfo(bucketName, null, "/", GoogleCloudStorage.MAX_RESULTS_UNLIMITED);

    // Specifying any exact values seems like it's begging for this test to become flaky.
    assertWithMessage("Infos not expected to be empty").that(rootInfo.isEmpty()).isFalse();

    // Directory repair should have created an empty object for us:
    StorageResourceId d2 = new StorageResourceId(bucketName, "d2/");
    GoogleCloudStorageTestHelper.assertObjectContent(rawStorage, d2, new byte[0]);

    List<GoogleCloudStorageItemInfo> d2ItemInfo =
        rawStorage.listObjectInfo(
            bucketName, "d2/d3/", "/", GoogleCloudStorage.MAX_RESULTS_UNLIMITED);
    assertWithMessage("D2 item info not expected to be empty").that(d2ItemInfo.isEmpty()).isFalse();

    // Testing GCS treating object names as opaque blobs
    List<GoogleCloudStorageItemInfo> blobNamesInfo =
        rawStorage.listObjectInfo(bucketName, null, null, GoogleCloudStorage.MAX_RESULTS_UNLIMITED);
    assertWithMessage("blobNamesInfo not expected to be empty")
        .that(blobNamesInfo.isEmpty())
        .isFalse();
  }

  @Test
  public void testCopySingleItem() throws IOException {
    String bucketName = getSharedBucketName();

    byte[] bytesToWrite = new byte[4096];
    GoogleCloudStorageTestHelper.fillBytes(bytesToWrite);

    // Verify the bucket exist by creating an object
    StorageResourceId objectToCreate =
        new StorageResourceId(bucketName, "testCopySingleItem_CopySingleItemCopySourceObject");
    try (WritableByteChannel channel = rawStorage.create(objectToCreate)) {
      channel.write(ByteBuffer.wrap(bytesToWrite));
    }

    GoogleCloudStorageTestHelper.assertObjectContent(rawStorage, objectToCreate, bytesToWrite);

    StorageResourceId copiedResourceId =
        new StorageResourceId(bucketName, "testCopySingleItem_CopySingleItemDestinationObject");
    // Do the copy:
    rawStorage.copy(
        bucketName,
        ImmutableList.of(objectToCreate.getObjectName()),
        bucketName,
        ImmutableList.of(copiedResourceId.getObjectName()));

    GoogleCloudStorageTestHelper.assertObjectContent(rawStorage, copiedResourceId, bytesToWrite);
  }

  @Test
  public void testCopyToDifferentBucket() throws IOException {
    String sourceBucketName = createUniqueBucket("copy_src_bucket");
    String destinationBucketName = createUniqueBucket("copy_dst_bucket");

    byte[] bytesToWrite = new byte[4096];
    GoogleCloudStorageTestHelper.fillBytes(bytesToWrite);

    // Verify the bucket exist by creating an object
    StorageResourceId objectToCreate = new StorageResourceId(sourceBucketName, "CopyTestObject");
    try (WritableByteChannel channel = rawStorage.create(objectToCreate)) {
      channel.write(ByteBuffer.wrap(bytesToWrite));
    }

    GoogleCloudStorageTestHelper.assertObjectContent(rawStorage, objectToCreate, bytesToWrite);

    StorageResourceId copiedResourceId =
        new StorageResourceId(destinationBucketName, "CopiedObject");

    // Do the copy:
    rawStorage.copy(
        sourceBucketName,
        ImmutableList.of(objectToCreate.getObjectName()),
        destinationBucketName,
        ImmutableList.of(copiedResourceId.getObjectName()));

    GoogleCloudStorageTestHelper.assertObjectContent(rawStorage, copiedResourceId, bytesToWrite);
  }

  @Test
  public void testCopySingleItemOverExistingItem() throws IOException {
    String bucketName = getSharedBucketName();

    byte[] bytesToWrite = new byte[4096];
    GoogleCloudStorageTestHelper.fillBytes(bytesToWrite);

    StorageResourceId objectToCopy =
        new StorageResourceId(bucketName, "testCopySingleItemOverExistingItem_CopyTestObject");
    try (WritableByteChannel channel = rawStorage.create(objectToCopy)) {
      channel.write(ByteBuffer.wrap(bytesToWrite));
    }

    GoogleCloudStorageTestHelper.assertObjectContent(rawStorage, objectToCopy, bytesToWrite);

    StorageResourceId secondObject =
        new StorageResourceId(bucketName, "testCopySingleItemOverExistingItem_CopyTestObject2");
    byte[] secondObjectBytes = new byte[2046];
    GoogleCloudStorageTestHelper.fillBytes(secondObjectBytes);
    try (WritableByteChannel channel = rawStorage.create(secondObject)) {
      channel.write(ByteBuffer.wrap(secondObjectBytes));
    }

    GoogleCloudStorageTestHelper.assertObjectContent(rawStorage, secondObject, secondObjectBytes);

    rawStorage.copy(
        bucketName,
        ImmutableList.of(objectToCopy.getObjectName()),
        bucketName,
        ImmutableList.of(secondObject.getObjectName()));

    // Second object should now have the bytes of the first.
    GoogleCloudStorageTestHelper.assertObjectContent(rawStorage, secondObject, bytesToWrite);
  }

  @Test
  public void testCopySingleItemOverItself() throws IOException {
    String bucketName = createUniqueBucket("copy_item_1_1");

    byte[] bytesToWrite = new byte[4096];
    GoogleCloudStorageTestHelper.fillBytes(bytesToWrite);

    StorageResourceId objectToCopy = new StorageResourceId(bucketName, "CopyTestObject");
    try (WritableByteChannel channel = rawStorage.create(objectToCopy)) {
      channel.write(ByteBuffer.wrap(bytesToWrite));
    }

    GoogleCloudStorageTestHelper.assertObjectContent(rawStorage, objectToCopy, bytesToWrite);

    try {
      rawStorage.copy(
          bucketName,
          ImmutableList.of(objectToCopy.getObjectName()),
          bucketName,
          ImmutableList.of(objectToCopy.getObjectName()));
      fail();
    } catch (IllegalArgumentException e) {
      assertThat(e).hasMessageThat().startsWith("Copy destination must be different");
    }
  }

  static class CopyObjectData {
    public final StorageResourceId sourceResourceId;
    public final StorageResourceId destinationResourceId;
    public final byte[] objectBytes;

    CopyObjectData(
        StorageResourceId sourceResourceId,
        StorageResourceId destinationResourceId,
        byte[] objectBytes) {
      this.sourceResourceId = sourceResourceId;
      this.destinationResourceId = destinationResourceId;
      this.objectBytes = objectBytes;
    }
  }

  @Test
  public void testCopyMultipleItems() throws IOException {
    String bucketName = createUniqueBucket("copy_multi_item");

    final int copyObjectCount = 3;

    List<CopyObjectData> objectsToCopy = new ArrayList<>();
    for (int i = 0; i < copyObjectCount; i++) {
      String sourceObjectName = "sourceObject_" + i;
      String destinationObjectName = "destinationObject_" + i;
      byte[] objectBytes = new byte[1024 * i];
      GoogleCloudStorageTestHelper.fillBytes(objectBytes);

      try (WritableByteChannel channel =
          rawStorage.create(new StorageResourceId(bucketName, sourceObjectName))) {
        channel.write(ByteBuffer.wrap(objectBytes));
      }

      objectsToCopy.add(
          new CopyObjectData(
              new StorageResourceId(bucketName, sourceObjectName),
              new StorageResourceId(bucketName, destinationObjectName),
              objectBytes));
    }

    List<String> sourceObjects =
        Lists.transform(
            objectsToCopy,
            new Function<CopyObjectData, String>() {
              @Override
              public String apply(CopyObjectData copyObjectData) {
                return copyObjectData.sourceResourceId.getObjectName();
              }
            });

    List<String> destinationObjects =
        Lists.transform(
            objectsToCopy,
            new Function<CopyObjectData, String>() {
              @Override
              public String apply(CopyObjectData copyObjectData) {
                return copyObjectData.destinationResourceId.getObjectName();
              }
            });

    rawStorage.copy(bucketName, sourceObjects, bucketName, destinationObjects);

    for (CopyObjectData copyObjectData : objectsToCopy) {
      GoogleCloudStorageTestHelper.assertObjectContent(
          rawStorage, copyObjectData.sourceResourceId, copyObjectData.objectBytes);
      GoogleCloudStorageTestHelper.assertObjectContent(
          rawStorage, copyObjectData.destinationResourceId, copyObjectData.objectBytes);
    }

    List<StorageResourceId> objectsToCleanup = new ArrayList<>(copyObjectCount * 2);
    for (CopyObjectData copyObjectData : objectsToCopy) {
      objectsToCleanup.add(copyObjectData.sourceResourceId);
      objectsToCleanup.add(copyObjectData.destinationResourceId);
    }
  }

  @Test
  public void testCopyNonExistentItem() throws IOException {
    String bucketName = createUniqueBucket("copy_item_ne");
    String notExistentName = "IDontExist";

    try {
      rawStorage.copy(
          bucketName,
          ImmutableList.of(notExistentName),
          bucketName,
          ImmutableList.of("Some_destination"));
      fail();
    } catch (FileNotFoundException e) {
      // expected
    }
  }

  @Test
  public void testCopyMultipleItemsToSingleDestination() throws IOException {
    String bucketName = createUniqueBucket("copy_mutli_2_1");

    try {
      rawStorage.copy(
          bucketName,
          ImmutableList.of("SomeSource", "SomeSource2"),
          bucketName,
          ImmutableList.of("Some_destination"));
      fail("Copying multiple items to a single source should fail.");
    } catch (IllegalArgumentException e) {
      assertThat(e).hasMessageThat().startsWith("Must supply same number of elements");
    }
  }

  @Test
  public void testOpen() throws IOException {
    String bucketName = getSharedBucketName();

    byte[] bytesToWrite = new byte[100];

    StorageResourceId objectToCreate = new StorageResourceId(bucketName, "testOpen_OpenTestObject");
    try (WritableByteChannel channel = rawStorage.create(objectToCreate)) {
      channel.write(ByteBuffer.wrap(bytesToWrite));
    }

    GoogleCloudStorageTestHelper.assertObjectContent(rawStorage, objectToCreate, bytesToWrite);
  }

  @Test
  public void testOpenNonExistentItem() throws IOException {
    String bucketName = getSharedBucketName();

    try {
      rawStorage.open(new StorageResourceId(bucketName, "testOpenNonExistentItem_AnObject"));
      fail("Exception expected from opening an non-existent object");
    } catch (FileNotFoundException e) {
      // expected
    }
  }

  @Test
  public void testOpenEmptyObject() throws IOException {
    String bucketName = createUniqueBucket("open_empty");

    StorageResourceId resourceId = new StorageResourceId(bucketName, "EmptyObject");
    rawStorage.createEmptyObject(resourceId);
    GoogleCloudStorageTestHelper.assertObjectContent(rawStorage, resourceId, new byte[0]);
  }

  @Test
  public void testOpenLargeObject() throws IOException {
    String bucketName = getSharedBucketName();
    StorageResourceId objectToCreate =
        new StorageResourceId(bucketName, "testOpenLargeObject_LargeObject");
    GoogleCloudStorageTestHelper.readAndWriteLargeObject(objectToCreate, rawStorage);
  }

  @Test
  public void testPlusInObjectNames() throws IOException {
    String bucketName = getSharedBucketName();

    StorageResourceId resourceId =
        new StorageResourceId(bucketName, "testPlusInObjectNames_an+object");
    rawStorage.createEmptyObject(resourceId);
    GoogleCloudStorageTestHelper.assertObjectContent(rawStorage, resourceId, new byte[0]);
  }

  @Test
  public void testObjectPosition() throws IOException {
    final int totalBytes = 1200;
    byte[] data = new byte[totalBytes];
    GoogleCloudStorageTestHelper.fillBytes(data);

    String bucketName = getSharedBucketName();

    StorageResourceId resourceId =
        new StorageResourceId(bucketName, "testObjectPosition_SeekTestObject");
    try (WritableByteChannel channel = rawStorage.create(resourceId)) {
      channel.write(ByteBuffer.wrap(data));
    }

    byte[] readBackingArray = new byte[totalBytes];
    ByteBuffer readBuffer = ByteBuffer.wrap(readBackingArray);
    try (SeekableByteChannel readChannel = rawStorage.open(resourceId)) {
      assertWithMessage("Expected new file to open at position 0")
          .that(readChannel.position())
          .isEqualTo(0);
      assertWithMessage("Unexpected readChannel.size()")
          .that(readChannel.size())
          .isEqualTo(totalBytes);

      readBuffer.limit(4);
      int bytesRead = readChannel.read(readBuffer);
      assertWithMessage("Unexpected number of bytes read").that(bytesRead).isEqualTo(4);
      assertWithMessage("Unexpected position after read()")
          .that(readChannel.position())
          .isEqualTo(4);

      readChannel.position(4);
      assertWithMessage("Unexpected position after no-op")
          .that(readChannel.position())
          .isEqualTo(4);

      readChannel.position(6);
      assertWithMessage("Unexpected position after explicit position(6)")
          .that(readChannel.position())
          .isEqualTo(6);

      readChannel.position(data.length - 1);
      assertWithMessage("Unexpected position after seek to EOF - 1")
          .that(readChannel.position())
          .isEqualTo(data.length - 1);
      readBuffer.clear();
      bytesRead = readChannel.read(readBuffer);
      assertWithMessage("Expected to read 1 byte").that(bytesRead).isEqualTo(1);
      assertWithMessage("Unexpected data read for last byte")
          .that(readBackingArray[0])
          .isEqualTo(data[data.length - 1]);

      bytesRead = readChannel.read(readBuffer);
      assertWithMessage("Expected to read -1 bytes for EOF marker").that(bytesRead).isEqualTo(-1);

      readChannel.position(0);
      assertWithMessage("Unexpected position after reset to 0")
          .that(readChannel.position())
          .isEqualTo(0);

      try {
        readChannel.position(-1);
        fail("Expected IllegalArgumentException");
      } catch (EOFException eofe) {
        // Expected.
      }

      try {
        readChannel.position(totalBytes);
        fail("Expected IllegalArgumentException");
      } catch (EOFException eofe) {
        // Expected.
      }
    }
  }

  @Test
  public void testReadPartialObjects() throws IOException {
    final int segmentSize = 553;
    final int segmentCount = 5;
    byte[] data = new byte[segmentCount * segmentSize];
    GoogleCloudStorageTestHelper.fillBytes(data);

    String bucketName = getSharedBucketName();

    StorageResourceId resourceId =
        new StorageResourceId(bucketName, "testReadPartialObjects_ReadPartialTest");
    try (WritableByteChannel channel = rawStorage.create(resourceId)) {
      channel.write(ByteBuffer.wrap(data));
    }

    byte[][] readSegments = new byte[segmentCount][segmentSize];
    try (SeekableByteChannel readChannel = rawStorage.open(resourceId)) {
      for (int i = 0; i < segmentCount; i++) {
        ByteBuffer segmentBuffer = ByteBuffer.wrap(readSegments[i]);
        int bytesRead = readChannel.read(segmentBuffer);
        assertThat(bytesRead).isEqualTo(segmentSize);
        byte[] expectedSegment =
            Arrays.copyOfRange(
                data,
                i * segmentSize, /* from index */
                (i * segmentSize) + segmentSize /* to index */);
        assertWithMessage("Unexpected segment data read.")
            .that(readSegments[i])
            .isEqualTo(expectedSegment);
      }
    }
  }

  @Test
  public void testSpecialResourceIds() throws IOException {
    assertWithMessage("Unexpected ROOT item info returned")
        .that(rawStorage.getItemInfo(StorageResourceId.ROOT))
        .isEqualTo(GoogleCloudStorageItemInfo.ROOT_INFO);

    try {
      StorageResourceId.createReadableString(null, "objectName");
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void testChannelClosedException() throws IOException {
    final int totalBytes = 1200;
    byte[] data = new byte[totalBytes];
    GoogleCloudStorageTestHelper.fillBytes(data);
    String bucketName = getSharedBucketName();

    StorageResourceId resourceId =
        new StorageResourceId(bucketName, "testChannelClosedException_ReadClosedChannelTest");
    try (WritableByteChannel channel = rawStorage.create(resourceId)) {
      channel.write(ByteBuffer.wrap(data));
    }

    byte[] readArray = new byte[totalBytes];
    SeekableByteChannel readableByteChannel = rawStorage.open(resourceId);
    ByteBuffer readBuffer = ByteBuffer.wrap(readArray);
    readBuffer.limit(5);
    readableByteChannel.read(readBuffer);
    assertThat(readableByteChannel.position()).isEqualTo(readBuffer.position());

    readableByteChannel.close();
    readBuffer.clear();

    try {
      readableByteChannel.read(readBuffer);
      fail();
    } catch (ClosedChannelException e) {
      // expected
    }
  }

  @Test @Ignore("Not implemented")
  public void testOperationsAfterCloseFail() {

  }

  @Test
  public void testMetadataIsWrittenWhenCreatingObjects() throws IOException {
    String bucketName = getSharedBucketName();

    byte[] bytesToWrite = new byte[100];
    GoogleCloudStorageTestHelper.fillBytes(bytesToWrite);

    Map<String, byte[]> metadata =
        ImmutableMap.of(
            "key1", "value1".getBytes(StandardCharsets.UTF_8),
            "key2", "value2".getBytes(StandardCharsets.UTF_8));

    // Verify the bucket exist by creating an object
    StorageResourceId objectToCreate =
        new StorageResourceId(bucketName, "testUpdateItemInfoUpdatesMetadata");
    try (WritableByteChannel channel =
        rawStorage.create(objectToCreate, new CreateObjectOptions(false, metadata))) {
      channel.write(ByteBuffer.wrap(bytesToWrite));
    }

    // Verify metadata was set on create.
    GoogleCloudStorageItemInfo itemInfo = rawStorage.getItemInfo(objectToCreate);
    assertMapsEqual(metadata, itemInfo.getMetadata(), BYTE_ARRAY_EQUIVALENCE);
  }

  @Test
  public void testMetdataIsWrittenWhenCreatingEmptyObjects() throws IOException {
    String bucketName = getSharedBucketName();

    Map<String, byte[]> metadata =
        ImmutableMap.of(
            "key1", "value1".getBytes(StandardCharsets.UTF_8),
            "key2", "value2".getBytes(StandardCharsets.UTF_8));

    // Verify the bucket exist by creating an object
    StorageResourceId objectToCreate =
        new StorageResourceId(bucketName, "testMetdataIsWrittenWhenCreatingEmptyObjects");
    rawStorage.createEmptyObject(objectToCreate, new CreateObjectOptions(false, metadata));

    // Verify we get metadata from getItemInfo
    GoogleCloudStorageItemInfo itemInfo = rawStorage.getItemInfo(objectToCreate);
    assertMapsEqual(metadata, itemInfo.getMetadata(), BYTE_ARRAY_EQUIVALENCE);
  }

  @Test
  public void testUpdateItemInfoUpdatesMetadata() throws IOException {
    String bucketName = getSharedBucketName();

    byte[] bytesToWrite = new byte[100];
    GoogleCloudStorageTestHelper.fillBytes(bytesToWrite);

    Map<String, byte[]> metadata =
        ImmutableMap.of(
            "key1", "value1".getBytes(StandardCharsets.UTF_8),
            "key2", "value2".getBytes(StandardCharsets.UTF_8));

    // Verify the bucket exist by creating an object
    StorageResourceId objectToCreate =
        new StorageResourceId(bucketName, "testUpdateItemInfoUpdatesMetadata");
    try (WritableByteChannel channel = rawStorage.create(objectToCreate)) {
      channel.write(ByteBuffer.wrap(bytesToWrite));
    }

    GoogleCloudStorageItemInfo itemInfo = rawStorage.getItemInfo(objectToCreate);
    assertWithMessage("iniital metadata should be empty")
        .that(itemInfo.getMetadata().size())
        .isEqualTo(0);

    // Verify we can update metadata:
    List<GoogleCloudStorageItemInfo> results =
        rawStorage.updateItems(ImmutableList.of(new UpdatableItemInfo(objectToCreate, metadata)));

    assertThat(results).hasSize(1);
    assertMapsEqual(metadata, results.get(0).getMetadata(), BYTE_ARRAY_EQUIVALENCE);

    // Verify we get metadata from getItemInfo
    itemInfo = rawStorage.getItemInfo(objectToCreate);
    assertMapsEqual(metadata, itemInfo.getMetadata(), BYTE_ARRAY_EQUIVALENCE);

    // Delete key1 from metadata:
    Map<String, byte[]> deletionMap = new HashMap<>();
    deletionMap.put("key1", null);
    rawStorage.updateItems(ImmutableList.of(new UpdatableItemInfo(objectToCreate, deletionMap)));

    itemInfo = rawStorage.getItemInfo(objectToCreate);
    // Ensure that only key2:value2 still exists:
    assertMapsEqual(
        ImmutableMap.of("key2", "value2".getBytes(StandardCharsets.UTF_8)),
        itemInfo.getMetadata(),
        BYTE_ARRAY_EQUIVALENCE);
  }

  @Test
  public void testCompose() throws Exception {
    String bucketName = getSharedBucketName();

    StorageResourceId destinationObject =
        new StorageResourceId(bucketName, "testCompose_DestinationObject");

    // Create source objects
    String content1 = "Content 1";
    String content2 = "Content 2";
    StorageResourceId sourceObject1 =
        new StorageResourceId(bucketName, "testCompose_SourceObject1");
    StorageResourceId sourceObject2 =
        new StorageResourceId(bucketName, "testCompose_SourceObject2");
    try (WritableByteChannel channel = rawStorage.create(sourceObject1)) {
      channel.write(ByteBuffer.wrap(content1.getBytes(UTF_8)));
    }
    try (WritableByteChannel channel = rawStorage.create(sourceObject2)) {
      channel.write(ByteBuffer.wrap(content2.getBytes(UTF_8)));
    }
    GoogleCloudStorageTestHelper.assertObjectContent(
        rawStorage, sourceObject1, content1.getBytes(UTF_8));
    GoogleCloudStorageTestHelper.assertObjectContent(
        rawStorage, sourceObject2, content2.getBytes(UTF_8));

    // Do the compose
    rawStorage.compose(
        bucketName,
        ImmutableList.of("testCompose_SourceObject1", "testCompose_SourceObject2"),
        destinationObject.getObjectName(),
        CreateFileOptions.DEFAULT_CONTENT_TYPE);

    GoogleCloudStorageTestHelper.assertObjectContent(
        rawStorage, destinationObject, content1.concat(content2).getBytes(UTF_8));
  }

  @Test
  public void testObjectVerificationAttributes() throws IOException {
    String bucketName = getSharedBucketName();

    StorageResourceId testObject =
        new StorageResourceId(bucketName, "testObjectValidationAttributes");
    byte[] objectBytes = new byte[1024];
    GoogleCloudStorageTestHelper.fillBytes(objectBytes);
    HashCode originalMd5 = Hashing.md5().hashBytes(objectBytes);
    HashCode originalCrc32c = Hashing.crc32c().hashBytes(objectBytes);
    // Note that HashCode#asBytes returns a little-endian encoded array while
    // GCS uses big-endian. We avoid that by grabbing the int value of the CRC32c
    // and running it through Ints.toByteArray which encodes using big-endian.
    byte[] bigEndianCrc32c = Ints.toByteArray(originalCrc32c.asInt());

    // Don't use hashes in object creation, just validate the round trip. This of course
    // could lead to flaky looking tests due to bit flip errors.
    try (WritableByteChannel channel = rawStorage.create(testObject)) {
      channel.write(ByteBuffer.wrap(objectBytes));
    }

    GoogleCloudStorageItemInfo itemInfo = rawStorage.getItemInfo(testObject);

    GoogleCloudStorageTestHelper.assertByteArrayEquals(
        originalMd5.asBytes(), itemInfo.getVerificationAttributes().getMd5hash());
    // These string versions are slightly easier to debug (used when trying to
    // replicate GCS crc32c values in InMemoryGoogleCloudStorage).
    String originalCrc32cString = Integer.toHexString(Ints.fromByteArray(bigEndianCrc32c));
    String newCrc32cString =
        Integer.toHexString(Ints.fromByteArray(itemInfo.getVerificationAttributes().getCrc32c()));
    assertThat(newCrc32cString).isEqualTo(originalCrc32cString);
    GoogleCloudStorageTestHelper.assertByteArrayEquals(
        bigEndianCrc32c, itemInfo.getVerificationAttributes().getCrc32c());

    VerificationAttributes expectedAttributes =
        new VerificationAttributes(originalMd5.asBytes(), bigEndianCrc32c);

    assertThat(itemInfo.getVerificationAttributes()).isEqualTo(expectedAttributes);
  }

  static <K, V> void assertMapsEqual(
      Map<K, V> expected, Map<K, V> result, Equivalence<V> valueEquivalence) {
    MapDifference<K, V> difference = Maps.difference(expected, result, valueEquivalence);
    if (!difference.areEqual()) {
      StringBuilder builder = new StringBuilder();
      builder.append("Maps differ. ");
      builder.append("Entries differing: ").append(difference.entriesDiffering()).append("\n");
      builder.append("Missing entries: ").append(difference.entriesOnlyOnLeft()).append("\n");
      builder.append("Extra entries: ").append(difference.entriesOnlyOnRight()).append("\n");
      fail(builder.toString());
    }
  }
}
