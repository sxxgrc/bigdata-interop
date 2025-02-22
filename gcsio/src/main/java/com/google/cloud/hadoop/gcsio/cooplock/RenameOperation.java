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

package com.google.cloud.hadoop.gcsio.cooplock;

import com.google.common.base.MoreObjects;
import java.util.Objects;

public class RenameOperation {
  private long lockEpochMilli;
  private String srcResource;
  private String dstResource;
  private boolean copySucceeded;

  public long getLockEpochMilli() {
    return lockEpochMilli;
  }

  public RenameOperation setLockEpochMilli(long lockEpochMilli) {
    this.lockEpochMilli = lockEpochMilli;
    return this;
  }

  public String getSrcResource() {
    return srcResource;
  }

  public RenameOperation setSrcResource(String srcResource) {
    this.srcResource = srcResource;
    return this;
  }

  public String getDstResource() {
    return dstResource;
  }

  public RenameOperation setDstResource(String dstResource) {
    this.dstResource = dstResource;
    return this;
  }

  public boolean getCopySucceeded() {
    return copySucceeded;
  }

  public RenameOperation setCopySucceeded(boolean copySucceeded) {
    this.copySucceeded = copySucceeded;
    return this;
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof RenameOperation && equalsInternal((RenameOperation) obj);
  }

  private boolean equalsInternal(RenameOperation other) {
    return Objects.equals(lockEpochMilli, other.lockEpochMilli)
        && Objects.equals(srcResource, other.srcResource)
        && Objects.equals(dstResource, other.dstResource)
        && Objects.equals(copySucceeded, other.copySucceeded);
  }

  @Override
  public int hashCode() {
    return Objects.hash(lockEpochMilli, srcResource, dstResource, copySucceeded);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("lockEpochMilli", lockEpochMilli)
        .add("srcResource", srcResource)
        .add("dstResource", dstResource)
        .add("copySucceeded", copySucceeded)
        .toString();
  }
}
