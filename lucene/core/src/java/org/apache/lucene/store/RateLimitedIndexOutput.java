/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
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
package org.apache.lucene.store;

import java.io.IOException;

/**
 * A {@link RateLimiter rate limiting} {@link IndexOutput}
 *
 * @lucene.internal
 */// 只有在段合并时候才用到了，可以认为就是监控段合并速度。(在fdt, fdm和fdx中都使用了限流)
public final class RateLimitedIndexOutput extends FilterIndexOutput {

  private final RateLimiter rateLimiter;

  /** How many bytes we've written since we last called rateLimiter.pause. */
  private long bytesSinceLastPause;// 上次调用限速检查接口、到现在的写入字节，还在持续统计写入的字节数

  /**
   * Cached here not not always have to call RateLimiter#getMinPauseCheckBytes() which does volatile
   * read.
   */
  private long currentMinPauseCheckBytes; // 当写了多少数据后,才check是否写入超速了， 不超过1MB

  public RateLimitedIndexOutput(final RateLimiter rateLimiter, final IndexOutput out) {
    super("RateLimitedIndexOutput(" + out + ")", out.getName(), out);
    this.rateLimiter = rateLimiter;
    this.currentMinPauseCheckBytes = rateLimiter.getMinPauseCheckBytes();
  }

  @Override
  public void writeByte(byte b) throws IOException {
    bytesSinceLastPause++;
    checkRate();
    out.writeByte(b);
  }

  @Override
  public void writeBytes(byte[] b, int offset, int length) throws IOException {
    bytesSinceLastPause += length;
    checkRate();
    // The bytes array slice is written without pauses.
    // This can cause instant write rate to breach rate limit if there have
    // been no writes for enough time to keep the average write rate within limit.
    // See https://issues.apache.org/jira/browse/LUCENE-10448
    out.writeBytes(b, offset, length);
  }

  @Override
  public void writeInt(int i) throws IOException {
    bytesSinceLastPause += Integer.BYTES;
    checkRate();
    out.writeInt(i);
  }

  @Override
  public void writeShort(short i) throws IOException {
    bytesSinceLastPause += Short.BYTES;
    checkRate();
    out.writeShort(i);
  }

  @Override
  public void writeLong(long i) throws IOException {
    bytesSinceLastPause += Long.BYTES;
    checkRate();
    out.writeLong(i);
  }

  private void checkRate() throws IOException {// 检查是否需要merge中断
    if (bytesSinceLastPause > currentMinPauseCheckBytes) {
      rateLimiter.pause(bytesSinceLastPause);// 每次写多检查一次，是否需要暂停
      bytesSinceLastPause = 0;
      currentMinPauseCheckBytes = rateLimiter.getMinPauseCheckBytes();// 实时算出来的：当前限速下，写入速度*25ms的数据量就检查一次
    }
  }
}
