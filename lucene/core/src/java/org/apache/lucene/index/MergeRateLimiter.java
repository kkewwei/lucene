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
package org.apache.lucene.index;


import org.apache.lucene.store.RateLimiter;
import org.apache.lucene.util.ThreadInterruptedException;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.index.MergePolicy.OneMergeProgress;
import org.apache.lucene.index.MergePolicy.OneMergeProgress.PauseReason;

/** This is the {@link RateLimiter} that {@link IndexWriter} assigns to each running merge, to 
 *  give {@link MergeScheduler}s ionice like control.
 *
 *  @lucene.internal */

public class MergeRateLimiter extends RateLimiter {

  private final static int MIN_PAUSE_CHECK_MSEC = 25;
  
  private final static long MIN_PAUSE_NS = TimeUnit.MILLISECONDS.toNanos(2); // 2ms
  private final static long MAX_PAUSE_NS = TimeUnit.MILLISECONDS.toNanos(250); // 250ms

  private volatile double mbPerSec; // 每个线程初始化时不限速
  private volatile long minPauseCheckBytes;// 不超过1mb，也不超过当前写入速度的25ms,就检查一次。

  private long lastNS; // 上次检查符合规范的起始时间

  private AtomicLong totalBytesWritten = new AtomicLong(); // 这个文件这次打开，总共的写入byte

  private final OneMergeProgress mergeProgress;

  /** Sole constructor. */
  public MergeRateLimiter(OneMergeProgress mergeProgress) {
    // Initially no IO limit; use setter here so minPauseCheckBytes is set:
    this.mergeProgress = mergeProgress;
    setMBPerSec(Double.POSITIVE_INFINITY); // 默认不限速
  }
  // 可以从段合并那里跳转过来（从ConcurrentMergeScheduler.updateMergeThreads），或者段初始化时都会设置
  @Override // 别的线程可以调用这个merge的等待，唤醒这个merge
  public void setMBPerSec(double mbPerSec) {
    // Synchronized to make updates to mbPerSec and minPauseCheckBytes atomic. 
    synchronized (this) {
      // 0.0 is allowed: it means the merge is paused
      if (mbPerSec < 0.0) {
        throw new IllegalArgumentException("mbPerSec must be positive; got: " + mbPerSec);
      }
      this.mbPerSec = mbPerSec;
  
      // NOTE: Double.POSITIVE_INFINITY casts to Long.MAX_VALUE  按照当前写入速度的25ms,就检查一次。
      this.minPauseCheckBytes = Math.min(1024*1024, (long) ((MIN_PAUSE_CHECK_MSEC / 1000.0) * mbPerSec * 1024 * 1024));// 最大不超过1M
      assert minPauseCheckBytes >= 0;
    }

    mergeProgress.wakeup(); // 唤醒别人一次。
  }

  @Override
  public double getMBPerSec() {
    return mbPerSec;
  }

  /** Returns total bytes written by this merge. */
  public long getTotalBytesWritten() {
    return totalBytesWritten.get();
  }
  //
  @Override
  public long pause(long bytes) throws MergePolicy.MergeAbortedException {
    totalBytesWritten.addAndGet(bytes);

    // While loop because we may wake up and check again when our rate limit
    // is changed while we were pausing:
    long paused = 0;
    long delta;
    while ((delta = maybePause(bytes, System.nanoTime())) >= 0) { // merge暂停累加
      // Keep waiting.
      paused += delta;
    }

    return paused;
  }

  /** Total NS merge was stopped. */
  public long getTotalStoppedNS() {
    return mergeProgress.getPauseTimes().get(PauseReason.STOPPED);
  } 

  /** Total NS merge was paused to rate limit IO. */
  public long getTotalPausedNS() {
    return mergeProgress.getPauseTimes().get(PauseReason.PAUSED);
  } 

  /** 
   * Returns the number of nanoseconds spent in a paused state or <code>-1</code>
   * if no pause was applied. If the thread needs pausing, this method delegates 
   * to the linked {@link OneMergeProgress}.
   */  // merge可能会中断    这次检查前，bytes：写了多少数据量，curNS:当前时间
  private long maybePause(long bytes, long curNS) throws MergePolicy.MergeAbortedException {
    // Now is a good time to abort the merge:
    if (mergeProgress.isAborted()) {
      throw new MergePolicy.MergeAbortedException("Merge aborted.");
    }
   // 比较简单，就是看当前限速多少，周期性检查超过没，超过了就暂停多长时间。
    double rate = mbPerSec; // read from volatile rate once.
    double secondsToPause = (bytes/1024./1024.) / rate; // 写入这么多数据，按道理总共的耗时

    // Time we should sleep until; this is purely instantaneous
    // rate (just adds seconds onto the last time we had paused to);
    // maybe we should also offer decayed recent history one?
    long targetNS = lastNS + (long) (1000000000 * secondsToPause); // 理论写入这些byte后的时间

    long curPauseNS = targetNS - curNS; // 若>0,则说明写快了

    // We don't bother with thread pausing if the pause is smaller than 2 msec.
    if (curPauseNS <= MIN_PAUSE_NS) { // 若需要停止的时间小于2ms，那么没必要停止
      // Set to curNS, not targetNS, to enforce the instant rate, not
      // the "averaged over all history" rate:
      lastNS = curNS; //
      return -1;
    }

    // Defensive: don't sleep for too long; the loop above will call us again if
    // we should keep sleeping and the rate may be adjusted in between.
    if (curPauseNS > MAX_PAUSE_NS) { // 若停顿时间超过250ms。加入合并速度为0，那么最多停顿为MAX_PAUSE_NS，然后写一条，再进来停顿
      curPauseNS = MAX_PAUSE_NS; // 最多暂停250ms
    }

    long start = System.nanoTime();
    try {
      mergeProgress.pauseNanos( // 那么就暂停写入吧
          curPauseNS, 
          rate == 0.0 ? PauseReason.STOPPED : PauseReason.PAUSED,
          () -> rate == mbPerSec);// 除非限速值发生了改变，否则别人唤醒后再继续睡眠（每次setMBPerSec都会唤醒一次）
    } catch (InterruptedException ie) {
      throw new ThreadInterruptedException(ie);
    }
    return System.nanoTime() - start;
  }

  @Override
  public long getMinPauseCheckBytes() {
    return minPauseCheckBytes;
  }
}
