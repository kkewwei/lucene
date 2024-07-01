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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Executor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.index.MergePolicy.OneMerge;
import org.apache.lucene.internal.tests.TestSecrets;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RateLimitedIndexOutput;
import org.apache.lucene.store.RateLimiter;
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.ThreadInterruptedException;

/**
 * A {@link MergeScheduler} that runs each merge using a separate thread.
 *
 * <p>Specify the max number of threads that may run at once, and the maximum number of simultaneous
 * merges with {@link #setMaxMergesAndThreads}.
 *
 * <p>If the number of merges exceeds the max number of threads then the largest merges are paused
 * until one of the smaller merges completes.
 *
 * <p>If more than {@link #getMaxMergeCount} merges are requested then this class will forcefully
 * throttle the incoming threads by pausing until one more merges complete.
 *
 * <p>This class sets defaults based on Java's view of the cpu count, and it assumes a solid state
 * disk (or similar). If you have a spinning disk and want to maximize performance, use {@link
 * #setDefaultMaxMergesAndThreads(boolean)}.
 */
public class ConcurrentMergeScheduler extends MergeScheduler {

  /**
   * Dynamic default for {@code maxThreadCount} and {@code maxMergeCount}, based on CPU core count.
   * {@code maxThreadCount} is set to {@code max(1, min(4, cpuCoreCount/2))}. {@code maxMergeCount}
   * is set to {@code maxThreadCount + 5}.
   */
  public static final int AUTO_DETECT_MERGES_AND_THREADS = -1;

  /**
   * Used for testing.
   *
   * @lucene.internal
   */
  public static final String DEFAULT_CPU_CORE_COUNT_PROPERTY = "lucene.cms.override_core_count";

  /** List of currently active {@link MergeThread}s. */
  protected final List<MergeThread> mergeThreads = new ArrayList<>(); // 任何merge线程都会放进来
  // 大于4个的大段，绝对会停止，4个大段，会被限速（若新增和旧的有相似的，4个大段会被提速10%，若存量的4个大段有阻塞或者全部合并的段个数大于4，存量阻塞，速度不变；若都没阻塞，降速10%）
  // Max number of merge threads allowed to be running at
  // once.  When there are more merges then this, we
  // forcefully pause the larger ones, letting the smaller
  // ones run, up until maxMergeCount merges at which point
  // we forcefully pause incoming threads (that presumably
  // are the ones causing so much merging).  // Lucene层面，线上默认1。真正merge线程个数
  private int maxThreadCount = AUTO_DETECT_MERGES_AND_THREADS; //作用：1.在新大段合并开始前，会检查当前合并的总线程个数（包括小段），若新的没堵，旧的合并线程大于maxThreadCount，那么就说明旧的堵了，全局合并速度不变；若发现新的堵了，全局合并速度提升10%
  // 2.  新的合并跑起来了后，合并线程从大到小排序，接着检查当前大于50M的合并线程个数，大于maxThreadCount个数的次小线程会直接暂停合并。
  // Max number of merges we accept before forcefully
  // throttling the incoming threads // shard粒度统计个数，小于50MB的也包含在内，线上默认6，write线程也会进行merge,这个参数限制全聚在merged的总个数
  private int maxMergeCount = AUTO_DETECT_MERGES_AND_THREADS; // es层面，侧重于目前shard bulk写入能力上的限制.若一限制，写入都会暂停。仅仅限制写入，并不会限制merge继续产生（lucene会限制merge速度为0），不会做任何别的处理
  // 若merge_thread=1，只允许1个大于50mb的段合并，合并还限速，然后50mb以下的段合并不限速。然后es层面检测达到6个段合并（包含大小段），就开始禁止新的segment跑了
  /** How many {@link MergeThread}s have kicked off (this is use to name them). */
  protected int mergeThreadCounter;

  /** Floor for IO write rate limit (we will never go any lower than this) */
  private static final double MIN_MERGE_MB_PER_SEC = 5.0;

  /** Ceiling for IO write rate limit (we will never go any higher than this) */
  private static final double MAX_MERGE_MB_PER_SEC = 10240.0; // 10gb/s

  /** Initial value for IO write rate limit when doAutoIOThrottle is true */
  private static final double START_MB_PER_SEC = 20.0;

  /**
   * Merges below this size are not counted in the maxThreadCount, i.e. they can freely run in their
   * own thread (up until maxMergeCount).
   */
  private static final double MIN_BIG_MERGE_MB = 50.0;

  /** Current IO writes throttle rate */
  protected double targetMBPerSec = START_MB_PER_SEC; // 会在新增合并段时更新，在updateMergeThreads时会强制运用到所有正在跑的段里面

  /** true if we should rate-limit writes for each merge */
  private boolean doAutoIOThrottle = false;

  private double forceMergeMBPerSec = Double.POSITIVE_INFINITY;

  /** The executor provided for intra-merge parallelization */
  protected CachedExecutor intraMergeExecutor;

  /** Sole constructor, with all settings set to default values. */
  public ConcurrentMergeScheduler() {}

  /**
   * Expert: directly set the maximum number of merge threads and simultaneous merges allowed.
   *
   * @param maxMergeCount the max # simultaneous merges that are allowed. If a merge is necessary
   *     yet we already have this many threads running, the incoming thread (that is calling
   *     add/updateDocument) will block until a merge thread has completed. Note that we will only
   *     run the smallest <code>maxThreadCount</code> merges at a time.
   * @param maxThreadCount the max # simultaneous merge threads that should be running at once. This
   *     must be &lt;= <code>maxMergeCount</code>
   */
  public synchronized void setMaxMergesAndThreads(int maxMergeCount, int maxThreadCount) {
    if (maxMergeCount == AUTO_DETECT_MERGES_AND_THREADS
        && maxThreadCount == AUTO_DETECT_MERGES_AND_THREADS) {
      // OK
      this.maxMergeCount = AUTO_DETECT_MERGES_AND_THREADS;
      this.maxThreadCount = AUTO_DETECT_MERGES_AND_THREADS;
    } else if (maxMergeCount == AUTO_DETECT_MERGES_AND_THREADS) {
      throw new IllegalArgumentException(
          "both maxMergeCount and maxThreadCount must be AUTO_DETECT_MERGES_AND_THREADS");
    } else if (maxThreadCount == AUTO_DETECT_MERGES_AND_THREADS) {
      throw new IllegalArgumentException(
          "both maxMergeCount and maxThreadCount must be AUTO_DETECT_MERGES_AND_THREADS");
    } else {
      if (maxThreadCount < 1) {
        throw new IllegalArgumentException("maxThreadCount should be at least 1");
      }
      if (maxMergeCount < 1) {
        throw new IllegalArgumentException("maxMergeCount should be at least 1");
      }
      if (maxThreadCount > maxMergeCount) {
        throw new IllegalArgumentException(
            "maxThreadCount should be <= maxMergeCount (= " + maxMergeCount + ")");
      }
      this.maxThreadCount = maxThreadCount;
      this.maxMergeCount = maxMergeCount;
    }
  }

  /**
   * Sets max merges and threads to proper defaults for rotational or non-rotational storage.
   *
   * @param spins true to set defaults best for traditional rotatational storage (spinning disks),
   *     else false (e.g. for solid-state disks)
   */
  public synchronized void setDefaultMaxMergesAndThreads(boolean spins) {
    if (spins) {
      maxThreadCount = 1;
      maxMergeCount = 6;
    } else {
      int coreCount = Runtime.getRuntime().availableProcessors();

      // Let tests override this to help reproducing a failure on a machine that has a different
      // core count than the one where the test originally failed:
      try {
        String value = System.getProperty(DEFAULT_CPU_CORE_COUNT_PROPERTY);
        if (value != null) {
          coreCount = Integer.parseInt(value);
        }
      } catch (
          @SuppressWarnings("unused")
          Throwable ignored) {
      }

      // If you are indexing at full throttle, how many merge threads do you need to keep up? It
      // depends: for most data structures, merging is cheaper than indexing/flushing, but for knn
      // vectors, merges can require about as much work as the initial indexing/flushing. Plus
      // documents are indexed/flushed only once, but may be merged multiple times.
      // Here, we assume an intermediate scenario where merging requires about as much work as
      // indexing/flushing overall, so we give half the core count to merges.

      maxThreadCount = Math.max(1, coreCount / 2);
      maxMergeCount = maxThreadCount + 5;
    }
  }

  /**
   * Set the per-merge IO throttle rate for forced merges (default: {@code
   * Double.POSITIVE_INFINITY}).
   */
  public synchronized void setForceMergeMBPerSec(double v) {
    forceMergeMBPerSec = v;
    updateMergeThreads();
  }

  /** Get the per-merge IO throttle rate for forced merges. */
  public synchronized double getForceMergeMBPerSec() {
    return forceMergeMBPerSec;
  }

  /**
   * Turn on dynamic IO throttling, to adaptively rate limit writes bytes/sec to the minimal rate
   * necessary so merges do not fall behind. By default this is disabled and writes are not
   * rate-limited.
   */
  public synchronized void enableAutoIOThrottle() {
    doAutoIOThrottle = true;
    targetMBPerSec = START_MB_PER_SEC;
    updateMergeThreads();
  }

  /**
   * Turn off auto IO throttling.
   *
   * @see #enableAutoIOThrottle
   */
  public synchronized void disableAutoIOThrottle() {
    doAutoIOThrottle = false;
    updateMergeThreads();
  }

  /** Returns true if auto IO throttling is currently enabled. */
  public synchronized boolean getAutoIOThrottle() {
    return doAutoIOThrottle;
  }

  /**
   * Returns the currently set per-merge IO writes rate limit, if {@link #enableAutoIOThrottle} was
   * called, else {@code Double.POSITIVE_INFINITY}.
   */
  public synchronized double getIORateLimitMBPerSec() {
    if (doAutoIOThrottle) {
      return targetMBPerSec;
    } else {
      return Double.POSITIVE_INFINITY;
    }
  }

  /**
   * Returns {@code maxThreadCount}.
   *
   * @see #setMaxMergesAndThreads(int, int)
   */
  public synchronized int getMaxThreadCount() {
    return maxThreadCount;
  }

  /** See {@link #setMaxMergesAndThreads}. */
  public synchronized int getMaxMergeCount() {
    return maxMergeCount;
  }

  /** Removes the calling thread from the active merge threads. */
  synchronized void removeMergeThread() {
    Thread currentThread = Thread.currentThread();
    // Paranoia: don't trust Thread.equals:
    for (int i = 0; i < mergeThreads.size(); i++) {
      if (mergeThreads.get(i) == currentThread) {
        mergeThreads.remove(i);
        return;
      }
    }

    assert false : "merge thread " + currentThread + " was not found";
  }

  @Override
  public Executor getIntraMergeExecutor(OneMerge merge) {
    assert intraMergeExecutor != null : "scaledExecutor is not initialized";
    // don't do multithreaded merges for small merges
    if (merge.estimatedMergeBytes < MIN_BIG_MERGE_MB * 1024 * 1024) {
      return super.getIntraMergeExecutor(merge);
    }
    return intraMergeExecutor;
  }

  @Override
  public Directory wrapForMerge(OneMerge merge, Directory in) {
    Thread mergeThread = Thread.currentThread();
    if (!MergeThread.class.isInstance(mergeThread)) {
      throw new AssertionError(
          "wrapForMerge should be called from MergeThread. Current thread: " + mergeThread);
    }

    // Return a wrapped Directory which has rate-limited output.
    // Note: the rate limiter is only per thread. So, if there are multiple merge threads running
    // and throttling is required, each thread will be throttled independently.
    // The implication of this, is that the total IO rate could be higher than the target rate.
    RateLimiter rateLimiter = ((MergeThread) mergeThread).rateLimiter;
    return new FilterDirectory(in) {
      @Override
      public IndexOutput createOutput(String name, IOContext context) throws IOException {
        ensureOpen();

        // This Directory is only supposed to be used during merging,
        // so all writes should have MERGE context, else there is a bug
        // somewhere that is failing to pass down the right IOContext:
        assert context.context() == IOContext.Context.MERGE : "got context=" + context.context();

        return new RateLimitedIndexOutput(rateLimiter, in.createOutput(name, context));
      }
    };
  }

  /**
   * Called whenever the running merges have changed, to set merge IO limits. This method sorts the
   * merge threads by their merge size in descending order and then pauses/unpauses threads from
   * first to last -- that way, smaller merges are guaranteed to run before larger ones.
   *///新merge开始后/merge结束后，首先暂停超过maxThreadCount的大段，然后更新不多余maxThreadCount超过50mb的大段merge速度
  protected synchronized void updateMergeThreads() {

    // Only look at threads that are alive & not in the
    // process of stopping (ie have an active merge):
    final List<MergeThread> activeMerges = new ArrayList<>();

    int threadIdx = 0;
    while (threadIdx < mergeThreads.size()) {
      final MergeThread mergeThread = mergeThreads.get(threadIdx);
      if (!mergeThread.isAlive()) {
        // Prune any dead threads
        mergeThreads.remove(threadIdx);
        continue;
      }
      activeMerges.add(mergeThread);
      threadIdx++;
    } // 更新维持的merge线程情况，去掉已经完成的

    // Sort the merge threads, largest first:
    CollectionUtil.timSort(activeMerges);  // segment最大的放最前面

    final int activeMergeCount = activeMerges.size();

    int bigMergeCount = 0;

    for (threadIdx = activeMergeCount - 1; threadIdx >= 0; threadIdx--) {
      MergeThread mergeThread = activeMerges.get(threadIdx);
      if (mergeThread.merge.estimatedMergeBytes > MIN_BIG_MERGE_MB * 1024 * 1024) {
        bigMergeCount = 1 + threadIdx; //只是统计大于50的段合并线程个数
        break;
      }
    } // 找大于50M的正在合并的段个数

    long now = System.nanoTime();

    StringBuilder message;
    if (verbose()) {
      message = new StringBuilder();
      message.append(
          String.format(
              Locale.ROOT,
              "updateMergeThreads ioThrottle=%s targetMBPerSec=%.1f MB/sec",
              doAutoIOThrottle,
              targetMBPerSec));
    } else {
      message = null;
    }

    for (threadIdx = 0; threadIdx < activeMergeCount; threadIdx++) {// 遍历全部的段合并线程
      MergeThread mergeThread = activeMerges.get(threadIdx);

      OneMerge merge = mergeThread.merge;

      // pause the thread if maxThreadCount is smaller than the number of merge threads.
      final boolean doPause = threadIdx < bigMergeCount - maxThreadCount; // 若最大的超过限制个数的最大的那个几个，直接暂停速度。小于50M的不限速，别的调整规定速度。
      //线上只允许有一个超过50m的merge在进行，其余的全部暂停
      double newMBPerSec;
      if (doPause) { // 大于50MB的合并线程超了，那么直接将超的几个大的线程合并速度降为0
        newMBPerSec = 0.0;
      } else if (merge.maxNumSegments != -1) { // 是强制合并的速度
        newMBPerSec = forceMergeMBPerSec;
      } else if (doAutoIOThrottle == false) { // 限流还没开启(一般都会开启)
        newMBPerSec = Double.POSITIVE_INFINITY;
      } else if (merge.estimatedMergeBytes < MIN_BIG_MERGE_MB * 1024 * 1024) { // 若端的大小小于50M，则没有限制
        // Don't rate limit small merges:
        newMBPerSec = Double.POSITIVE_INFINITY;
      } else {// 没超过限制，超过50mb，就使用调整后的
        newMBPerSec = targetMBPerSec; // // 通过全局的段合并速度来更新本身（唯一一个超过50m）的段合并速度
      }

      MergeRateLimiter rateLimiter = mergeThread.rateLimiter;
      double curMBPerSec = rateLimiter.getMBPerSec();

      if (verbose()) {
        long mergeStartNS = merge.mergeStartNS;
        if (mergeStartNS == -1) {
          // IndexWriter didn't start the merge yet:
          mergeStartNS = now;
        }
        message.append('\n');
        message.append(
            String.format(
                Locale.ROOT,
                "merge thread %s estSize=%.1f MB (written=%.1f MB) runTime=%.1fs (stopped=%.1fs, paused=%.1fs) rate=%s\n",
                mergeThread.getName(),
                bytesToMB(merge.estimatedMergeBytes),
                bytesToMB(rateLimiter.getTotalBytesWritten()),
                nsToSec(now - mergeStartNS),
                nsToSec(rateLimiter.getTotalStoppedNS()),
                nsToSec(rateLimiter.getTotalPausedNS()),
                rateToString(rateLimiter.getMBPerSec())));

        if (newMBPerSec != curMBPerSec) {
          if (newMBPerSec == 0.0) {
            message.append("  now stop");
          } else if (curMBPerSec == 0.0) {
            if (newMBPerSec == Double.POSITIVE_INFINITY) {
              message.append("  now resume");
            } else {
              message.append(
                  String.format(Locale.ROOT, "  now resume to %.1f MB/sec", newMBPerSec));
            }
          } else {
            message.append(
                String.format(
                    Locale.ROOT,
                    "  now change from %.1f MB/sec to %.1f MB/sec",
                    curMBPerSec,
                    newMBPerSec));
          }
        } else if (curMBPerSec == 0.0) {
          message.append("  leave stopped");
        } else {
          message.append(String.format(Locale.ROOT, "  leave running at %.1f MB/sec", curMBPerSec));
        }
      }
      // 更新本身的段合并速度
      rateLimiter.setMBPerSec(newMBPerSec);
    }
    if (verbose()) {
      message(message.toString());
    }
  }

  private synchronized void initDynamicDefaults(Directory directory) throws IOException {
    if (maxThreadCount == AUTO_DETECT_MERGES_AND_THREADS) {// 默认不为-1
      setDefaultMaxMergesAndThreads(false);
      if (verbose()) {
        message(
            "initDynamicDefaults maxThreadCount="
                + maxThreadCount
                + " maxMergeCount="
                + maxMergeCount);
      }
    }
  }

  private static String rateToString(double mbPerSec) {
    if (mbPerSec == 0.0) {
      return "stopped";
    } else if (mbPerSec == Double.POSITIVE_INFINITY) {
      return "unlimited";
    } else {
      return String.format(Locale.ROOT, "%.1f MB/sec", mbPerSec);
    }
  }

  @Override
  public void close() throws IOException {
    super.close();
    try {
      sync();
    } finally {
      if (intraMergeExecutor != null) {
        intraMergeExecutor.shutdown();
      }
    }
  }

  /**
   * Wait for any running merge threads to finish. This call is not interruptible as used by {@link
   * #close()}.
   */
  public void sync() {
    boolean interrupted = false;
    try {
      while (true) {
        MergeThread toSync = null;
        synchronized (this) {
          for (MergeThread t : mergeThreads) {
            // In case a merge thread is calling us, don't try to sync on
            // itself, since that will never finish!
            if (t.isAlive() && t != Thread.currentThread()) {
              toSync = t;
              break;
            }
          }
        }
        if (toSync != null) {
          try {
            toSync.join();
          } catch (
              @SuppressWarnings("unused")
              InterruptedException ie) {
            // ignore this Exception, we will retry until all threads are dead
            interrupted = true;
          }
        } else {
          break;
        }
      }
    } finally {
      // finally, restore interrupt status:
      if (interrupted) Thread.currentThread().interrupt();
    }
  }

  /**
   * Returns the number of merge threads that are alive, ignoring the calling thread if it is a
   * merge thread. Note that this number is &le; {@link #mergeThreads} size.
   *
   * @lucene.internal
   */
  public synchronized int mergeThreadCount() {
    Thread currentThread = Thread.currentThread();
    int count = 0;
    for (MergeThread mergeThread : mergeThreads) {
      if (currentThread != mergeThread
          && mergeThread.isAlive()
          && mergeThread.merge.isAborted() == false) {
        count++;
      }
    }
    return count;
  }

  @Override
  void initialize(InfoStream infoStream, Directory directory) throws IOException {
    super.initialize(infoStream, directory);
    initDynamicDefaults(directory);
    if (intraMergeExecutor == null) {
      intraMergeExecutor = new CachedExecutor();
    }
  }
  //es主merge流程进来了，会单独分发多个merge线程
  @Override  // 可以从IndexWriter.maybeMerge()中跳转过来。也可以从MergeThread.runOnMergeFinished()中跳转过来
  public synchronized void merge(MergeSource mergeSource, MergeTrigger trigger) throws IOException {

    if (trigger == MergeTrigger.CLOSING) {
      // Disable throttling on close:
      targetMBPerSec = MAX_MERGE_MB_PER_SEC;
      updateMergeThreads();
    }

    // First, quickly run through the newly proposed merges
    // and add any orthogonal merges (ie a merge not
    // involving segments already pending to be merged) to
    // the queue.  If we are way behind on merging, many of
    // these newly proposed merges will likely already be
    // registered.

    if (verbose()) {
      message("now merge");
      message("  index(source): " + mergeSource.toString());
    }

    // Iterate, pulling from the IndexWriter's queue of
    // pending merges, until it's empty:
    while (true) {
      // es里面将跑到ElasticsearchConcurrentMergeScheduler.maybeStall(),返回true
      if (maybeStall(mergeSource) == false) {
        break;
      }

      OneMerge merge = mergeSource.getNextMerge(); // 将跑到IndexWriter$IndexWriterMergeSource
      if (merge == null) {
        if (verbose()) {
          message("  no more merges pending; now return");
        }
        return;
      }

      boolean success = false;
      try {
        // OK to spawn a new merge thread to handle this
        // merge:    // 无论多大的merge,都会另起线程合并
        final MergeThread newMergeThread = getMergeThread(mergeSource, merge);
        mergeThreads.add(newMergeThread);
        // 更新全局维护的段合并速度（看是否有合并慢的段）。同时初始化新创建的这个的merge速度
        updateIOThrottle(newMergeThread.merge, newMergeThread.rateLimiter);

        if (verbose()) {
          message("    launch new thread [" + newMergeThread.getName() + "]");
        }
        // 开始产生merge线程进行merge
        newMergeThread.start();
        updateMergeThreads(); // 更新全局维持的段合并速度，来更新存在的每个的段合并速度

        success = true;
      } finally {
        if (!success) {
          mergeSource.onMergeFinished(merge);
        }
      }
    }
  }

  /**
   * This is invoked by {@link #merge} to possibly stall the incoming thread when there are too many
   * merges running or pending. The default behavior is to force this thread, which is producing too
   * many segments for merging to keep up, to wait until merges catch up. Applications that can take
   * other less drastic measures, such as limiting how many threads are allowed to index, can do
   * nothing here and throttle elsewhere.
   *
   * <p>If this method wants to stall but the calling thread is a merge thread, it should return
   * false to tell caller not to kick off any new merges.
   */
  protected synchronized boolean maybeStall(MergeSource mergeSource) {
    long startStallTime = 0;
    while (mergeSource.hasPendingMerges() && mergeThreadCount() >= maxMergeCount) {

      // This means merging has fallen too far behind: we
      // have already created maxMergeCount threads, and
      // now there's at least one more merge pending.
      // Note that only maxThreadCount of
      // those created merge threads will actually be
      // running; the rest will be paused (see
      // updateMergeThreads).  We stall this producer
      // thread to prevent creation of new segments,
      // until merging has caught up:

      if (mergeThreads.contains(Thread.currentThread())) {// 若是从MergeThread.runOnMergeFinished()中跳转过来的，直接退出
        // Never stall a merge thread since this blocks the thread from
        // finishing and calling updateMergeThreads, and blocking it
        // accomplishes nothing anyway (it's not really a segment producer):
        return false;
      }

      if (startStallTime == 0) {
        startStallTime = System.currentTimeMillis();
        if (verbose()) {
          message("    too many merges; stalling...");
        }
      }
      doStall();
    }

    if (verbose() && startStallTime != 0) {
      message("  stalled for " + (System.currentTimeMillis() - startStallTime) + " ms");
    }

    return true;
  }

  /** Called from {@link #maybeStall} to pause the calling thread for a bit. */
  protected synchronized void doStall() {
    try {
      // Defensively wait for only .25 seconds in case we are missing a .notify/All somewhere:
      wait(250);
    } catch (InterruptedException ie) {
      throw new ThreadInterruptedException(ie);
    }
  }
// 单个Merge Thread线程run会优先跑到这里
  /**
   * Does the actual merge, by calling {@link
   * org.apache.lucene.index.MergeScheduler.MergeSource#merge}
   */
  protected void doMerge(MergeSource mergeSource, OneMerge merge) throws IOException {
    mergeSource.merge(merge); // 进的是ElasticsearchConcurrentMergeScheduler，第二次才进入IndexWriter
  }
 // 这个函数在es被复写了。
  /** Create and return a new MergeThread */
  protected synchronized MergeThread getMergeThread(MergeSource mergeSource, OneMerge merge)
      throws IOException {
    final MergeThread thread = new MergeThread(mergeSource, merge);// 默认速度就是Double.POSITIVE_INFINITY
    thread.setDaemon(true);
    thread.setName("Lucene Merge Thread #" + mergeThreadCounter++);
    return thread;
  }

  /**
   * Called when a merge thread finishes.
   *
   * @lucene.experimental
   */
  protected synchronized void runOnMergeFinished(
      MergeSource mergeSource, OneMerge merge, MergeRateLimiter rateLimiter) {
    // the merge call as well as the merge thread handling in the finally
    // block must be sync'd on CMS otherwise stalling decisions might cause
    // us to miss pending merges
    assert mergeThreads.contains(Thread.currentThread()) : "caller is not a merge thread";
    // Let CMS run new merges if necessary:
    try {
      merge(mergeSource, MergeTrigger.MERGE_FINISHED);// 循环检查是否还有merge需要进行。都是产生新的merge线程，自己先退出来
    } catch (
        @SuppressWarnings("unused")
        AlreadyClosedException ace) {
      // OK
    } catch (IOException ioe) {
      throw new UncheckedIOException(ioe);
    } finally {
      removeMergeThread();
      updateMergeThreads();
      // In case we had stalled indexing, we can now wake up
      // and possibly unstall:
      notifyAll();
    }
  }

  /** Runs a merge thread to execute a single merge, then exits. */
  protected class MergeThread extends Thread implements Comparable<MergeThread> {
    final MergeSource mergeSource;
    final OneMerge merge;
    final MergeRateLimiter rateLimiter;

    /** Sole constructor. */
    public MergeThread(MergeSource mergeSource, OneMerge merge) {
      this.mergeSource = mergeSource;
      this.merge = merge;
      this.rateLimiter = new MergeRateLimiter(merge.getMergeProgress()); // 每个semgent都会自带一个rateLimiter，并不是全局的
    }

    @Override
    public int compareTo(MergeThread other) {
      // Larger merges sort first:
      return Long.compare(other.merge.estimatedMergeBytes, merge.estimatedMergeBytes);
    }

    @Override
    public void run() {
      try {
        if (verbose()) {
          message(String.format(Locale.ROOT, "merge thread %s start", this.getName()));
        }
        // merge线程才会真正去调用beforeMerge，aftermerge
        doMerge(mergeSource, merge);
        if (verbose()) {
          message(
              String.format(
                  Locale.ROOT,
                  "merge thread %s merge segment [%s] done estSize=%.1f MB (written=%.1f MB) runTime=%.1fs (stopped=%.1fs, paused=%.1fs) rate=%s",
                  this.getName(),
                  getSegmentName(merge),
                  bytesToMB(merge.estimatedMergeBytes),
                  bytesToMB(rateLimiter.getTotalBytesWritten()),
                  nsToSec(System.nanoTime() - merge.mergeStartNS),
                  nsToSec(rateLimiter.getTotalStoppedNS()),
                  nsToSec(rateLimiter.getTotalPausedNS()),
                  rateToString(rateLimiter.getMBPerSec())));
        }

        runOnMergeFinished(mergeSource, merge, rateLimiter);

        if (verbose()) {
          message(String.format(Locale.ROOT, "merge thread %s end", this.getName()));
        }
      } catch (Throwable exc) {
        if (exc instanceof MergePolicy.MergeAbortedException) {
          // OK to ignore
        } else if (suppressExceptions == false) {
          // suppressExceptions is normally only set during
          // testing.
          handleMergeException(exc);
        }
      }
    }
  }

  /** Called when an exception is hit in a background merge thread */
  protected void handleMergeException(Throwable exc) {
    throw new MergePolicy.MergeException(exc);
  }

  private boolean suppressExceptions;

  /** Used for testing */
  void setSuppressExceptions() {
    if (verbose()) {
      message("will suppress merge exceptions");
    }
    suppressExceptions = true;
  }

  /** Used for testing */
  void clearSuppressExceptions() {
    if (verbose()) {
      message("will not suppress merge exceptions");
    }
    suppressExceptions = false;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName()
        + ": "
        + "maxThreadCount="
        + maxThreadCount
        + ", "
        + "maxMergeCount="
        + maxMergeCount
        + ", "
        + "ioThrottle="
        + doAutoIOThrottle;
  }
  // 新段小于50MB的话，在外边就直接退出了
  private boolean isBacklog(long now, OneMerge merge) {
    double mergeMB = bytesToMB(merge.estimatedMergeBytes);// 。去掉delete部分之后的sum(size)
    for (MergeThread mergeThread : mergeThreads) { // 遍历所有正在合并的段
      long mergeStartNS = mergeThread.merge.mergeStartNS;
      if (mergeThread.isAlive()
          && mergeThread.merge != merge// 检查所有正在合并的大于50M的段
          && mergeStartNS != -1
          && mergeThread.merge.estimatedMergeBytes >= MIN_BIG_MERGE_MB * 1024 * 1024// 大于50M的段个数
          && nsToSec(now - mergeStartNS) > 3.0) {// 比当前时间早3s合并的段
        double otherMergeMB = bytesToMB(mergeThread.merge.estimatedMergeBytes);
        double ratio = otherMergeMB / mergeMB;
        if (ratio > 0.3 && ratio < 3.0) { // 若大小相差范围在0.3-3之间，则返回true
          return true;
        }
      }
    }

    return false;
  }
  // 大于50M的合并的段个数来限速。根据当前正在进行的合并速度，修改全局保存的速度（只对新merge有效）。
  /** Tunes IO throttle when a new merge starts. */ // 只有在新产生一个Merge线程对象时，会去检查下
  private synchronized void updateIOThrottle(OneMerge newMerge, MergeRateLimiter rateLimiter)
      throws IOException {
    if (doAutoIOThrottle == false) { // 默认自动限速
      return;
    }

    double mergeMB = bytesToMB(newMerge.estimatedMergeBytes);// 待合并的merge大小
    if (mergeMB < MIN_BIG_MERGE_MB) { // 如果待合并的小于 50M的话，就没必要更新速度
      // Only watch non-trivial merges for throttling; this is safe because the MP must eventually
      // have to do larger merges:
      return;
    }

    long now = System.nanoTime();

    // Simplistic closed-loop feedback control: if we find any other similarly
    // sized merges running, then we are falling behind, so we bump up the
    // IO throttle, else we lower it:
    boolean newBacklog = isBacklog(now, newMerge); // 如果发现有相似的merge，则说明当前档位的merge落后了。否则当前级别合并时，之前的应该早就合并完了

    boolean curBacklog = false; // 检查已经的合并是否被阻塞了

    if (newBacklog == false) { // 新的没有相似的merge
      if (mergeThreads.size() > maxThreadCount) { // 若Mereg线程数（包含小于50mb的）合并个数超过maxThreadCount限制（1个）
        // If there are already more than the maximum merge threads allowed, count that as backlog:
        curBacklog = true; // 旧的阻塞了
      } else {
        // Now see if any still-running merges are backlog'd:
        for (MergeThread mergeThread : mergeThreads) {
          if (isBacklog(now, mergeThread.merge)) { // 检查旧的合并是否被阻塞了
            curBacklog = true;// 旧的阻塞了
            break;
          }
        }
      }
    }

    double curMBPerSec = targetMBPerSec; // 初始值为20M

    if (newBacklog) { // 新的是被阻塞了,那么整体提速20%
      // This new merge adds to the backlog: increase IO throttle by 20%
      targetMBPerSec *= 1.20; // 合并速度提速20%
      if (targetMBPerSec > MAX_MERGE_MB_PER_SEC) {
        targetMBPerSec = MAX_MERGE_MB_PER_SEC; // 5m-1024m之间变动
      }
      if (verbose()) {
        if (curMBPerSec == targetMBPerSec) {
          message(
              String.format(
                  Locale.ROOT,
                  "io throttle: new merge backlog; leave IO rate at ceiling %.1f MB/sec",
                  targetMBPerSec));
        } else {
          message(
              String.format(
                  Locale.ROOT,
                  "io throttle: new merge backlog; increase IO rate to %.1f MB/sec",
                  targetMBPerSec));
        }
      }
    } else if (curBacklog) { // 若存量的被阻塞了，则不变.
      // We still have an existing backlog; leave the rate as is:
      if (verbose()) {
        message(
            String.format(
                Locale.ROOT,
                "io throttle: current merge backlog; leave IO rate at %.1f MB/sec",
                targetMBPerSec));
      }
    } else { // 若新旧合并都没有问题
      // We are not falling behind: decrease IO throttle by 10%
      targetMBPerSec /= 1.10; // 那么降速10%，合并没阻塞的话，那么就以最低速度合并5MB/s
      if (targetMBPerSec < MIN_MERGE_MB_PER_SEC) {
        targetMBPerSec = MIN_MERGE_MB_PER_SEC;
      }
      if (verbose()) {
        if (curMBPerSec == targetMBPerSec) {
          message(
              String.format(
                  Locale.ROOT,
                  "io throttle: no merge backlog; leave IO rate at floor %.1f MB/sec",
                  targetMBPerSec));
        } else {
          message(
              String.format(
                  Locale.ROOT,
                  "io throttle: no merge backlog; decrease IO rate to %.1f MB/sec",
                  targetMBPerSec));
        }
      }
    }

    double rate;

    if (newMerge.maxNumSegments != -1) {
      rate = forceMergeMBPerSec;
    } else {
      rate = targetMBPerSec;
    }
    rateLimiter.setMBPerSec(rate);// 设置新的merge线程速度
    targetMBPerSecChanged(); // 不做任何事情
  }

  /** Subclass can override to tweak targetMBPerSec. */
  protected void targetMBPerSecChanged() {}

  private static double nsToSec(long ns) {
    return ns / (double) TimeUnit.SECONDS.toNanos(1);
  }

  private static double bytesToMB(long bytes) {
    return bytes / 1024. / 1024.;
  }

  private static String getSegmentName(MergePolicy.OneMerge merge) {
    return merge.info != null ? merge.info.info.name : "_na_";
  }

  static {
    TestSecrets.setConcurrentMergeSchedulerAccess(ConcurrentMergeScheduler::setSuppressExceptions);
  }

  /**
   * This executor provides intra-merge threads for parallel execution of merge tasks. It provides a
   * limited number of threads to execute merge tasks. In particular, if the number of
   * `mergeThreads` is equal to `maxThreadCount`, then the executor will execute the merge task in
   * the calling thread.
   */
  private class CachedExecutor implements Executor {

    private final AtomicInteger activeCount = new AtomicInteger(0);
    private final ThreadPoolExecutor executor;

    public CachedExecutor() {
      this.executor =
          new ThreadPoolExecutor(0, 1024, 1L, TimeUnit.MINUTES, new SynchronousQueue<>());
    }

    void shutdown() {
      executor.shutdown();
    }

    @Override
    public void execute(Runnable command) {
      final boolean isThreadAvailable;
      // we need to check if a thread is available before submitting the task to the executor
      // synchronize on CMS to get an accurate count of current threads
      synchronized (ConcurrentMergeScheduler.this) {
        int max = maxThreadCount - mergeThreads.size() - 1;
        int value = activeCount.get();
        if (value < max) {
          activeCount.incrementAndGet();
          assert activeCount.get() > 0 : "active count must be greater than 0 after increment";
          isThreadAvailable = true;
        } else {
          isThreadAvailable = false;
        }
      }
      if (isThreadAvailable) {
        executor.execute(
            () -> {
              try {
                command.run();
              } catch (Throwable exc) {
                if (suppressExceptions == false) {
                  // suppressExceptions is normally only set during
                  // testing.
                  handleMergeException(exc);
                }
              } finally {
                activeCount.decrementAndGet();
                assert activeCount.get() >= 0 : "unexpected negative active count";
              }
            });
      } else {
        command.run();
      }
    }
  }
}
