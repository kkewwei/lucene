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


import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.ThreadInterruptedException;

/**
 * This class controls {@link DocumentsWriterPerThread} flushing during
 * indexing. It tracks the memory consumption per
 * {@link DocumentsWriterPerThread} and uses a configured {@link FlushPolicy} to
 * decide if a {@link DocumentsWriterPerThread} must flush.
 * <p>
 * In addition to the {@link FlushPolicy} the flush control might set certain
 * {@link DocumentsWriterPerThread} as flush pending iff a
 * {@link DocumentsWriterPerThread} exceeds the
 * {@link IndexWriterConfig#getRAMPerThreadHardLimitMB()} to prevent address
 * space exhaustion.
 */ // DocumentsWriterFlushControl 类来控制flush策略，记录每一个DocumentsWriterPerThread内存消耗的量
final class DocumentsWriterFlushControl implements Accountable, Closeable {

  private final long hardMaxBytesPerDWPT;
  private long activeBytes = 0; // 所有构建lucene线程使用的内存统计，主要是拆分文本时构建索引结构占用的内存，不包含刷新时候的内存(修改numPending时，就会减少activeBytes)
  private volatile long flushBytes = 0; // 每写完一个文档，会判断文档/内存是否用超了，若用超，会设置为刷新，将activeBytes占用的内存统计全部转移到flushBytes中
  private volatile int numPending = 0; // 比如内存超了，就会立马设置该状态，但是还没有放入flushingWriters等待刷新,统计个数
  private int numDocsSinceStalled = 0; // only with assert
  private final AtomicBoolean flushDeletes = new AtomicBoolean(false);
  private boolean fullFlush = false; // 比如ES周期性refresh，会将fullFlush置位
  private boolean fullFlushMarkDone = false; // only for assertion that we don't get stale DWPTs from the pool，已经将需要refresh的标记完成了
  // The flushQueue is used to concurrently distribute DWPTs that are ready to be flushed ie. when a full flush is in
  // progress. This might be triggered by a commit or NRT refresh. The trigger will only walk all eligible DWPTs and
  // mark them as flushable putting them in the flushQueue ready for other threads (ie. indexing threads) to help flushing
  private final Queue<DocumentsWriterPerThread> flushQueue = new LinkedList<>();// 存放等待刷新的的DWPTs。等待排队被主动flush的DocumentsWriterPerThread。会依次从flushQueue出队列进行flush
  // only for safety reasons if a DWPT is close to the RAM limit // 正在进行full flush时，本DWPT也正在处于待刷新状态，那么本DWPT会先挂起，知道full flush完成后再继续
  private final Queue<DocumentsWriterPerThread> blockedFlushes = new LinkedList<>();
  // flushingWriters holds all currently flushing writers. There might be writers in this list that
  // are also in the flushQueue which means that writers in the flushingWriters list are not necessarily
  // already actively flushing. They are only in the state of flushing and might be picked up in the future by
  // polling the flushQueue // 从flushQueue取出进行flush, 仅当合并doAfterFlush()完成后才从flushingWriters去除。
  private final List<DocumentsWriterPerThread> flushingWriters = new ArrayList<>(); // flushQueue和flushingWriters内容基本一致，flushingWriters先放入，但是写入线程是从flushQueue中拿的待刷新的缓存文件。
  //只有 DWPT完成刷新后，才会从flushingWriters中去掉。还未完成flush的DocumentsWriterPerThread。
  private double maxConfiguredRamBuffer = 0;
  private long peakActiveBytes = 0;// only with assert
  private long peakFlushBytes = 0;// only with assert
  private long peakNetBytes = 0;// only with assert
  private long peakDelta = 0; // only with assert
  private boolean flushByRAMWasDisabled; // only with assert
  final DocumentsWriterStallControl stallControl = new DocumentsWriterStallControl();// 是否需要阻塞写入，当刷新线程的DocumentsWriterPerThread个数大于写入线程的DocumentsWriterPerThread个数，就需要阻塞写入了
  private final DocumentsWriterPerThreadPool perThreadPool; // 是DocumentsWriterPerThread循环使用池子
  private final FlushPolicy flushPolicy;
  private boolean closed = false;
  private final DocumentsWriter documentsWriter;
  private final LiveIndexWriterConfig config;
  private final InfoStream infoStream;

  DocumentsWriterFlushControl(DocumentsWriter documentsWriter, LiveIndexWriterConfig config) {
    this.infoStream = config.getInfoStream();
    this.perThreadPool = documentsWriter.perThreadPool;
    this.flushPolicy = config.getFlushPolicy();
    this.config = config;
    this.hardMaxBytesPerDWPT = config.getRAMPerThreadHardLimitMB() * 1024 * 1024; // 默认1045MB
    this.documentsWriter = documentsWriter;
  }

  public synchronized long activeBytes() {
    return activeBytes;
  }

  long getFlushingBytes() {
    return flushBytes;
  }

  synchronized long netBytes() {
    return flushBytes + activeBytes;
  }
  
  private long stallLimitBytes() { // 阻塞的阈值是indices.memory.index_buffer_size配置的两倍
    final double maxRamMB = config.getRAMBufferSizeMB();
    return maxRamMB != IndexWriterConfig.DISABLE_AUTO_FLUSH ? (long)(2 * (maxRamMB * 1024 * 1024)) : Long.MAX_VALUE;
  }
  //每次写入时候都会去检查
  private boolean assertMemory() {
    final double maxRamMB = config.getRAMBufferSizeMB(); //配置的
    // We can only assert if we have always been flushing by RAM usage; otherwise the assert will false trip if e.g. the
    // flush-by-doc-count * doc size was large enough to use far more RAM than the sudden change to IWC's maxRAMBufferSizeMB:
    if (maxRamMB != IndexWriterConfig.DISABLE_AUTO_FLUSH && flushByRAMWasDisabled == false) {
      // for this assert we must be tolerant to ram buffer changes!
      maxConfiguredRamBuffer = Math.max(maxRamMB, maxConfiguredRamBuffer);
      final long ram = flushBytes + activeBytes;
      final long ramBufferBytes = (long) (maxConfiguredRamBuffer * 1024 * 1024);
      // take peakDelta into account - worst case is that all flushing, pending and blocked DWPT had maxMem and the last doc had the peakDelta
      
      // 2 * ramBufferBytes -> before we stall we need to cross the 2xRAM Buffer border this is still a valid limit
      // (numPending + numFlushingDWPT() + numBlockedFlushes()) * peakDelta) -> those are the total number of DWPT that are not active but not yet fully flushed
      // all of them could theoretically be taken out of the loop once they crossed the RAM buffer and the last document was the peak delta
      // (numDocsSinceStalled * peakDelta) -> at any given time there could be n threads in flight that crossed the stall control before we reached the limit and each of them could hold a peak document
      final long expected = (2 * ramBufferBytes) + ((numPending + numFlushingDWPT() + numBlockedFlushes()) * peakDelta) + (numDocsSinceStalled * peakDelta);
      // the expected ram consumption is an upper bound at this point and not really the expected consumption
      if (peakDelta < (ramBufferBytes >> 1)) {
        /*
         * if we are indexing with very low maxRamBuffer like 0.1MB memory can
         * easily overflow if we check out some DWPT based on docCount and have
         * several DWPT in flight indexing large documents (compared to the ram
         * buffer). This means that those DWPT and their threads will not hit
         * the stall control before asserting the memory which would in turn
         * fail. To prevent this we only assert if the the largest document seen
         * is smaller than the 1/2 of the maxRamBufferMB
         */
        assert ram <= expected : "actual mem: " + ram + " byte, expected mem: " + expected
            + " byte, flush mem: " + flushBytes + ", active mem: " + activeBytes
            + ", pending DWPT: " + numPending + ", flushing DWPT: "
            + numFlushingDWPT() + ", blocked DWPT: " + numBlockedFlushes()
            + ", peakDelta mem: " + peakDelta + " bytes, ramBufferBytes=" + ramBufferBytes
            + ", maxConfiguredRamBuffer=" + maxConfiguredRamBuffer;
      }
    } else {
      flushByRAMWasDisabled = true;
    }
    return true;
  }
  // 单条数据建立完索引后，解析完索引后，会跑到这里。
  private synchronized void commitPerThreadBytes(DocumentsWriterPerThread perThread) {
    final long delta = perThread.commitLastBytesUsed(); //这是本次目前该线程使用的内存大小
    /*
     * We need to differentiate here if we are pending since setFlushPending
     * moves the perThread memory to the flushBytes and we could be set to
     * pending during a delete
     */
    if (perThread.isFlushPending()) {
      flushBytes += delta;
    } else {  // 一般都能把内存加到构建索引时候了
      activeBytes += delta; // 跑到这里
    }
    assert updatePeaks(delta); // 啥都不做
  }

  // only for asserts
  private boolean updatePeaks(long delta) {
    peakActiveBytes = Math.max(peakActiveBytes, activeBytes);
    peakFlushBytes = Math.max(peakFlushBytes, flushBytes);
    peakNetBytes = Math.max(peakNetBytes, netBytes());
    peakDelta = Math.max(peakDelta, delta);
    
    return true;
  }
  // 单条数据建立完lucene索引后，就会跑到这里。这里会检查是否因为设置的内存或者文档个数进行强制flush
  synchronized DocumentsWriterPerThread doAfterDocument(DocumentsWriterPerThread perThread, boolean isUpdate) {
    try {
      commitPerThreadBytes(perThread);// 里面可以看下byte处理
      if (!perThread.isFlushPending()) {// 不刷新
        if (isUpdate) {
          flushPolicy.onUpdate(this, perThread);
        } else { // 仅仅置位flushPending
          flushPolicy.onInsert(this, perThread); // 会跑到这里,看下lucene flush策略，可能flush出一个segment（一般不会触发，这里是有些问题的，一个shard写入内存使用不会超过）
        }
        if (!perThread.isFlushPending() && perThread.bytesUsed() > hardMaxBytesPerDWPT) { // 若还没有置为等待刷新flush（不会刷新），但是实际内存已经超过硬限制1094M的话，也会置为刷新
          // Safety check to prevent a single DWPT exceeding its RAM limit. This
          // is super important since we can not address more than 2048 MB per DWPT
          setFlushPending(perThread);
        }
      }
      return checkout(perThread, false); // 写完也会检查是否需要帮忙
    } finally {
      boolean stalled = updateStallState(); // 检查内存使用
      assert assertNumDocsSinceStalled(stalled) && assertMemory();
    }
  }
  // check出这个perThread（一般是文档数满了），和fullFlush的checkoutForFlush（fullFlush直接跑的是这里）还是不一样的
  private DocumentsWriterPerThread checkout(DocumentsWriterPerThread perThread, boolean markPending) {
    assert Thread.holdsLock(this);
    if (fullFlush) { // 比如用户主动调用IndexWriter.flush时候，或者周期性refresh时，就会置该位
      if (perThread.isFlushPending()) {// 已经被置位
        checkoutAndBlock(perThread); // 先阻塞下，不刷新这个DWPT
        return nextPendingFlush(); // 从flushQueue中取一个
      }
    } else {
      if (markPending) { // 是否已经mark为pending
        assert perThread.isFlushPending() == false;
        setFlushPending(perThread);
      }

      if (perThread.isFlushPending()) { // 被置为等待刷新（比如内存超了）
        return checkOutForFlush(perThread);// 会从dwpts中去掉
      }
    }
    return null;
  }
  
  private boolean assertNumDocsSinceStalled(boolean stalled) {
    /*
     *  updates the number of documents "finished" while we are in a stalled state.
     *  this is important for asserting memory upper bounds since it corresponds 
     *  to the number of threads that are in-flight and crossed the stall control // 为了应对横在进行的，但是已经失控的检查，
     *  check before we actually stalled.
     *  see #assertMemory()
     */
    if (stalled) { 
      numDocsSinceStalled++; // 被阻塞的文档树
    } else {
      numDocsSinceStalled = 0;
    }
    return true;
  }
 // 会从DocumentsWriter.doFlush()中跑过来，此时segment已经完成了刷新
  synchronized void doAfterFlush(DocumentsWriterPerThread dwpt) {
    assert flushingWriters.contains(dwpt);
    try {
      flushingWriters.remove(dwpt);
      flushBytes -= dwpt.getLastCommittedBytesUsed();
      assert assertMemory();// 检查内存使用情况
    } finally {
      try {
        updateStallState();
      } finally {
        notifyAll();
      }
    }
  }

  private long stallStartNS;
 // 更新阻塞状态
  private boolean updateStallState() {
    
    assert Thread.holdsLock(this);
    final long limit = stallLimitBytes(); // 冂
    /*
     * we block indexing threads if net byte grows due to slow flushes
     * yet, for small ram buffers and large documents we can easily
     * reach the limit without any ongoing flushes. we need to ensure
     * that we don't stall/block if an ongoing or pending flush can
     * not free up enough memory to release the stall lock.
     */ // 我们block写入线程，保证即将的刷新不会释放足够的内存来解锁(内存超的时候，我们才需需阻塞下)
    final boolean stall = (activeBytes + flushBytes) > limit &&
      activeBytes < limit &&
      !closed;

    if (infoStream.isEnabled("DWFC")) {
      if (stall != stallControl.anyStalledThreads()) { // 是否有任何被阻塞的线程
        if (stall) { // 若被阻塞
          infoStream.message("DW", String.format(Locale.ROOT, "now stalling flushes: netBytes: %.1f MB flushBytes: %.1f MB fullFlush: %b",
                                                 netBytes()/1024./1024., getFlushingBytes()/1024./1024., fullFlush));
          stallStartNS = System.nanoTime();
        } else {
          infoStream.message("DW", String.format(Locale.ROOT, "done stalling flushes for %.1f msec: netBytes: %.1f MB flushBytes: %.1f MB fullFlush: %b",
                                                 (System.nanoTime()-stallStartNS)/1000000., netBytes()/1024./1024., getFlushingBytes()/1024./1024., fullFlush));
        }
      }
    }

    stallControl.updateStalled(stall); // 若内存使用没超，会唤醒阻塞的线程
    return stall;
  }
  
  public synchronized void waitForFlush() {
    while (flushingWriters.size() != 0) {
      try {
        this.wait();
      } catch (InterruptedException e) {
        throw new ThreadInterruptedException(e);
      }
    }
  }

  /**
   * Sets flush pending state on the given {@link DocumentsWriterPerThread}. The
   * {@link DocumentsWriterPerThread} must have indexed at least on Document and must not be
   * already pending.
   */ // 比如es 索引周期性refresh的话，则将当前DocumentsWriterPerThread放入flushPending中
  public synchronized void setFlushPending(DocumentsWriterPerThread perThread) {
    assert !perThread.isFlushPending();
    if (perThread.getNumDocsInRAM() > 0) {
      perThread.setFlushPending(); // write access synced       flush置位
      final long bytes = perThread.getLastCommittedBytesUsed();
      flushBytes += bytes;
      activeBytes -= bytes;
      numPending++; // write access synced
      assert assertMemory();
    } // don't assert on numDocs since we could hit an abort excp. while selecting that dwpt for flushing
    
  }
  
  synchronized void doOnAbort(DocumentsWriterPerThread perThread) {
    try {
      assert perThreadPool.isRegistered(perThread);
      assert perThread.isHeldByCurrentThread();
      if (perThread.isFlushPending()) {
        flushBytes -= perThread.getLastCommittedBytesUsed();
      } else {
        activeBytes -= perThread.getLastCommittedBytesUsed();
      }
      assert assertMemory();
      // Take it out of the loop this DWPT is stale
    } finally {
      updateStallState();
      boolean checkedOut = perThreadPool.checkout(perThread);
      assert checkedOut;
    }
  }
  // 只有fullFlush被置位，恰好该线程又准备flush，才会被阻塞
  private void checkoutAndBlock(DocumentsWriterPerThread perThread) {
    assert perThreadPool.isRegistered(perThread);
    assert perThread.isHeldByCurrentThread();
    assert perThread.isFlushPending() : "can not block non-pending threadstate";
    assert fullFlush : "can not block if fullFlush == false";
    numPending--;
    blockedFlushes.add(perThread);
    boolean checkedOut = perThreadPool.checkout(perThread);
    assert checkedOut;
  }
  // 写完后是否需要刷新，将当前DocumentsWriterPerThread从DocumentsWriterPerThreadPool.dwpts中checkut出。
  private synchronized DocumentsWriterPerThread checkOutForFlush(DocumentsWriterPerThread perThread) {
    assert Thread.holdsLock(this);
    assert perThread.isFlushPending(); // 调用该函数前，已经保证perThread.isFlushPending()
    assert perThread.isHeldByCurrentThread();
    assert perThreadPool.isRegistered(perThread);
    try {
        addFlushingDWPT(perThread); // 放入到flushingWriters
        numPending--; // write access synced
        boolean checkedOut = perThreadPool.checkout(perThread); // 从DocumentsWriterPerThreadPool.dwpts和freeList中去掉
        assert checkedOut; // 一定可以checkout
        return perThread;
    } finally {
      updateStallState(); // 目前flushBytes和activeBytes已经发生改变
    }
  }

  private void addFlushingDWPT(DocumentsWriterPerThread perThread) {
    assert flushingWriters.contains(perThread) == false : "DWPT is already flushing";
    // Record the flushing DWPT to reduce flushBytes in doAfterFlush
    flushingWriters.add(perThread);
  }

  @Override
  public String toString() {
    return "DocumentsWriterFlushControl [activeBytes=" + activeBytes
        + ", flushBytes=" + flushBytes + "]";
  }
  // 获取下一个等待刷新的的任务：首先从现成的flushQueue中，不行的话从perThreadPool中等待flush的
  DocumentsWriterPerThread nextPendingFlush() {
    int numPending;
    boolean fullFlush;
    synchronized (this) {
      final DocumentsWriterPerThread poll;
      if ((poll = flushQueue.poll()) != null) { //首先从flush队列中找到一个
        updateStallState();
        return poll;
      }
      fullFlush = this.fullFlush;
      numPending = this.numPending;
    } // 没找到存量刷新的话，并且已经有numPending（比如单个DWTP的内存满了）
    if (numPending > 0 && fullFlush == false) { // don't check if we are doing a full flush
      for (final DocumentsWriterPerThread next : perThreadPool) { // 然后从perThreadPool找等待刷新的
        if (next.isFlushPending()) { // 等待被刷新的
          if (next.tryLock()) {
            try {
              if (perThreadPool.isRegistered(next)) {// 还在，双层确认
                return checkOutForFlush(next);
              }
            } finally {
              next.unlock();
            }
          }
        }
      }
    }
    return null;
  }

  @Override
  public synchronized void close() {
    // set by DW to signal that we are closing. in this case we try to not stall any threads anymore etc.
    closed = true;
  }

  /**
   * Returns an iterator that provides access to all currently active {@link DocumentsWriterPerThread}s
   */
  public Iterator<DocumentsWriterPerThread> allActiveWriters() {
    return perThreadPool.iterator();
  }

  synchronized void doOnDelete() {
    // pass null this is a global delete no update
    flushPolicy.onDelete(this, null);
  }

  /** Returns heap bytes currently consumed by buffered deletes/updates that would be
   *  freed if we pushed all deletes.  This does not include bytes consumed by
   *  already pushed delete/update packets. */
  public long getDeleteBytesUsed() {
    return documentsWriter.deleteQueue.ramBytesUsed();
  }

  @Override
  public long ramBytesUsed() {
    // TODO: improve this to return more detailed info?
    return getDeleteBytesUsed() + netBytes();
  }
  
  synchronized int numFlushingDWPT() {
    return flushingWriters.size();
  }
  
  public boolean getAndResetApplyAllDeletes() {
    return flushDeletes.getAndSet(false);
  }

  public void setApplyAllDeletes() {
    flushDeletes.set(true);
  }
    // 在进行全量刷新时，需要将这个参数设置为
  DocumentsWriterPerThread obtainAndLock() throws IOException {
    while (closed == false) { // 循环获取，校验不过再重新获取
      final DocumentsWriterPerThread perThread = perThreadPool.getAndLock(); // 简单封装了下ReentrantLock
      if (perThread.deleteQueue == documentsWriter.deleteQueue) { // 尽管处于fullFlush（详见）阶段，也是可以直接返回的。因为已经更新过了。
        // simply return the DWPT even in a flush all case since we already hold the lock and the DWPT is not stale
        // since it has the current delete queue associated with it. This means we have established a happens-before
        // relationship and all docs indexed into this DWPT are guaranteed to not be flushed with the currently
        // progress full flush. //
        return perThread;
      } else { // 循环重试，说明正处于fullFlush阶段，
        try {
          // we must first assert otherwise the full flush might make progress once we unlock the dwpt
          assert fullFlush && fullFlushMarkDone == false :
              "found a stale DWPT but full flush mark phase is already done fullFlush: "
                  + fullFlush  + " markDone: " + fullFlushMarkDone;
        } finally {
          perThread.unlock(); // 释放这个吧
          // There is a flush-all in process and this DWPT is
          // now stale - try another one
        }
      }
    }
    throw new AlreadyClosedException("flush control is closed");
  }
   // 比如主动调用lucen commit操作（外边已经保证了，只能一个线程进来）
  long markForFullFlush() {
    final DocumentsWriterDeleteQueue flushingQueue;
    long seqNo;
    synchronized (this) { // 这里是一次锁
      assert fullFlush == false: "called DWFC#markForFullFlush() while full flush is still running";
      assert fullFlushMarkDone == false : "full flush collection marker is still set to true";
      fullFlush = true; // 标志所有线程都进入flush阶段
      flushingQueue = documentsWriter.deleteQueue; // 旧的deleteQueue
      // Set a new delete queue - all subsequent DWPT will use this queue until
      // we do another full flush // 别的线程将不能再继续写入，因为分配不到ThreadStates了。因为要产生
      perThreadPool.lockNewWriters(); // no new thread-states while we do a flush otherwise the seqNo accounting might be off
      try {
        // Insert a gap in seqNo of current active thread count, in the worst case each of those threads now have one operation in flight.  It's fine
        // if we have some sequence numbers that were never assigned:
        DocumentsWriterDeleteQueue newQueue = documentsWriter.deleteQueue.advanceQueue(perThreadPool.size()); // 产生新的deleteQueue
        seqNo = documentsWriter.deleteQueue.getMaxSeqNo();
        documentsWriter.resetDeleteQueue(newQueue); // 重置queue
      } finally {
        perThreadPool.unlockNewWriters(); // 和lockNewThreadStates必须一起用
      }
    }
    final List<DocumentsWriterPerThread> fullFlushBuffer = new ArrayList<>();
    for (final DocumentsWriterPerThread next : perThreadPool.filterAndLock(dwpt -> dwpt.deleteQueue == flushingQueue)) { // 获取当前正在写入的doc,
        try { // 这有多个的原因：比如上个的flushAllThreads产生的DocumentsWriterPerThread仍然还放在flushQueue中，但是没有写入了。或者还没来得及有线程处理。（正常情况下，每个shard只能有一个正在写入的写入buffer）
          assert next.deleteQueue == flushingQueue
              || next.deleteQueue == documentsWriter.deleteQueue : " flushingQueue: "
              + flushingQueue
              + " currentqueue: "
              + documentsWriter.deleteQueue
              + " perThread queue: "
              + next.deleteQueue
              + " numDocsInRam: " + next.getNumDocsInRAM();

          if (next.getNumDocsInRAM() > 0) { // 有文档的话。当前内存中只会有一个正在写的缓存带刷新的segment
            final DocumentsWriterPerThread flushingDWPT;
            synchronized(this) {
              if (next.isFlushPending() == false) { // 若没有处于flushPending状态，则设置处于
                setFlushPending(next); // 就将该DocumentsWriterPerThread放入DocumentsWriterPerThread.flushPending
              }// 一定会在DocumentsWriterPerThreadPool.dwpts存在。因next.isFlushPending()为true的，已经脱离该对象的管理
              flushingDWPT = checkOutForFlush(next); // ongoing， 一定可以checkout出，放入到DocumentsWriterFlushControl.flushingWriters中。后面会再放入到flushQueue中
            }
            assert flushingDWPT != null : "DWPT must never be null here since we hold the lock and it holds documents";
            assert next == flushingDWPT : "flushControl returned different DWPT";
            fullFlushBuffer.add(flushingDWPT);
          } else {// 没有文档，因为我们再换每个DWPT里面的deleteQueue，旧的DWPT已经过期了，简单处理的话，就仅仅丢弃彻底丢弃DocumentsWriterPerThread
            // it's possible that we get a DWPT with 0 docs if we flush concurrently to
            // threads getting DWPTs from the pool. In this case we simply remove it from
            // the pool and drop it on the floor.
            boolean checkout = perThreadPool.checkout(next);
            assert checkout;
          }
        } finally {
          next.unlock(); // 从dwpts中取出后，也会释放了锁。
        }
    }
    synchronized (this) {
      /* make sure we move all DWPT that are where concurrently marked as
       * pending and moved to blocked are moved over to the flushQueue. There is
       * a chance that this happens since we marking DWPT for full flush without
       * blocking indexing.*/
      pruneBlockedQueue(flushingQueue); // 会将旧DocumentsWriterDeleteQueue的里面blockedFlushes待flush的DocumentsWriterPerThread全部移到flushQueue中
      assert assertBlockedFlushes(documentsWriter.deleteQueue);
      flushQueue.addAll(fullFlushBuffer); // 全部checkout出来，然后再将这批DWPT放在flushQueue中（为啥blockedFlushes先放进来的原因：这批是因为文档数满了|内存占用满了导致的flush，里面包含的DWPT都是大家伙）
      updateStallState();
      fullFlushMarkDone = true; // at this point we must have collected all DWPTs that belong to the old delete queue
    }
    assert assertActiveDeleteQueue(documentsWriter.deleteQueue);
    assert flushingQueue.getLastSequenceNumber() <= flushingQueue.getMaxSeqNo();
    return seqNo;
  }
  
  private boolean assertActiveDeleteQueue(DocumentsWriterDeleteQueue queue) {
    for (final DocumentsWriterPerThread next : perThreadPool) {
        assert next.deleteQueue == queue : "numDocs: " + next.getNumDocsInRAM();
    }
    return true;
  }

  /**
   * Prunes the blockedQueue by removing all DWPTs that are associated with the given flush queue.
   */
  private void pruneBlockedQueue(final DocumentsWriterDeleteQueue flushingQueue) {
    assert Thread.holdsLock(this);
    Iterator<DocumentsWriterPerThread> iterator = blockedFlushes.iterator();
    while (iterator.hasNext()) {
      DocumentsWriterPerThread blockedFlush = iterator.next();
      if (blockedFlush.deleteQueue == flushingQueue) {
        iterator.remove();
        addFlushingDWPT(blockedFlush); // 在就deleteQueue中的DWPT同时会移到flushingWriters和flushQueue中flushingWriters中
        // don't decr pending here - it's already done when DWPT is blocked
        flushQueue.add(blockedFlush);
      }
    }
  }
  // 会从DocumentsWriter.finishFullFlush()中主动跳到这里
  synchronized void finishFullFlush() {
    assert fullFlush;
    assert flushQueue.isEmpty();
    assert flushingWriters.isEmpty();
    try {
      if (!blockedFlushes.isEmpty()) { // 若发现
        assert assertBlockedFlushes(documentsWriter.deleteQueue);
        pruneBlockedQueue(documentsWriter.deleteQueue);
        assert blockedFlushes.isEmpty();
      }
    } finally {
      fullFlushMarkDone = fullFlush = false;

      updateStallState();
    }
  }
  
  boolean assertBlockedFlushes(DocumentsWriterDeleteQueue flushingQueue) {
    for (DocumentsWriterPerThread blockedFlush : blockedFlushes) {
      assert blockedFlush.deleteQueue == flushingQueue;
    }
    return true;
  }

  synchronized void abortFullFlushes() {
   try {
     abortPendingFlushes();
   } finally {
     fullFlushMarkDone = fullFlush = false;
   }
  }
  
  synchronized void abortPendingFlushes() {
    try {
      for (DocumentsWriterPerThread dwpt : flushQueue) {
        try {
          documentsWriter.subtractFlushedNumDocs(dwpt.getNumDocsInRAM());
          dwpt.abort();
        } catch (Exception ex) {
          // that's fine we just abort everything here this is best effort
        } finally {
          doAfterFlush(dwpt);
        }
      }
      for (DocumentsWriterPerThread blockedFlush : blockedFlushes) {
        try {
          addFlushingDWPT(blockedFlush); // add the blockedFlushes for correct accounting in doAfterFlush
          documentsWriter.subtractFlushedNumDocs(blockedFlush.getNumDocsInRAM());
          blockedFlush.abort();
        } catch (Exception ex) {
          // that's fine we just abort everything here this is best effort
        } finally {
          doAfterFlush(blockedFlush);
        }
      }
    } finally {
      flushQueue.clear();
      blockedFlushes.clear();
      updateStallState();
    }
  }
  
  /**
   * Returns <code>true</code> if a full flush is currently running
   */
  synchronized boolean isFullFlush() {
    return fullFlush;
  }

  /**
   * Returns the number of flushes that are already checked out but not yet
   * actively flushing
   */
  synchronized int numQueuedFlushes() {
    return flushQueue.size();
  }

  /**
   * Returns the number of flushes that are checked out but not yet available
   * for flushing. This only applies during a full flush if a DWPT needs
   * flushing but must not be flushed until the full flush has finished.
   */ // 发现正在进行full flush的同事，本DWPT也准备flush，那么先将本DWPT挂起，知道full flush完后后再继续进行
  synchronized int numBlockedFlushes() {
    return blockedFlushes.size();
  }
  
  /**
   * This method will block if too many DWPT are currently flushing and no
   * checked out DWPT are available
   */
  void waitIfStalled() {
    stallControl.waitIfStalled();
  }

  /**
   * Returns <code>true</code> iff stalled
   */
  boolean anyStalledThreads() {
    return stallControl.anyStalledThreads();
  }
  
  /**
   * Returns the {@link IndexWriter} {@link InfoStream}
   */
  public InfoStream getInfoStream() {
    return infoStream;
  }

  synchronized DocumentsWriterPerThread findLargestNonPendingWriter() {
    DocumentsWriterPerThread maxRamUsingWriter = null;
    long maxRamSoFar = 0;
    int count = 0;
    for (DocumentsWriterPerThread next : perThreadPool) {
      if (next.isFlushPending() == false && next.getNumDocsInRAM() > 0) {
        final long nextRam = next.bytesUsed();
        if (infoStream.isEnabled("FP")) {
          infoStream.message("FP", "thread state has " + nextRam + " bytes; docInRAM=" + next.getNumDocsInRAM());
        }
        count++;
        if (nextRam > maxRamSoFar) {
          maxRamSoFar = nextRam;
          maxRamUsingWriter = next;
        }
      }
    }
    if (infoStream.isEnabled("FP")) {
      infoStream.message("FP", count + " in-use non-flushing threads states");
    }
    return maxRamUsingWriter;
  }

  /**
   * Returns the largest non-pending flushable DWPT or <code>null</code> if there is none.
   */
  final DocumentsWriterPerThread checkoutLargestNonPendingWriter() {
    DocumentsWriterPerThread largestNonPendingWriter = findLargestNonPendingWriter();
    if (largestNonPendingWriter != null) {
      // we only lock this very briefly to swap it's DWPT out - we don't go through the DWPTPool and it's free queue
      largestNonPendingWriter.lock();
      try {
        if (perThreadPool.isRegistered(largestNonPendingWriter)) {
          synchronized (this) {
            try {
              return checkout(largestNonPendingWriter, largestNonPendingWriter.isFlushPending() == false);
            } finally {
              updateStallState();
            }
          }
        }
      } finally {
        largestNonPendingWriter.unlock();
      }
    }
    return null;
  }

  long getPeakActiveBytes() {
    return peakActiveBytes;
  }

  long getPeakNetBytes() {
    return peakNetBytes;
  }
}
