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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.IOSupplier;
import org.apache.lucene.util.ThreadInterruptedException;

/**
 * {@link DocumentsWriterPerThreadPool} controls {@link DocumentsWriterPerThread} instances
 * and their thread assignments during indexing. Each {@link DocumentsWriterPerThread} is once a
 * obtained from the pool exclusively used for indexing a
 * single document or list of documents by the obtaining thread. Each indexing thread must obtain
 * such a {@link DocumentsWriterPerThread} to make progress. Depending on the
 * {@link DocumentsWriterPerThreadPool} implementation {@link DocumentsWriterPerThread}
 * assignments might differ from document to document.
 * <p>
 * Once a {@link DocumentsWriterPerThread} is selected for flush the {@link DocumentsWriterPerThread} will
 * be checked out of the thread pool and won't be reused for indexing. See {@link #checkout(DocumentsWriterPerThread)}.
 * </p> // 每个shard 都拥有一个DocumentsWriterPerThreadPool
 */ // DocumentsWriterPerThread 对象创建了DocConsumer 即IndexChain(整个索引的核心)，同时ThreadState  封装了DocumentsWriterPerThread对象，同时拥有每一个线程需要flush的对象数据，他得每一个成员和方法必须在一个时刻只能一个线程访问，调用者必须自己加锁，解锁。
final class DocumentsWriterPerThreadPool implements Iterable<DocumentsWriterPerThread>, Closeable {
  // DocumentsWriterPerThread定义在DocumentsWriter初始化中
  private final Set<DocumentsWriterPerThread> dwpts = Collections.newSetFromMap(new IdentityHashMap<>());// freeList中的一定包含在dwpts；若某个写入触发es refresh的话，那么这个DocumentsWriterPerThread就从dwpts中彻底去掉了
  private final Deque<DocumentsWriterPerThread> freeList = new ArrayDeque<>();// 锁放这里，每写一个doc时，都会从freeList（删掉）申请获取一个现成的，若没有的话，则从新产生一个，并同时放入dwpts。写完后最终再放入 
  private final IOSupplier<DocumentsWriterPerThread> dwptFactory; // 定义在DocumentsWriter初始化中。每个shard,只会拥有唯一一个dwptFactory。
  private int takenWriterPermits = 0;// 作用类似型号量，与刷新时相关，在全量刷新阶段，会短暂临时+1, 此时将不能继续不能创建新的ThreadState。
  private boolean closed;


  DocumentsWriterPerThreadPool(IOSupplier<DocumentsWriterPerThread> dwptFactory) {
    this.dwptFactory = dwptFactory;
  }

  /**
   * Returns the active number of {@link DocumentsWriterPerThread} instances.
   */
  synchronized int size() {
    return dwpts.size();
  }
  // 不让再继续创建锁进行写入了
  synchronized void lockNewWriters() {
    // this is similar to a semaphore - we need to acquire all permits ie. takenWriterPermits must be == 0
    // any call to lockNewWriters() must be followed by unlockNewWriters() otherwise we will deadlock at some
    // point
    assert takenWriterPermits >= 0;// 此时新线程将不能再获取新的ThreadState
    takenWriterPermits++;
  }
  // 作用类似型号量
  synchronized void unlockNewWriters() {
    assert takenWriterPermits > 0;
    takenWriterPermits--;
    if (takenWriterPermits == 0) {
      notifyAll();
    }
  }

  /**
   * Returns a new already locked {@link DocumentsWriterPerThread}
   *
   * @return a new {@link DocumentsWriterPerThread}
   */
  private synchronized DocumentsWriterPerThread newWriter() throws IOException {// 使用了并发控制，难道只能有一个线程获取takenThreadStatePermits
    assert takenWriterPermits >= 0;
    while (takenWriterPermits > 0) { // 只能一直等待
      // we can't create new DWPTs while not all permits are available
      try {
        wait();
      } catch (InterruptedException ie) {
        throw new ThreadInterruptedException(ie);
      }
    }
    DocumentsWriterPerThread dwpt = dwptFactory.get(); // 将跑到 DocumentsWriter 构造函数perThreadPool的构造中
    dwpt.lock(); // lock so nobody else will get this DWPT
    dwpts.add(dwpt);
    return dwpt;
  }

  // TODO: maybe we should try to do load leveling here: we want roughly even numbers
  // of items (docs, deletes, DV updates) to most take advantage of concurrency while flushing

  /** This method is used by DocumentsWriter/FlushControl to obtain a DWPT to do an indexing operation (add/updateDocument). */
  DocumentsWriterPerThread getAndLock() throws IOException {
    synchronized (this) {
      if (closed) {
        throw new AlreadyClosedException("DWPTPool is already closed");
      }
      // Important that we are LIFO here! This way if number of concurrent indexing threads was once high,
      // but has now reduced, we only use a limited number of DWPTs. This also guarantees that if we have suddenly
      // a single thread indexing
      final Iterator<DocumentsWriterPerThread> descendingIterator = freeList.descendingIterator(); // 若freeList有空闲，是不是就绕过这里了
      while (descendingIterator.hasNext()) { // 循环式的
        DocumentsWriterPerThread perThread = descendingIterator.next();
        if (perThread.tryLock()) { //若不成功，说明已经被抢占了
          descendingIterator.remove();
          return perThread;
        }
      }
      // DWPT is already locked before return by this method:
      return newWriter(); // 这个被锁
    }
  }

  void marksAsFreeAndUnlock(DocumentsWriterPerThread state) {
    synchronized (this) {
      assert dwpts.contains(state) : "we tried to add a DWPT back to the pool but the pool doesn't know aobut this DWPT";
      freeList.add(state); // 将ThreadState归还，但是并没有将DocumentsWriterPerThread清空
    }
    state.unlock();
  }

  @Override
  public synchronized Iterator<DocumentsWriterPerThread> iterator() { // 实际遍历的dwpts
    return Arrays.asList(dwpts.toArray(new DocumentsWriterPerThread[dwpts.size()])).iterator(); // copy on read - this is a quick op since num states is low
  }

  /**
   * Filters all DWPTs the given predicate applies to and that can be checked out of the pool via
   * {@link #checkout(DocumentsWriterPerThread)}. All DWPTs returned from this method are already locked
   * and {@link #isRegistered(DocumentsWriterPerThread)} will return <code>true</code> for all returned DWPTs
   */
  List<DocumentsWriterPerThread> filterAndLock(Predicate<DocumentsWriterPerThread> predicate) {
    List<DocumentsWriterPerThread> list = new ArrayList<>();
    for (DocumentsWriterPerThread perThread : this) { //遍历的dwpts
      if (predicate.test(perThread)) {
        perThread.lock(); // 先锁住
        if (isRegistered(perThread)) { //
          list.add(perThread);
        } else {
          // somebody else has taken this DWPT out of the pool.
          // unlock and let it go
          perThread.unlock();
        }
      }
    }
    return Collections.unmodifiableList(list);
  }

  /**
   * Removes the given DWPT from the pool unless it's already been removed before.
   * @return <code>true</code> iff the given DWPT has been removed. Otherwise <code>false</code>
   */ // 从当前dwpts、freeList中删掉
  synchronized boolean checkout(DocumentsWriterPerThread perThread) {
   assert perThread.isHeldByCurrentThread();
    if (dwpts.remove(perThread)) { //
      freeList.remove(perThread);
    } else {
      assert freeList.contains(perThread) == false;
      return false;
    }
    return true;
  }

  /**
   * Returns <code>true</code> if this DWPT is still part of the pool
   */
  synchronized boolean isRegistered(DocumentsWriterPerThread perThread) {
    return dwpts.contains(perThread);
  }

  @Override
  public synchronized void close() {
    this.closed = true;
  }
}
