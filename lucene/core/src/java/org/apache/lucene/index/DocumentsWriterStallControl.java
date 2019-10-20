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

import java.util.IdentityHashMap;
import java.util.Map;

import org.apache.lucene.util.ThreadInterruptedException;

/**
 * Controls the health status of a {@link DocumentsWriter} sessions. This class
 * used to block incoming indexing threads if flushing significantly slower than
 * indexing to ensure the {@link DocumentsWriter}s healthiness. If flushing is
 * significantly slower than indexing the net memory used within an
 * {@link IndexWriter} session can increase very quickly and easily exceed the
 * JVM's available memory.
 * <p>
 * To prevent OOM Errors and ensure IndexWriter's stability this class blocks
 * incoming threads from indexing once 2 x number of available
 * {@link DocumentsWriterPerThread}s in {@link DocumentsWriterPerThreadPool} is exceeded.
 * Once flushing catches up and the number of flushing DWPT is equal or lower
 * than the number of active {@link DocumentsWriterPerThread}s threads are released and can
 * continue indexing. // 当刷新的DWPT小于等于活跃的ThreadState县城个数，就可以继续写入了
 */
final class DocumentsWriterStallControl { // DocumentsWriter阻塞控制器，为了阻止写入程序，防止写入速度大于刷新速度
  
  private volatile boolean stalled; // 最新判断的阻塞状态
  private int numWaiting; // only with assert 增加因为内存用超而等待的线程数
  private boolean wasStalled; // only with assert  // 仅仅是为了在test中使用，没啥用
  private final Map<Thread, Boolean> waiting = new IdentityHashMap<>(); // only with assert

  /**
   * Update the stalled flag status. This method will set the stalled flag to
   * <code>true</code> iff the number of flushing
   * {@link DocumentsWriterPerThread} is greater than the number of active
   * {@link DocumentsWriterPerThread}. Otherwise it will reset the
   * {@link DocumentsWriterStallControl} to healthy and release all threads
   * waiting on {@link #waitIfStalled()}
   */ // 当刷新DocumentsWriterPerThread大于写入的DocumentsWriterPerThread个数，就说明需要阻塞写入了
  synchronized void updateStalled(boolean stalled) {
    if (this.stalled != stalled) {
      this.stalled = stalled;
      if (stalled) {
        wasStalled = true;// 仅仅是为了在test中使用，没啥用
      }
      notifyAll(); // 没事就试着唤醒下所有被阻塞的
    }
  }
  
  /**
   * Blocks if documents writing is currently in a stalled state. 
   * 
   */ // 仅仅阻塞1s钟
  void waitIfStalled() {
    if (stalled) { // 是阻塞的状态
      synchronized (this) {
        if (stalled) { // react on the first wakeup call!
          // don't loop here, higher level logic will re-stall!
          try {
            incWaiters();
            // Defensive, in case we have a concurrency bug that fails to .notify/All our thread:
            // just wait for up to 1 second here, and let caller re-stall if it's still needed:
            wait(1000); // 仅最长等待时间1s。
            decrWaiters();
          } catch (InterruptedException e) {
            throw new ThreadInterruptedException(e);
          }
        }
      }
    }
  }
  
  boolean anyStalledThreads() {
    return stalled; // false
  }
  
  private void incWaiters() {
    numWaiting++;
    assert waiting.put(Thread.currentThread(), Boolean.TRUE) == null; // 之前没有被阻塞过
    assert numWaiting > 0;
  }
  
  private void decrWaiters() {
    numWaiting--;
    assert waiting.remove(Thread.currentThread()) != null;
    assert numWaiting >= 0;
  }
  
  synchronized boolean hasBlocked() { // for tests
    return numWaiting > 0;
  }
  
  boolean isHealthy() { // for tests
    return !stalled; // volatile read!
  }
  
  synchronized boolean isThreadQueued(Thread t) { // for tests
    return waiting.containsKey(t);
  }

  synchronized boolean wasStalled() { // for tests
    return wasStalled;
  }
}
