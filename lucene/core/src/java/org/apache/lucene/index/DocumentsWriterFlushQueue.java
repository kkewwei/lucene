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
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import org.apache.lucene.index.DocumentsWriterPerThread.FlushedSegment;
import org.apache.lucene.util.IOConsumer;

/**
 * @lucene.internal
 */
final class DocumentsWriterFlushQueue {// 一个DocuentsWriter会私下产生一个DocumentsWriterFlushQueue
  private final Queue<FlushTicket> queue = new ArrayDeque<>();// 排序了
  // we track tickets separately since count must be present even before the ticket is
  // constructed ie. queue.size would not reflect it.
  private final AtomicInteger ticketCount = new AtomicInteger(); // 记录有多少个ticket, 在放入queue之前，还需要时间
  private final ReentrantLock purgeLock = new ReentrantLock();// 从queue中清除FlushTicket的lock, publishFlushedSegments时会锁住
  // 这里并发控制执行，删除顺序不能颠倒（详见说明见https://www.amazingkoala.com.cn/Lucene/Index/2019/0718/75.html介绍）
  synchronized FlushTicket addTicket(Supplier<FlushTicket> ticketSupplier) throws IOException { // 每一个DWPT执行doFlush后，都会生成一个FlushTicket对象，并同步的添加到Queue<FlushTicket> queue中。
    // first inc the ticket count - freeze opens a window for #anyChanges to fail
    incTickets();//  每次刷新都会分配一个ticket，以便获取ticketQueue锁
    boolean success = false;
    try { // 看来每个DWPT进来时，都会主动从全局globalSlice创建一个作用于已生产的所有已存在segment的FrozenBufferedUpdates，以删除。
      FlushTicket ticket = ticketSupplier.get(); // 冻结全局的删除Nodes
      if (ticket != null) {
        // no need to publish anything if we don't have any frozen updates
        queue.add(ticket); // 每个dwpt都会以获取FlushTicket的顺序给放入queue
        success = true;
      }
      return ticket;
    } finally {
      if (!success) {
        decTickets();
      }
    }
  }
// 只有innerPurge结束了（执行完publishFlushedSegments），才会调用
  private void incTickets() {
    int numTickets = ticketCount.incrementAndGet();
    assert numTickets > 0;
  }
// 只有innerPurge结束了（执行完publishFlushedSegments），才会调用
  private void decTickets() {
    int numTickets = ticketCount.decrementAndGet();
    assert numTickets >= 0;
  }

  synchronized void addSegment(FlushTicket ticket, FlushedSegment segment) {
    assert ticket.hasSegment;
    // the actual flush is done asynchronously and once done the FlushedSegment
    // is passed to the flush ticket
    ticket.setSegment(segment);
  }

  synchronized void markTicketFailed(FlushTicket ticket) {
    assert ticket.hasSegment;
    // to free the queue we mark tickets as failed just to clean up the queue.
    ticket.setFailed();
  }

  boolean hasTickets() {
    assert ticketCount.get() >= 0 : "ticketCount should be >= 0 but was: " + ticketCount.get();
    return ticketCount.get() != 0;
  }
  //innerPurge函数依次从该队列中获取SegmentFlushTicket，调用其publish函数将其写入IndexWriter的BufferedUpdatesStream中。操作成功后通过poll函数从队列中删除该SegmentFlushTicket。
  private void innerPurge(IOConsumer<FlushTicket> consumer) throws IOException {
    assert purgeLock.isHeldByCurrentThread();// 其实已经锁起来了。
    while (true) { //这里会依次发布每个segment
      final FlushTicket head;
      final boolean canPublish;
      synchronized (this) { // 循环进行publish
        head = queue.peek();// 前面顺序放。获取顶部，并没有拿出来
        canPublish = head != null && head.canPublish(); // do this synced   是否可以publish
      }
      if (canPublish) { // 拿出来一个进行publish
        try {
          /*
           * if we block on publish -> lock IW -> lock BufferedDeletes we don't block
           * concurrent segment flushes just because they want to append to the queue.
           * the downside is that we need to force a purge on fullFlush since there could
           * be a ticket still in the queue.
           */
          consumer.accept(head); // 会跳转到IndexWriter.publishFlushedSegment()里面定义的发布代码里面

        } finally {
          synchronized (this) {
            // finally remove the published ticket from the queue
            final FlushTicket poll = queue.poll(); // 最后才移除这个ticket
            decTickets(); // 只有innerPurge结束了（执行完publishFlushedSegments），才会调用
            // we hold the purgeLock so no other thread should have polled:
            assert poll == head;
          }
        }
      } else {
        break;
      }
    }
  }
// 强制清除queue里面的FlushTicket
  void forcePurge(IOConsumer<FlushTicket> consumer) throws IOException {
    assert !Thread.holdsLock(this);
    purgeLock.lock(); // 一定的获取到，否则就阻塞
    try {
      innerPurge(consumer);
    } finally {
      purgeLock.unlock();
    }
  }

  void tryPurge(IOConsumer<FlushTicket> consumer) throws IOException {
    assert !Thread.holdsLock(this);
    if (purgeLock.tryLock()) { // 尝试获取锁，只允许一个线程进来工作
      try {
        innerPurge(consumer);
      } finally {
        purgeLock.unlock();
      }// 获取不到就直接退出了
    }
  }

  int getTicketCount() {
    return ticketCount.get();
  }

  static final class FlushTicket { // frozenUpdates是一个包含删除信息且作用于其他段中的文档的全局FrozenBufferedUpdate对象
    private final FrozenBufferedUpdates frozenUpdates; // 在执行DWPT的doFlush()流程中需要生成一个全局的删除信息FrozenBufferedUpdates，它将作用（apply）到索引目录中已有的段
    private final boolean hasSegment;
    private FlushedSegment segment; // FlushedSegment不为空：FlushTicket在发布生成的段的流程中需要执行将删除信息作用（apply）到其他段以及更新生成的段的任务；FlushedSegment为空：FlushTicket在发布生成的段的流程中仅仅需要执行将删除信息作用到其他段的任务
    private boolean failed = false;
    private boolean published = false;

    FlushTicket(FrozenBufferedUpdates frozenUpdates, boolean hasSegment) {
      this.frozenUpdates = frozenUpdates;// 刷新时，第一个DocumentsWriterPerThread的 frozenUpdates 是全局FrozenBufferedUpdates， 后面的segment的改值都为空
      this.hasSegment = hasSegment;// 后面的frozenUpdates都是空的
    }
    // 只要成功建立了segment，就可以publish
    boolean canPublish() {
      return hasSegment == false || segment != null || failed; //
    }

    synchronized void markPublished() {
      assert published == false : "ticket was already published - can not publish twice";
      published = true;
    }

    private void setSegment(FlushedSegment segment) {
      assert !failed;
      this.segment = segment;
    }

    private void setFailed() {
      assert segment == null;
      failed = true;
    }

    /**
     * Returns the flushed segment or <code>null</code> if this flush ticket doesn't have a segment.
     * This can be the case if this ticket represents a flushed global frozen updates package.
     */
    FlushedSegment getFlushedSegment() {
      return segment;
    }

    /** Returns a frozen global deletes package. */
    FrozenBufferedUpdates getFrozenUpdates() {
      return frozenUpdates; // 全局的，第一个frozen后后面的就为null了
    }
  }
}
