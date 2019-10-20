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
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.lucene.index.DocumentsWriterPerThread.FlushedSegment;
import org.apache.lucene.util.IOUtils;

/**
 * @lucene.internal 
 */  // 一个DocuentsWriter会私下产生一个DocumentsWriterFlushQueue
final class DocumentsWriterFlushQueue {
  private final Queue<FlushTicket> queue = new LinkedList<>(); // 全局打包待删除的Node
  // we track tickets separately since count must be present even before the ticket is
  // constructed ie. queue.size would not reflect it.
  private final AtomicInteger ticketCount = new AtomicInteger(); // 记录有多少个ticket
  private final ReentrantLock purgeLock = new ReentrantLock();

  synchronized boolean addDeletes(DocumentsWriterDeleteQueue deleteQueue) throws IOException {
    incTickets();// first inc the ticket count - freeze opens
                 // a window for #anyChanges to fail
    boolean success = false;
    try {
      FrozenBufferedUpdates frozenBufferedUpdates = deleteQueue.maybeFreezeGlobalBuffer();
      if (frozenBufferedUpdates != null) { // no need to publish anything if we don't have any frozen updates
        queue.add(new FlushTicket(frozenBufferedUpdates, false));
        success = true;
      }
    } finally {
      if (!success) {
        decTickets();
      }
    }
    return success;
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
  }// 这里并发控制执行，删除顺序不能颠倒（详见说明见https://www.amazingkoala.com.cn/Lucene/Index/2019/0718/75.html介绍）
  // 每一个DWPT执行doFlush后，都会生成一个FlushTicket对象，并同步的添加到Queue<FlushTicket> queue中。
  synchronized FlushTicket addFlushTicket(DocumentsWriterPerThread dwpt) throws IOException {
    // Each flush is assigned a ticket in the order they acquire the ticketQueue
    // lock 每次刷新都会分配一个ticket，以便获取ticketQueue锁
    incTickets();
    boolean success = false;
    try { // 看来每个DWPT进来时，都会主动从全局globalSlice创建一个作用于已生产的所有已存在segment的FrozenBufferedUpdates，以删除。
      // prepare flush freezes the global deletes - do in synced block!
      final FlushTicket ticket = new FlushTicket(dwpt.prepareFlush(), true); // 冻结全局的删除Nodes
      queue.add(ticket);
      success = true;
      return ticket;
    } finally {
      if (!success) {
        decTickets();
      }
    }
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

  private void innerPurge(IOUtils.IOConsumer<FlushTicket> consumer) throws IOException {
    assert purgeLock.isHeldByCurrentThread();
    while (true) {
      final FlushTicket head;
      final boolean canPublish;
      synchronized (this) { // 循环进行publish
        head = queue.peek(); // 获取顶部，并没有拿出来
        canPublish = head != null && head.canPublish(); // do this synced  是否可以publish
      }
      if (canPublish) {
        try {
          /*
           * if we block on publish -> lock IW -> lock BufferedDeletes we don't block
           * concurrent segment flushes just because they want to append to the queue.
           * the downside is that we need to force a purge on fullFlush since there could
           * be a ticket still in the queue. 
           */
          consumer.accept(head); // 会跳转到IndexWriter.publishFlushedSegments()里面定义的发布代码里面

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

  void forcePurge(IOUtils.IOConsumer<FlushTicket> consumer) throws IOException {
    assert !Thread.holdsLock(this);
    purgeLock.lock(); // 一定的获取到，否则就阻塞
    try {
      innerPurge(consumer);
    } finally {
      purgeLock.unlock();
    }
  }

  void tryPurge(IOUtils.IOConsumer<FlushTicket> consumer) throws IOException {
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
    return ticketCount.get(); // 只有
  }

  static final class FlushTicket { // frozenUpdates是一个包含删除信息且作用于其他段中的文档的全局FrozenBufferedUpdate对象
    private final FrozenBufferedUpdates frozenUpdates; // 在执行DWPT的doFlush()流程中需要生成一个全局的删除信息FrozenBufferedUpdates，它将作用（apply）到索引目录中已有的段
    private final boolean hasSegment;
    private FlushedSegment segment; // FlushedSegment不为空：FlushTicket在发布生成的段的流程中需要执行将删除信息作用（apply）到其他段以及更新生成的段的任务；FlushedSegment为空：FlushTicket在发布生成的段的流程中仅仅需要执行将删除信息作用到其他段的任务
    private boolean failed = false;
    private boolean published = false;

    FlushTicket(FrozenBufferedUpdates frozenUpdates, boolean hasSegment) {
      this.frozenUpdates = frozenUpdates;
      this.hasSegment = hasSegment;
    }
    // 只要成功建立了segment，就可以publish
    boolean canPublish() {
      return hasSegment == false || segment != null || failed; //
    }

    synchronized void markPublished() {
      assert published == false: "ticket was already published - can not publish twice";
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
     * Returns the flushed segment or <code>null</code> if this flush ticket doesn't have a segment. This can be the
     * case if this ticket represents a flushed global frozen updates package.
     */
    FlushedSegment getFlushedSegment() {
      return segment;
    }

    /**
     * Returns a frozen global deletes package.
     */
    FrozenBufferedUpdates getFrozenUpdates() {
      return frozenUpdates;
    }
  }
}
