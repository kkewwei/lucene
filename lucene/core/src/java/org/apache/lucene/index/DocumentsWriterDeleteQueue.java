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
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;
import org.apache.lucene.index.DocValuesUpdate.BinaryDocValuesUpdate;
import org.apache.lucene.index.DocValuesUpdate.NumericDocValuesUpdate;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.InfoStream;

/**
 * {@link DocumentsWriterDeleteQueue} is a non-blocking linked pending deletes queue. In contrast to
 * other queue implementation we only maintain the tail of the queue. A delete queue is always used
 * in a context of a set of DWPTs and a global delete pool. Each of the DWPT and the global pool
 * need to maintain their 'own' head of the queue (as a DeleteSlice instance per {@link
 * DocumentsWriterPerThread}). The difference between the DWPT and the global pool is that the DWPT
 * starts maintaining a head once it has added its first document since for its segments private
 * deletes only the deletes after that document are relevant. The global pool instead starts
 * maintaining the head once this instance is created by taking the sentinel instance as its initial
 * head.
 *
 * <p>Since each {@link DeleteSlice} maintains its own head and the list is only single linked the
 * garbage collector takes care of pruning the list for us. All nodes in the list that are still
 * relevant should be either directly or indirectly referenced by one of the DWPT's private {@link
 * DeleteSlice} or by the global {@link BufferedUpdates} slice.
 *
 * <p>Each DWPT as well as the global delete pool maintain their private DeleteSlice instance. In
 * the DWPT case updating a slice is equivalent to atomically finishing the document. The slice
 * update guarantees a "happens before" relationship to all other updates in the same indexing
 * session. When a DWPT updates a document it:
 *
 * <ol>
 *   <li>consumes a document and finishes its processing
 *   <li>updates its private {@link DeleteSlice} either by calling {@link #updateSlice(DeleteSlice)}
 *       or {@link #add(Node, DeleteSlice)} (if the document has a delTerm)
 *   <li>applies all deletes in the slice to its private {@link BufferedUpdates} and resets it
 *   <li>increments its internal document id
 * </ol>
 *
 * The DWPT also doesn't apply its current documents delete term until it has updated its delete
 * slice which ensures the consistency of the update. If the update fails before the DeleteSlice
 * could have been updated the deleteTerm will also not be added to its private deletes neither to
 * the global deletes. // 删除操作可参考：https://www.jianshu.com/p/26ba09055175
 */  // 一个DocuentsWriter会私下维持一个DocumentsWriterFlushQueue。每次full flush 都会产生一个新的
final class DocumentsWriterDeleteQueue implements Accountable, Closeable {
  // 主要用于在flush时记录每个DWPT的刷新操作，这里记录的刷新操作主要是刷新之后形成的FlushedSegment以及全局删除操作，FlushedSegment是此次刷新之后的最终段内存视图。其中各种更新操作已经进行过具体处理，Term删除也进行了处理，但是Query删除还依然保持未处理，所以FlushedSegment中还保存了Query删除操作，但是没有Term删除操作。
  // the current end (latest delete operation) in the delete queue:
  private volatile Node<?> tail; // 只要有一个改了，那么全局都改了。代表最新的的delete

  private volatile boolean closed = false;

  /**
   * Used to record deletes against all prior (already written to disk) segments. Whenever any
   * segment flushes, we bundle up this set of deletes and insert into the buffered updates stream
   * before the newly flushed segment(s).
   */
  private final DeleteSlice globalSlice; // 在刷新前，所有delete已经放入globalBufferedUpdates了

  private final BufferedUpdates globalBufferedUpdates; // 它描述了所有的删除信息，该删除信息会被保存在全局BufferedUpdates。会作用剩余存量的全局的segments的
// globalSlice 和 globalBufferedUpdates 变更读取，都依靠globalBufferLock
  // only acquired to update the global deletes, pkg-private for access by tests:
  final ReentrantLock globalBufferLock = new ReentrantLock();

  final long generation; // 每刷新时产生一个新的DocumentsWriterDeleteQueue，那么generation+1

  /**
   * Generates the sequence number that IW returns to callers changing the index, showing the
   * effective serialization of all operations.
   */
  private final AtomicLong nextSeqNo;// 全局惟一的，任何add delete, update都会调用该函数

  private final InfoStream infoStream;

  private volatile long maxSeqNo = Long.MAX_VALUE; /// 启动的时候为0,当前已经使用的最大的

  private final long startSeqNo; //三个
  private final LongSupplier previousMaxSeqId;
  private boolean advanced;// 这个queue是否已经被替换了

  DocumentsWriterDeleteQueue(InfoStream infoStream) {
    // seqNo must start at 1 because some APIs negate this to also return a boolean
    this(infoStream, 0, 1, () -> 0);
  }
  // 每次flush时产生新的
  private DocumentsWriterDeleteQueue(
      InfoStream infoStream, long generation, long startSeqNo, LongSupplier previousMaxSeqId) {
    this.infoStream = infoStream;
    this.globalBufferedUpdates = new BufferedUpdates("global");
    this.generation = generation;
    this.nextSeqNo = new AtomicLong(startSeqNo);
    this.startSeqNo = startSeqNo;
    this.previousMaxSeqId = previousMaxSeqId;
    long value = previousMaxSeqId.getAsLong();
    assert value <= startSeqNo : "illegal max sequence ID: " + value + " start was: " + startSeqNo;
    /*
     * we use a sentinel instance as our initial tail. No slice will ever try to
     * apply this tail since the head is always omitted.
     */
    tail = new Node<>(null); // sentinel   哨兵
    globalSlice = new DeleteSlice(tail); // //globalSlice默认指向globalQueue的尾部
  }

  long addDelete(Query... queries) {
    long seqNo = add(new QueryArrayNode(queries)); //这里是QueryArrayNode，split时就会进来，传递query
    tryApplyGlobalSlice();
    return seqNo;
  }

  long addDelete(Term... terms) {
    long seqNo = add(new TermArrayNode(terms)); //向全局的 tail 指向的链表尾部插入，tail始终指向尾部
    tryApplyGlobalSlice(); //将globalSlice中截取的删除分片放入globalBufferedUpdates中
    return seqNo;
  }

  long addDocValuesUpdates(DocValuesUpdate... updates) {
    long seqNo = add(new DocValuesUpdatesNode(updates));
    tryApplyGlobalSlice();
    return seqNo;
  }

  static Node<Term> newNode(Term term) {
    return new TermNode(term);
  }

  static Node<Query> newNode(Query query) {
    return new QueryNode(query);
  }

  static Node<DocValuesUpdate[]> newNode(DocValuesUpdate... updates) {
    return new DocValuesUpdatesNode(updates);// 一般是 NumericDocValuesUpdate
  }

  /** invariant for document update */
  long add(Node<?> deleteNode, DeleteSlice slice) {
    long seqNo = add(deleteNode);// 先让全局tail向前移动。tail代表最新未应用的删除
    /*
     * this is an update request where the term is the updated documents
     * delTerm. in that case we need to guarantee that this insert is atomic
     * with regards to the given delete slice. This means if two threads try to
     * update the same document with in turn the same delTerm one of them must
     * win. By taking the node we have created for our del term as the new tail
     * it is guaranteed that if another thread adds the same right after us we
     * will apply this delete next time we update our slice and one of the two
     * competing updates wins!
     */
    slice.sliceTail = deleteNode;// 再移动PerThread.slice的sliceTail
    assert slice.sliceHead != slice.sliceTail : "slice head and tail must differ after add";
    tryApplyGlobalSlice(); // TODO doing this each time is not necessary maybe 再运用全局的
    // we can do it just every n times or so?

    return seqNo;
  }
  // 尾插法，tail始终指向尾部
  synchronized long add(Node<?> newNode) {
    ensureOpen();
    tail.next = newNode; // tail = TermArrayNode 先接上去再说
    this.tail = newNode;
    return getNextSequenceNumber();
  }

  boolean anyChanges() {
    globalBufferLock.lock();
    try {
      /*
       * check if all items in the global slice were applied
       * and if the global slice is up-to-date
       * and if globalBufferedUpdates has changes
       */
      return globalBufferedUpdates.any()
          || !globalSlice.isEmpty()
          || globalSlice.sliceTail != tail
          || tail.next != null;
    } finally {
      globalBufferLock.unlock();
    }
  }
  //首先globalSlice截取tail的放入sliceTail，然后再将globalSlice的sliceHead->sliceTail里面的DelteTerm放入globalBufferedUpdates中
  void tryApplyGlobalSlice() {
    if (globalBufferLock.tryLock()) {// 只是尝试获取，获取不到就不会去更新
      try {
        ensureOpen();
        /*
         * The global buffer must be locked but we don't need to update them if
         * there is an update going on right now. It is sufficient to apply the
         * deletes that have been added after the current in-flight global slices
         * tail the next time we can get the lock!
         */
        if (updateSliceNoSeqNo(globalSlice)) {// 若globalSlice的sliceTail不是最新的，那么将globalSlice的sliceTail指向最新
          globalSlice.apply(globalBufferedUpdates, BufferedUpdates.MAX_INT);// 这里的docIDUpto=max。后面会修改过来
        }// 将globalSlice中更新的逐个放入globalBufferedUpdates中
      } finally {
        globalBufferLock.unlock();
      }
    }
  }
  // callerSlice：每个DWPT都会调用freezeGlobalBuffer。只有一个地方调用这个函数:DocumentsWriterPerThread.prepareFlush()
  FrozenBufferedUpdates freezeGlobalBuffer(DeleteSlice callerSlice) {// 每个DWPT都能感知到删除动作
    globalBufferLock.lock();
    try {
      ensureOpen();
      /*
       * Here we freeze the global buffer so we need to lock it, apply all
       * deletes in the queue and reset the global slice to let the GC prune the
       * queue.
       */// 每个
      final Node<?> currentTail = tail; // take the current tail make this local any
      // Changes after this call are applied later
      // and not relevant here
      if (callerSlice != null) {// 在Flush时，每个DocumentsWriterPerThread都会进来获取下缺少的
        // Update the callers slices so we are on the same page
        callerSlice.sliceTail = currentTail; // 先给每个WDPT最新的全局tail(以免漏掉)
      }
      return freezeGlobalBufferInternal(currentTail);
    } finally {
      globalBufferLock.unlock();
    }
  }

  /**
   * This may freeze the global buffer unless the delete queue has already been closed. If the queue
   * has been closed this method will return <code>null</code>
   */
  FrozenBufferedUpdates maybeFreezeGlobalBuffer() {
    globalBufferLock.lock();
    try {
      if (closed == false) {
        /*
         * Here we freeze the global buffer so we need to lock it, apply all
         * deletes in the queue and reset the global slice to let the GC prune the
         * queue.
         */
        return freezeGlobalBufferInternal(tail); // take the current tail make this local any
      } else {
        assert anyChanges() == false : "we are closed but have changes";
        return null;
      }
    } finally {
      globalBufferLock.unlock();
    }
  }
  // 两处调用，传递进来的都是全局tail
  private FrozenBufferedUpdates freezeGlobalBufferInternal(final Node<?> currentTail) {
    assert globalBufferLock.isHeldByCurrentThread();
    if (globalSlice.sliceTail != currentTail) { // 最新的是不是我们传递进来的
      globalSlice.sliceTail = currentTail;
      globalSlice.apply(globalBufferedUpdates, BufferedUpdates.MAX_INT); // 若globalSlice还有未取完的，先取完放入
    }

    if (globalBufferedUpdates.any()) {// 全局的，第一个触发了之后，后面的就不用再放入DocumentsWriterPerThread。
      final FrozenBufferedUpdates packet =
          new FrozenBufferedUpdates(infoStream, globalBufferedUpdates, null);// 作用全局，所以privateSegment=null
      globalBufferedUpdates.clear(); // global内容此时已经放入packet了。这个冻结的全局globalBufferedUpdates只用于已经存在的segment，只要一个segment来appled就行了。
      return packet;
    } else {
      return null; // 后面的DocumentsWriterPerThread触发将返回null
    }
  }
  // 每个DocumentsWriterPerThread都拥有一个
  DeleteSlice newSlice() {
    return new DeleteSlice(tail); // 若tail修改了，这里可以立马感知到
  }
  // 多线程共享的
  /** Negative result means there were new deletes since we last applied */
  synchronized long updateSlice(DeleteSlice slice) {// 每个DocumentsWriterPerThread.finishDocuments()写完一个文档，会更新这里。
    ensureOpen();
    long seqNo = getNextSequenceNumber();
    if (slice.sliceTail != tail) { // 映射了这个DWPT工作期间，全局tail的删除情况(并没有冻结DWDQ的)
      // new deletes arrived since we last checked
      slice.sliceTail = tail;
      seqNo = -seqNo; // 有删除了
    }
    return seqNo;
  }

  /** Just like updateSlice, but does not assign a sequence number */
  boolean updateSliceNoSeqNo(DeleteSlice slice) {
    if (slice.sliceTail != tail) {
      // new deletes arrived since we last checked
      slice.sliceTail = tail;
      return true;
    }
    return false;
  }

  private void ensureOpen() {
    if (closed) {
      throw new AlreadyClosedException(
          "This " + DocumentsWriterDeleteQueue.class.getSimpleName() + " is already closed");
    }
  }

  public boolean isOpen() {
    return closed == false;
  }

  @Override
  public synchronized void close() {
    globalBufferLock.lock();
    try {
      if (anyChanges()) {
        throw new IllegalStateException("Can't close queue unless all changes are applied");
      }
      this.closed = true;
      long seqNo = nextSeqNo.get();
      assert seqNo <= maxSeqNo
          : "maxSeqNo must be greater or equal to " + seqNo + " but was " + maxSeqNo;
      nextSeqNo.set(maxSeqNo + 1);
    } finally {
      globalBufferLock.unlock();
    }
  }
  // 一个DocumentsWriterPerThread会产生一个DeleteSlice，而每个DeleteSlice都会作为DocumentsWriterDeleteQueue中一个节点存放起来
  static class DeleteSlice {
    // No need to be volatile, slices are thread captive (only accessed by one thread)!
    Node<?> sliceHead; // we don't apply this one 这个是过期的那个头，开区间
    Node<?> sliceTail; // 用来存globalQueue上截取自上次flush之后自己需要记录的删除操作。一般都是sliceTail记录最新的那部分

    DeleteSlice(Node<?> currentTail) {
      assert currentTail != null;
      /*
       * Initially this is a 0 length slice pointing to the 'current' tail of
       * the queue. Once we update the slice we only need to assign the tail and
       * have a new slice
       */
      sliceHead = sliceTail = currentTail;
    }

    void apply(BufferedUpdates del, int docIDUpto) {
      if (sliceHead == sliceTail) {
        // 0 length slice
        return;
      }
      /*
       * When we apply a slice we take the head and get its next as our first
       * item to apply and continue until we applied the tail. If the head and
       * tail in this slice are not equal then there will be at least one more
       * non-null node in the slice!
       */
      Node<?> current = sliceHead; // 从head（过期的头，里面数据直接丢弃）
      do {
        current = current.next;
        assert current != null
            : "slice property violated between the head on the tail must not be a null node";
        current.apply(del, docIDUpto); // 默认会跑到DocumentsWriterDeleteQueue$TermArrayNode，或者QueryArrayNode。将NumericDocValuesUpdate逐个放入del中
      } while (current != sliceTail);
      reset(); //将DeleteSlice中sliceHead=sliceTail，清位
    }

    void reset() {
      // Reset to a 0 length slice
      sliceHead = sliceTail;
    }

    /**
     * Returns <code>true</code> iff the given node is identical to the slices tail, otherwise
     * <code>false</code>.
     */
    boolean isTail(Node<?> node) {
      return sliceTail == node;
    }

    /**
     * Returns <code>true</code> iff the given item is identical to the item hold by the slices
     * tail, otherwise <code>false</code>.
     */
    boolean isTailItem(Object object) {
      return sliceTail.item == object;
    }

    boolean isEmpty() {
      return sliceHead == sliceTail;
    }
  }

  /** For test purposes. */
  int numGlobalTermDeletes() {
    return globalBufferedUpdates.deleteTerms.size();
  }

  void clear() {
    globalBufferLock.lock();
    try {
      final Node<?> currentTail = tail;
      globalSlice.sliceHead = globalSlice.sliceTail = currentTail;
      globalBufferedUpdates.clear();
    } finally {
      globalBufferLock.unlock();
    }
  }

  static class Node<T> {
    volatile Node<?> next;
    final T item;

    Node(T item) {
      this.item = item; // 可以是Term，也可以是Query[1], 可以是DocValuesUpdate[]
    }

    void apply(BufferedUpdates bufferedDeletes, int docIDUpto) {
      throw new IllegalStateException("sentinel item must never be applied");
    }

    boolean isDelete() {
      return true;
    }
  }

  private static final class TermNode extends Node<Term> {

    TermNode(Term term) {
      super(term);
    }
    //这里要重点关注下apply函数，从下面函数实现可以知道其主要工作就是将删除操作放到BufferedUpdates中
    @Override
    void apply(BufferedUpdates bufferedDeletes, int docIDUpto) {
      bufferedDeletes.addTerm(item, docIDUpto);
    }

    @Override
    public String toString() {
      return "del=" + item;
    }
  }

  private static final class QueryNode extends Node<Query> {

    QueryNode(Query query) {
      super(query);
    }

    @Override
    void apply(BufferedUpdates bufferedDeletes, int docIDUpto) {
      bufferedDeletes.addQuery(item, docIDUpto);
    }

    @Override
    public String toString() {
      return "del=" + item;
    }
  }

  private static final class QueryArrayNode extends Node<Query[]> {
    QueryArrayNode(Query[] query) {
      super(query);
    }

    @Override
    void apply(BufferedUpdates bufferedUpdates, int docIDUpto) {
      for (Query query : item) {
        bufferedUpdates.addQuery(query, docIDUpto);
      }
    }
  }

  private static final class TermArrayNode extends Node<Term[]> {
    TermArrayNode(Term[] term) {
      super(term);
    }

    @Override
    void apply(BufferedUpdates bufferedUpdates, int docIDUpto) {
      for (Term term : item) {
        bufferedUpdates.addTerm(term, docIDUpto);
      }
    }

    @Override
    public String toString() {
      return "dels=" + Arrays.toString(item);
    }
  }

  private static final class DocValuesUpdatesNode extends Node<DocValuesUpdate[]> {

    DocValuesUpdatesNode(DocValuesUpdate... updates) {
      super(updates); //   每个DocValuesUpdate都是一条删除记录
    }

    @Override
    void apply(BufferedUpdates bufferedUpdates, int docIDUpto) {
      for (DocValuesUpdate update : item) {
        switch (update.type) {
          case NUMERIC:
            bufferedUpdates.addNumericUpdate((NumericDocValuesUpdate) update, docIDUpto);
            break;
          case BINARY:
            bufferedUpdates.addBinaryUpdate((BinaryDocValuesUpdate) update, docIDUpto);
            break;
          case NONE:
          case SORTED:
          case SORTED_SET:
          case SORTED_NUMERIC:
          default:
            throw new IllegalArgumentException(
                update.type + " DocValues updates not supported yet!");
        }
      }
    }

    @Override
    boolean isDelete() {
      return false;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("docValuesUpdates: ");
      if (item.length > 0) {
        sb.append("term=").append(item[0].term).append("; updates: [");
        for (DocValuesUpdate update : item) {
          sb.append(update.field).append(':').append(update.valueToString()).append(',');
        }
        sb.setCharAt(sb.length() - 1, ']');
      }
      return sb.toString();
    }
  }

  public int getBufferedUpdatesTermsSize() {
    final ReentrantLock lock = globalBufferLock; // Trusted final
    lock.lock();
    try {
      final Node<?> currentTail = tail;
      if (globalSlice.sliceTail != currentTail) {
        globalSlice.sliceTail = currentTail;
        globalSlice.apply(globalBufferedUpdates, BufferedUpdates.MAX_INT);
      }
      return globalBufferedUpdates.deleteTerms.size();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public long ramBytesUsed() {
    return globalBufferedUpdates.ramBytesUsed();
  }

  @Override
  public String toString() {
    return "DWDQ: [ generation: " + generation + " ]";
  }
  // 1.在每个DocumentsWriterPerThread.finishDocuments()调用这个函数，一批文档一个seqNo;2.在deleteTerms时会调用一次。
  public long getNextSequenceNumber() { // 任何add delete, updateSlice都会调用该函数
    long seqNo = nextSeqNo.getAndIncrement();
    assert seqNo <= maxSeqNo : "seqNo=" + seqNo + " vs maxSeqNo=" + maxSeqNo;
    return seqNo;
  }

  long getLastSequenceNumber() {
    return nextSeqNo.get() - 1;
  }

  /**
   * Inserts a gap in the sequence numbers. This is used by IW during flush or commit to ensure any
   * in-flight threads get sequence numbers inside the gap
   */
  void skipSequenceNumbers(long jump) {
    nextSeqNo.addAndGet(jump);
  }

  /** Returns the maximum completed seq no for this queue. */
  long getMaxCompletedSeqNo() {
    if (startSeqNo < nextSeqNo.get()) {
      return getLastSequenceNumber();
    } else {
      // if we haven't advanced the seqNo make sure we fall back to the previous queue
      long value = previousMaxSeqId.getAsLong();
      assert value < startSeqNo : "illegal max sequence ID: " + value + " start was: " + startSeqNo;
      return value;
    }
  }

  // we use a static method to get this lambda since we previously introduced a memory leak since it
  // would
  // implicitly reference this.nextSeqNo which holds on to this del queue. see LUCENE-9478 for
  // reference
  private static LongSupplier getPrevMaxSeqIdSupplier(AtomicLong nextSeqNo) {
    return () -> nextSeqNo.get() - 1;
  }

  /**
   * Advances the queue to the next queue on flush. This carries over the generation to the next
   * queue and set the {@link #getMaxSeqNo()} based on the given maxNumPendingOps. This method can
   * only be called once, subsequently the returned queue should be used.
   *
   * @param maxNumPendingOps the max number of possible concurrent operations that will execute on
   *     this queue after it was advanced. This corresponds to the number of DWPTs that own the
   *     current queue at the moment when this queue is advanced since each these DWPTs can
   *     increment the seqId after we advanced it.
   * @return a new queue as a successor of this queue.
   */ // 当flush时，产生一个新的queue
  synchronized DocumentsWriterDeleteQueue advanceQueue(int maxNumPendingOps) {
    if (advanced) {
      throw new IllegalStateException("queue was already advanced");
    }
    advanced = true;
    long seqNo = getLastSequenceNumber() + maxNumPendingOps + 1;// nextSeq+正在使用的+1
    maxSeqNo = seqNo;
    return new DocumentsWriterDeleteQueue(
        infoStream,
        generation + 1,
        seqNo + 1,
        // don't pass ::getMaxCompletedSeqNo here b/c otherwise we keep an reference to this queue
        // and this will be a memory leak since the queues can't be GCed
        getPrevMaxSeqIdSupplier(nextSeqNo));
  }

  /**
   * Returns the maximum sequence number for this queue. This value will change once this queue is
   * advanced.
   */
  long getMaxSeqNo() {
    return maxSeqNo;
  }

  /** Returns <code>true</code> if it was advanced. */
  synchronized boolean isAdvanced() {
    return advanced;
  }
}
