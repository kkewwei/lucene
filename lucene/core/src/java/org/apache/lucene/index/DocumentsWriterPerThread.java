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
// 当写入完成而不刷新时，该对象会释放到pool中，当触发了flush时，刷新完后会直接丢弃了。
import java.io.IOException;
import java.text.NumberFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.IntConsumer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.column.ColumnBatch;
import org.apache.lucene.index.DocumentsWriterDeleteQueue.DeleteSlice;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FlushInfo;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.TrackingDirectoryWrapper;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.SetOnce;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;

final class DocumentsWriterPerThread implements Accountable, Lock {  // 由ThreadState拥有,// 为了支持多线程并发索引,对每一个线程都有一个 DocumentsWriterThreadState,其为每一个线程根据 DocConsumer consumer 的索引链来创建每个线程的索引链(XXXPerThread),来进行对文档的并发处理。d
// 每写入一个文档时，都有拥有一个该对象，与ThreadState绑定在一起，若不需要刷新时，ThreadState被释放至池中，而DocumentsWriterPerThread不会被释放，若文档&内存超了，才会解绑
  private Throwable abortingException;

  private void onAbortingException(Throwable throwable) {
    assert throwable != null : "aborting exception must not be null";
    assert abortingException == null : "aborting exception has already been set";
    abortingException = throwable;
  }

  final boolean isAborted() {
    return aborted;
  }

  static final class FlushedSegment {
    final SegmentCommitInfo segmentInfo;
    final FieldInfos fieldInfos;
    final FrozenBufferedUpdates segmentUpdates; // 是从DWPT的pendingUpdates转移出来的
    final FixedBitSet liveDocs;
    final Sorter.DocMap sortMap;
    final int delCount;// 硬删除的文档数

    private FlushedSegment(
        InfoStream infoStream,
        SegmentCommitInfo segmentInfo,
        FieldInfos fieldInfos,
        BufferedUpdates segmentUpdates,
        FixedBitSet liveDocs,
        int delCount,//
        Sorter.DocMap sortMap) {
      this.segmentInfo = segmentInfo;
      this.fieldInfos = fieldInfos;
      this.segmentUpdates =
          segmentUpdates != null && segmentUpdates.any()
              ? new FrozenBufferedUpdates(infoStream, segmentUpdates, segmentInfo)
              : null;//作用单个segment,所以segmentInfo不为null
      this.liveDocs = liveDocs;
      this.delCount = delCount;
      this.sortMap = sortMap;
    }
  }

  /**
   * Called if we hit an exception at a bad time (when updating the index files) and must discard
   * all currently buffered docs. This resets our state, discarding any docs added since last flush.
   */
  void abort() throws IOException {
    aborted = true;
    pendingNumDocs.addAndGet(-numDocsInRAM);
    try {
      if (infoStream.isEnabled("DWPT")) {
        infoStream.message("DWPT", "now abort");
      }
      try {
        indexingChain.abort();
      } finally {
        pendingUpdates.clear();
      }
    } finally {
      if (infoStream.isEnabled("DWPT")) {
        infoStream.message("DWPT", "done abort");
      }
    }
  }

  private static final boolean INFO_VERBOSE = false;
  final Codec codec;// Lucene80Codec
  final TrackingDirectoryWrapper directory;//就是index_name/0/index目录
  private final IndexingChain indexingChain;// DefaultIndexingChain, 每个DocumentsWriterPerThread都会专门拥有一个DefaultIndexingChain，随着刷新落盘后。

  // Updates for our still-in-RAM (to be flushed next) segment
  private final BufferedUpdates pendingUpdates; // 快照的是deleteSlice映射的长度。和DocumentsWriterDeleteQueue.globalBufferedUpdates作用一样
  private final SegmentInfo segmentInfo; // Current segment we are working on   我们正在工作的segment。同DocumentsWriterPerThread生命周期一样
  private boolean aborted = false; // True if we aborted
  private SetOnce<Boolean> flushPending = new SetOnce<>();// 等待刷新.比如es索引周期性refresh的话，就将当前正在写入的
  private volatile long lastCommittedBytesUsed;
  private SetOnce<Boolean> hasFlushed = new SetOnce<>();

  private final FieldInfos.Builder fieldInfos;
  private final InfoStream infoStream; // LoggerInfoStream，控制了lucene日志在es中打印
  private int numDocsInRAM;  // 该DWPT在内存中写的数据量，索引结构已经建立。每es flush一次，该对象就新产生一个
  final DocumentsWriterDeleteQueue deleteQueue; // 一个DocuentsWriter会唯一会维持当前对应的一个DocumentsWriterFlushQueue
  private final DeleteSlice deleteSlice; //每个DocumentsWriterPerThread自己拥有的
  private final NumberFormat nf = NumberFormat.getInstance(Locale.ROOT);
  private final AtomicLong pendingNumDocs;
  private final LiveIndexWriterConfig indexWriterConfig;
  private final boolean enableTestPoints;
  private final ReentrantLock lock = new ReentrantLock();//每个DocumentsWriterPerThread被使用的时候，都会被locked。
  private int[] deleteDocIDs = new int[0];// 比如keyword 字段长度大于3万字符。那么会丢弃这条docId
  private int numDeletedDocIds = 0;// 真正需要丢的doc个数
  private final int indexMajorVersionCreated;
  private final boolean hasParentField;

  DocumentsWriterPerThread( // 复用的，一个shard所有的压缩方式都会保持不变,在DocumentsWriter初始化时，会产生DocumentsWriterPerThread构造器
      int indexMajorVersionCreated,
      String segmentName,
      Directory directoryOrig,
      Directory directory,
      LiveIndexWriterConfig indexWriterConfig,
      DocumentsWriterDeleteQueue deleteQueue,
      FieldInfos.Builder fieldInfos,
      AtomicLong pendingNumDocs,
      boolean enableTestPoints) {
    this.indexMajorVersionCreated = indexMajorVersionCreated;
    this.directory = new TrackingDirectoryWrapper(directory);
    this.fieldInfos = fieldInfos;
    this.indexWriterConfig = indexWriterConfig;
    this.infoStream = indexWriterConfig.getInfoStream();
    this.codec = indexWriterConfig.getCodec();// Lucene80Codec
    this.pendingNumDocs = pendingNumDocs;
    pendingUpdates = new BufferedUpdates(segmentName);
    this.deleteQueue = Objects.requireNonNull(deleteQueue);// 传递进来的
    assert numDocsInRAM == 0 : "num docs " + numDocsInRAM;
    deleteSlice = deleteQueue.newSlice();// 每个DeleteSlice都会作为DocumentsWriterDeleteQueue中一个节点存放起来
//  每个DocumentsWriterPerThread都会在刷新时产生一个segment。
    segmentInfo =
        new SegmentInfo(
            directoryOrig,
            Version.LATEST,
            Version.LATEST,
            segmentName,
            -1,
            false,
            false,
            codec,
            Collections.emptyMap(),
            StringHelper.randomId(),
            Collections.emptyMap(),
            indexWriterConfig.getIndexSort());
    assert numDocsInRAM == 0;
    if (INFO_VERBOSE && infoStream.isEnabled("DWPT")) {
      infoStream.message(
          "DWPT",
          Thread.currentThread().getName()
              + " init seg="
              + segmentName
              + " delQueue="
              + deleteQueue);
    }
    this.enableTestPoints = enableTestPoints;
    indexingChain =// getIndexingChain()将返回的是defaultIndexingChain(在该类前面定义的)，获取的是DefaultIndexingChain。每个
        new IndexingChain(
            indexMajorVersionCreated,
            segmentInfo,
            this.directory,
            fieldInfos,
            indexWriterConfig,
            this::onAbortingException);
    this.hasParentField = indexWriterConfig.getParentField() != null;
  }

  final void testPoint(String message) {
    if (enableTestPoints) {
      assert infoStream.isEnabled("TP"); // don't enable unless you need them.
      infoStream.message("TP", message);
    }
  }

  /** Anything that will add N docs to the index should reserve first to make sure it's allowed. */
  private void reserveOneDoc() { // 检查单个segment写入是否超过了上限
    if (pendingNumDocs.incrementAndGet() > IndexWriter.getActualMaxDocs()) { // bug_kw 是否有必要设置pendingNumDocs为AtomicLong，而不是AtomicInteger
      // Reserve failed: put the one doc back and throw exc:
      pendingNumDocs.decrementAndGet();
      throw new IllegalArgumentException(
          "number of documents in the index cannot exceed " + IndexWriter.getActualMaxDocs());
    }
  }

  /**
   * Atomically reserves capacity for {@code n} docs.
   *
   * @throws IllegalArgumentException if reserving {@code n} docs would exceed the maximum number of
   *     documents allowed in the index
   */
  private void reserveDocs(int n) {
    assert n >= 0;
    if (n == 0) {
      return;
    }
    final int maxDocs = IndexWriter.getActualMaxDocs();
    while (true) {
      long current = pendingNumDocs.get();
      long next = current + n;
      if (next > maxDocs) {
        throw new IllegalArgumentException(
            "number of documents in the index cannot exceed " + maxDocs);
      }
      if (pendingNumDocs.compareAndSet(current, next)) {
        return;
      }
    }
  }

  long updateDocuments(
      Iterable<? extends Iterable<? extends IndexableField>> docs,
      DocumentsWriterDeleteQueue.Node<?> deleteNode,
      DocumentsWriter.FlushNotifications flushNotifications,
      Runnable onNewDocOnRAM)
      throws IOException {
    try {
      testPoint("DocumentsWriterPerThread addDocuments start");
      assert abortingException == null : "DWPT has hit aborting exception but is still indexing";
      if (INFO_VERBOSE && infoStream.isEnabled("DWPT")) {
        infoStream.message(
            "DWPT",
            Thread.currentThread().getName()
                + " update delTerm="
                + deleteNode
                + " docID="
                + numDocsInRAM
                + " seg="
                + segmentInfo.name);
      }// 该文档写入之前的文档数，为了删除时只删除该文档写入前已经写入的文档。对之后写入的文档，都不再删除了
      final int docsInRamBefore = numDocsInRAM;// 当前DocumentsWriterPerThread内保存的还没有刷新到磁盘的文档。每次es flush后，会随着DocumentsWriterPerThread一起消失
      boolean allDocsIndexed = false;
      try {
        final Iterator<? extends Iterable<? extends IndexableField>> iterator = docs.iterator();
        while (iterator.hasNext()) {
          Iterable<? extends IndexableField> doc = iterator.next();// es文档删除也会跑进来，也就是delete时,delete不仅会产生一个deleteNode，也会产生一个doc（包含_id, _seq_no, _primary_term, _version:2, _tombstone:1, _soft_delete:1）
          final boolean isLastDoc = iterator.hasNext() == false;
          if (hasParentField == false
              && segmentInfo.getIndexSort() != null
              && isLastDoc == false
              && indexMajorVersionCreated >= Version.LUCENE_10_0_0.major) {
            // sort is configured but parent field is missing, yet we have a doc-block
            // yet we must not fail if this index was created in an earlier version where this
            // behavior was permitted.
            throw new IllegalArgumentException(
                "a parent field must be set in order to use document blocks with index sorting; see IndexWriterConfig#setParentField");
          }

          // Even on exception, the document is still added (but marked
          // deleted), so we don't need to un-reserve at that point.
          // Aborting exceptions will actually "lose" more than one
          // document, so the counter will be "wrong" in that case, but
          // it's very hard to fix (we can't easily distinguish aborting
          // vs non-aborting exceptions):
          reserveOneDoc();// 检查单个segment的shard是否超过上限了
          try {
            indexingChain.processDocument(numDocsInRAM++, doc, isLastDoc);// 每循环一次，就+1
          } finally {
            onNewDocOnRAM.run();
          }
        }
        final int numDocs = numDocsInRAM - docsInRamBefore;
        if (numDocs > 1) {
          segmentInfo.setHasBlocks();
        }
        allDocsIndexed = true;
        return finishDocuments(deleteNode, docsInRamBefore); // 这里就会主动将全局global删除信息放入单个DWPT中（免得之后产生的DWPT带上之前的删除信息）
      } finally {
        if (!allDocsIndexed && !aborted) {
          // the iterator threw an exception that is not aborting
          // go and mark all docs from this block as deleted
          deleteLastDocs(numDocsInRAM - docsInRamBefore); //循环抛出了异常，但是数据并没有丢弃，，于是标记这个文档为delete
        }
      }
    } finally {
      maybeAbort("updateDocuments", flushNotifications);
    }
  }

  long updateBatch(
      ColumnBatch columnBatch,
      DocumentsWriterDeleteQueue.Node<?> deleteNode,
      DocumentsWriter.FlushNotifications flushNotifications,
      IntConsumer onNewDocsOnRAM)
      throws IOException {
    try {
      testPoint("DocumentsWriterPerThread addBatch start");
      assert abortingException == null : "DWPT has hit aborting exception but is still indexing";
      if (INFO_VERBOSE && infoStream.isEnabled("DWPT")) {
        infoStream.message(
            "DWPT",
            Thread.currentThread().getName()
                + " update batch"
                + " docID="
                + numDocsInRAM
                + " seg="
                + segmentInfo.name);
      }
      final int docsInRamBefore = numDocsInRAM;
      final int numDocs = columnBatch.numDocs();
      boolean allDocsIndexed = false;
      // Atomically reserve all doc IDs up front. On failure reserveDocs fully rolls back, so
      // pendingNumDocs is never left holding orphaned reservations. Done before the try/finally
      // because nothing has been accounted for yet — if this throws, there is no cleanup to do.
      reserveDocs(numDocs);
      numDocsInRAM += numDocs;
      onNewDocsOnRAM.accept(numDocs);
      try {
        indexingChain.processBatch(docsInRamBefore, columnBatch);

        // A batch is N independent documents, not a parent-child block, so we deliberately
        // do not call segmentInfo.setHasBlocks() here.
        allDocsIndexed = true;
        return finishDocuments(deleteNode, docsInRamBefore);
      } finally {
        if (!allDocsIndexed && !aborted) {
          // the iterator threw an exception that is not aborting
          // go and mark all docs from this block as deleted
          deleteLastDocs(numDocsInRAM - docsInRamBefore);
        }
      }
    } finally {
      maybeAbort("updateBatch", flushNotifications);
    }
  }

  private long finishDocuments(DocumentsWriterDeleteQueue.Node<?> deleteNode, int docIdUpTo) {// docIdUpTo: 写该文档之前的docId
    /*
     * here we actually finish the document in two steps 1. push the delete into
     * the queue and update our slice. 2. increment the DWPT private document
     * id.
     *
     * the updated slice we get from 1. holds all the deletes that have occurred
     * since we updated the slice the last time.
     */
    // Apply delTerm only after all indexing has
    // succeeded, but apply it only to docs prior to when
    // this batch started:
    long seqNo;
    if (deleteNode != null) {// 有删除,
      seqNo = deleteQueue.add(deleteNode, deleteSlice);// 让本DocumentsWriterPerThread的tail向前移动
      assert deleteSlice.isTail(deleteNode) : "expected the delete term as the tail item";
      deleteSlice.apply(pendingUpdates, docIdUpTo);// 这里的docIdUpTo=docsInRamBefore，可以生效的那批。再运用局部的
      return seqNo;
    } else { // 默认跑到这里
      seqNo = deleteQueue.updateSlice(deleteSlice); //会跑到DocumentsWriterDeleteQueue.getNextSequenceNumber()获取下一个id
      if (seqNo < 0) { // 有删除发生了
        seqNo = -seqNo;
        deleteSlice.apply(pendingUpdates, docIdUpTo);
      } else {
        deleteSlice.reset();
      }
    }

    return seqNo;//返回的一定是正数
  }

  // This method marks the last N docs as deleted. This is used
  // in the case of a non-aborting exception. There are several cases
  // where we fail a document ie. due to an exception during analysis
  // that causes the doc to be rejected but won't cause the DWPT to be
  // stale nor the entire IW to abort and shutdown. In such a case
  // we only mark these docs as deleted and turn it into a livedocs
  // during flush
  private void deleteLastDocs(int docCount) { // 比如keyword 字段长度大于3万字符。那么会丢弃这条
    int from = numDocsInRAM - docCount;
    int to = numDocsInRAM;
    deleteDocIDs = ArrayUtil.grow(deleteDocIDs, numDeletedDocIds + (to - from));
    for (int docId = from; docId < to; docId++) {
      deleteDocIDs[numDeletedDocIds++] = docId;// 需要丢弃的docId
    }
    // NOTE: we do not trigger flush here.  This is
    // potentially a RAM leak, if you have an app that tries
    // to add docs but every single doc always hits a
    // non-aborting exception.  Allowing a flush here gets
    // very messy because we are only invoked when handling
    // exceptions so to do this properly, while handling an
    // exception we'd have to go off and flush new deletes
    // which is risky (likely would hit some other
    // confounding exception).
  }

  /** Returns the number of RAM resident documents in this {@link DocumentsWriterPerThread} */
  public int getNumDocsInRAM() {
    // public for FlushPolicy
    return numDocsInRAM;
  }

  /**
   * Prepares this DWPT for flushing. This method will freeze and return the {@link
   * DocumentsWriterDeleteQueue}s global buffer and apply all pending deletes to this DWPT.
   */
  FrozenBufferedUpdates prepareFlush() {// 在lucene flush前会再次freeze下
    assert numDocsInRAM > 0;// 2. 每个DocumentsWriterPerThread都会进来，会感知最新的删除node
    final FrozenBufferedUpdates globalUpdates = deleteQueue.freezeGlobalBuffer(deleteSlice);// 1。冻结了旧的（全局的），作用于其他段（私有FrozenBufferedUpdates会作用于当前段）
    /* deleteSlice can possibly be null if we have hit non-aborting exceptions during indexing and never succeeded
    adding a document. */
    if (deleteSlice != null) {//但凡该DWPT有一次写入，那么间隔两次写入之间的delete删除文档docIDUpto都会是最合适的。跑到这里的都是前面没有继续新增add的。精确的计数/最大计数是没有太多区别的
      // apply all deletes before we flush and release the delete slice
      deleteSlice.apply(pendingUpdates, numDocsInRAM); // 把deleteSlice中全部转到pendingUpdates中（其实 DocumentsWriterPerThread.pendingUpdates记录的和全局的是一样的）
      assert deleteSlice.isEmpty();//pendingUpdates在后面DocumentsWriterPerThread.flush()里面会生效termDelete部分
      deleteSlice.reset(); // 清空了。只要第一个segment来apply到存量segment就好了，新增的segment都有自己的copy 的FrozenBufferedUpdates
    }
    return globalUpdates;
  }
// 这里进来是因为设置的全局文档个数或者内存大小引起的es refresh刷新（需要产生segment）
  /** Flush all pending docs to a new segment */
  FlushedSegment flush(DocumentsWriter.FlushNotifications flushNotifications) throws IOException {
    assert flushPending.get() == Boolean.TRUE;
    assert numDocsInRAM > 0;
    assert deleteSlice.isEmpty() : "all deletes must be applied in prepareFlush";
    segmentInfo.setMaxDoc(numDocsInRAM);// 设置这个segmentInfo里面存储的文档书
    final SegmentWriteState flushState =
        new SegmentWriteState(
            infoStream,
            directory,
            segmentInfo,
            fieldInfos.finish(),
            pendingUpdates,
            IOContext.flush(new FlushInfo(numDocsInRAM, lastCommittedBytesUsed)));
    final double startMBUsed = lastCommittedBytesUsed / 1024. / 1024.;

    // Apply delete-by-docID now (delete-byDocID only
    // happens when an exception is hit processing that
    // doc, eg if analyzer has some problem w/ the text):
    if (numDeletedDocIds > 0) { // 比如写失败了，需要丢弃的条数
      flushState.liveDocs = new FixedBitSet(numDocsInRAM);
      flushState.liveDocs.set(0, numDocsInRAM);
      for (int i = 0; i < numDeletedDocIds; i++) {// 这些位数先清0
        flushState.liveDocs.clear(deleteDocIDs[i]);
      }
      flushState.delCountOnFlush = numDeletedDocIds;
      deleteDocIDs = new int[0];
    }

    if (aborted) {
      if (infoStream.isEnabled("DWPT")) {
        infoStream.message("DWPT", "flush: skip because aborting is set");
      }
      return null;
    }

    long t0 = System.nanoTime();
    // 会跑LoggerInfoStream中，判断是否为trace
    if (infoStream.isEnabled("DWPT")) {
      infoStream.message(
          "DWPT",
          "flush postings as segment " + flushState.segmentInfo.name + " numDocs=" + numDocsInRAM);
    }
    final Sorter.PackableDocMap packableSortMap;
    try {
      DocIdSetIterator softDeletedDocs;
      if (indexWriterConfig.getSoftDeletesField() != null) {// 是否设置了软删除字段__soft_deletes（比如delete_by_query进来的）
        softDeletedDocs = indexingChain.getHasDocValues(indexWriterConfig.getSoftDeletesField());// 获取文档中包含__soft_deletes的文档
      } else {
        softDeletedDocs = null;// 到这里
      }// flush主要是是这里
      packableSortMap = indexingChain.flush(flushState);// 进去呀，有较大的写入（在落盘前，若有删除，会标志哪些文档不需要删除，将存活文档放入state.liveDocs中，下面会有live文件生成）
      if (softDeletedDocs == null) {
        flushState.softDelCountOnFlush = 0;
      } else {
        flushState.softDelCountOnFlush =// 计算软删除文档数
            PendingSoftDeletes.countSoftDeletes(softDeletedDocs, flushState.liveDocs);
        assert flushState.segmentInfo.maxDoc()
            >= flushState.softDelCountOnFlush + flushState.delCountOnFlush;
      }
      // We clear this here because we already resolved them (private to this segment) when writing
      // postings:
      pendingUpdates.clearDeleteTerms();// 把该DocumentsWriterPerThread上的Term类删除信息清除了，已经在flushState.liveDocs保留了哪些doc不需要删除（写tip、tim时候）
      segmentInfo.setFiles(new HashSet<>(directory.getCreatedFiles())); // 给本segment所有的索引文件加上segment编号

      final SegmentCommitInfo segmentInfoPerCommit =
          new SegmentCommitInfo(
              segmentInfo,
              0,
              flushState.softDelCountOnFlush,
              -1L,
              -1L,
              -1L,
              StringHelper.randomId());
      if (infoStream.isEnabled("DWPT")) {
        infoStream.message(
            "DWPT",
            "new segment has "
                + (flushState.liveDocs == null ? 0 : flushState.delCountOnFlush)
                + " deleted docs");
        infoStream.message(
            "DWPT", "new segment has " + flushState.softDelCountOnFlush + " soft-deleted docs");
        infoStream.message(
            "DWPT",
            "new segment has "
                + (flushState.fieldInfos.hasTermVectors() ? "vectors" : "no vectors")
                + "; "
                + (flushState.fieldInfos.hasNorms() ? "norms" : "no norms")
                + "; "
                + (flushState.fieldInfos.hasDocValues() ? "docValues" : "no docValues")
                + "; "
                + (flushState.fieldInfos.hasProx() ? "prox" : "no prox")
                + "; "
                + (flushState.fieldInfos.hasFreq() ? "freqs" : "no freqs"));
        infoStream.message("DWPT", "flushedFiles=" + segmentInfoPerCommit.files());
        infoStream.message("DWPT", "flushed codec=" + codec);
      }

      final BufferedUpdates segmentDeletes;
      if (pendingUpdates.deleteQueries.isEmpty() && pendingUpdates.numFieldUpdates.get() == 0) {
        pendingUpdates.clear();//检查是否有需要处理的deleteQueries
        segmentDeletes = null;
      } else {
        segmentDeletes = pendingUpdates; // 开始处理需要删除的termQuery和numFieldUpdates
      }

      if (infoStream.isEnabled("DWPT")) {
        final double newSegmentSize = segmentInfoPerCommit.sizeInBytes() / 1024. / 1024.;
        infoStream.message(
            "DWPT",
            "flushed: segment="
                + segmentInfo.name
                + " ramUsed="
                + nf.format(startMBUsed)
                + " MB"
                + " newFlushedSize="
                + nf.format(newSegmentSize)
                + " MB"
                + " docs/MB="
                + nf.format(flushState.segmentInfo.maxDoc() / newSegmentSize));
      }

      assert segmentInfo != null;
// 注意，sealFlushedSegment中有删除操作的落盘（保留liv文档, 不会放入segment复合文件中）
      FlushedSegment fs =
          new FlushedSegment(
              infoStream,
              segmentInfoPerCommit,
              flushState.fieldInfos,
              segmentDeletes,
              flushState.liveDocs,
              flushState.delCountOnFlush,
              packableSortMap != null
                  ? packableSortMap.pack()
                  : null); // Use a packed version as the lifetime of FlushedSegment is long.
      sealFlushedSegment(fs, packableSortMap, flushNotifications);// 1.然后再将产生的单个索引文件合并产复合文件，还并没有mmap打开，在IndexWriter.getReader()中会mmap。 2。会产生liv文件，不再segment复合文件中
      if (infoStream.isEnabled("DWPT")) {
        infoStream.message(
            "DWPT",
            "flush time "
                + ((System.nanoTime() - t0) / (double) TimeUnit.MILLISECONDS.toNanos(1))
                + " ms");
      }
      return fs;
    } catch (Throwable t) {
      onAbortingException(t);
      throw t;
    } finally {
      maybeAbort("flush", flushNotifications);
      hasFlushed.set(Boolean.TRUE);
    }
  }

  private void maybeAbort(String location, DocumentsWriter.FlushNotifications flushNotifications)
      throws IOException {
    if (abortingException != null && aborted == false) {
      // if we are already aborted don't do anything here
      try {
        abort();
      } finally {
        // whatever we do here we have to fire this tragic event up.
        flushNotifications.onTragicEvent(abortingException, location);
      }
    }
  }
  //  准备删除的文件
  private final Set<String> filesToDelete = new HashSet<>(); // 12个索引文件

  Set<String> pendingFilesToDelete() {
    return filesToDelete;
  }

  private FixedBitSet sortLiveDocs(Bits liveDocs, Sorter.DocMap sortMap) {
    assert liveDocs != null && sortMap != null;
    FixedBitSet sortedLiveDocs = new FixedBitSet(liveDocs.length());
    sortedLiveDocs.set(0, liveDocs.length());
    for (int i = 0; i < liveDocs.length(); i++) {
      if (liveDocs.get(i) == false) {
        sortedLiveDocs.clear(sortMap.oldToNew(i));
      }
    }
    return sortedLiveDocs;
  }

  /**
   * Seals the {@link SegmentInfo} for the new flushed segment and persists the deleted documents
   * {@link FixedBitSet}.// 封印这个segment和持久化删除不用的文件
   */// 封闭/封印semgent:将fdt等12个索引文件封装成_n.cfs、_n.cfe、_n.si文件,同时有保存live操作
  void sealFlushedSegment(
      FlushedSegment flushedSegment,
      Sorter.DocMap sortMap,
      DocumentsWriter.FlushNotifications flushNotifications)
      throws IOException {
    assert flushedSegment != null;
    SegmentCommitInfo newSegment = flushedSegment.segmentInfo;
    
    IndexWriter.setDiagnostics(newSegment.info, IndexWriter.SOURCE_FLUSH);

    IOContext context =
        IOContext.flush(new FlushInfo(newSegment.info.maxDoc(), newSegment.sizeInBytes()));//sizeInBytes：每个文件的累加值

    boolean success = false;
    try {

      if (indexWriterConfig.getUseCompoundFile()) {//产生复合文件
        Set<String> originalFiles = newSegment.info.files();
        // TODO: like addIndexes, we are relying on createCompoundFile to successfully cleanup...
        IndexWriter.createCompoundFile(
            infoStream,
            new TrackingDirectoryWrapper(directory),
            newSegment.info,
            context,
            flushNotifications::deleteUnusedFiles);
        filesToDelete.addAll(originalFiles);//放置到需要原始的需要删除的文件
        newSegment.info.setUseCompoundFile(true);
      }

      // Have codec write SegmentInfo.  Must do this after
      // creating CFS so that 1) .si isn't slurped into CFS,
      // and 2) .si reflects useCompoundFile=true change
      // above:
      codec.segmentInfoFormat().write(directory, newSegment.info, context); // 构建_n.si文件名

      // TODO: ideally we would freeze newSegment here!!
      // because any changes after writing the .si will be
      // lost...

      // Must write deleted docs after the CFS so we don't
      // slurp the del file into CFS:
      if (flushedSegment.liveDocs != null) { // 有term类型的删除（比如关闭_soft_delete时候的term删除），那么会写live文件（还不包含query类删除和fieldTerm类删除，比如关闭_soft_delete的term删除）
        final int delCount = flushedSegment.delCount;
        assert delCount > 0;
        if (infoStream.isEnabled("DWPT")) {
          infoStream.message(
              "DWPT",
              "flush: write "
                  + delCount
                  + " deletes gen="
                  + flushedSegment.segmentInfo.getDelGen());
        }

        // TODO: we should prune the segment if it's 100%
        // deleted... but merge will also catch it.

        // TODO: in the NRT case it'd be better to hand
        // this del vector over to the
        // shortly-to-be-opened SegmentReader and let it
        // carry the changes; there's no reason to use
        // filesystem as intermediary here.

        SegmentCommitInfo info = flushedSegment.segmentInfo;
        Codec codec = info.info.getCodec();
        final FixedBitSet bits;
        if (sortMap == null) { // 一般跑到这里
          bits = flushedSegment.liveDocs;
        } else {
          bits = sortLiveDocs(flushedSegment.liveDocs, sortMap);
        }
        codec.liveDocsFormat().writeLiveDocs(bits, directory, info, delCount, context);// 写本地.liv文件
        newSegment.setDelCount(delCount); // 多少硬删除的文档
        newSegment.advanceDelGen(); // 会随机产生一个segment_id
      }

      success = true;
    } finally {
      if (!success) {
        if (infoStream.isEnabled("DWPT")) {
          infoStream.message(
              "DWPT",
              "hit exception creating compound file for newly flushed segment "
                  + newSegment.info.name);
        }
      }
    }
  }

  /** Get current segment info we are writing. */
  SegmentInfo getSegmentInfo() {
    return segmentInfo;
  }

  @Override
  public long ramBytesUsed() { // 主要是lucene在内存中构建索引时使用的bytep[]大小，包括intPool，bytePool
    assert lock.isHeldByCurrentThread();
    return (deleteDocIDs.length * (long) Integer.BYTES)
        + pendingUpdates.ramBytesUsed()
        + indexingChain.ramBytesUsed();
  }

  @Override
  public Collection<Accountable> getChildResources() {
    assert lock.isHeldByCurrentThread();
    return List.of(pendingUpdates, indexingChain);
  }

  @Override
  public String toString() {
    return "DocumentsWriterPerThread [pendingDeletes="
        + pendingUpdates
        + ", segment="
        + segmentInfo.name
        + ", aborted="
        + aborted
        + ", numDocsInRAM="
        + numDocsInRAM
        + ", deleteQueue="
        + deleteQueue
        + ", "
        + numDeletedDocIds
        + " deleted docIds"
        + "]";
  }

  /** Returns true iff this DWPT is marked as flush pending */// 等待刷新()
  boolean isFlushPending() {
    return flushPending.get() == Boolean.TRUE;
  }

  boolean isQueueAdvanced() {
    return deleteQueue.isAdvanced();
  }

  /** Sets this DWPT as flush pending. This can only be set once. */// 内存超了（或者全局设置Fullflush），DWPT则会立马设置该状态
  void setFlushPending() {
    flushPending.set(Boolean.TRUE);
  }

  /**
   * Returns the last committed bytes for this DWPT. This method can be called without acquiring the
   * DWPTs lock.
   */
  long getLastCommittedBytesUsed() {
    return lastCommittedBytesUsed;
  }

  /**
   * Commits the current {@link #ramBytesUsed()} and stores it's value for later reuse. The last
   * committed bytes used can be retrieved via {@link #getLastCommittedBytesUsed()}
   */
  void commitLastBytesUsed(long delta) {
    assert isHeldByCurrentThread();
    assert getCommitLastBytesUsedDelta() == delta : "delta has changed";
    lastCommittedBytesUsed += delta;
  }

  /**
   * Calculates the delta between the last committed bytes used and the currently used ram.
   *
   * @see #commitLastBytesUsed(long)
   * @return the delta between the current {@link #ramBytesUsed()} and the current {@link
   *     #getLastCommittedBytesUsed()}
   */
  long getCommitLastBytesUsedDelta() {
    assert isHeldByCurrentThread();
    long delta = ramBytesUsed() - lastCommittedBytesUsed;
    return delta;
  }

  @Override
  public void lock() {
    lock.lock();// lock就说明这个DocumentsWriterPerThread被正在使用写入
  }

  @Override
  public void lockInterruptibly() throws InterruptedException {
    lock.lockInterruptibly();
  }

  @Override
  public boolean tryLock() {// 可能被别的线程使用了还没释放（等待刷新时，也会释放了锁）
    return lock.tryLock();
  }

  @Override
  public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
    return lock.tryLock(time, unit);
  }

  /**
   * Returns true if the DWPT's lock is held by the current thread
   *
   * @see ReentrantLock#isHeldByCurrentThread()
   */
  boolean isHeldByCurrentThread() {
    return lock.isHeldByCurrentThread();
  }

  @Override
  public void unlock() {
    lock.unlock();
  }

  @Override
  public Condition newCondition() {
    throw new UnsupportedOperationException();
  }

  /** Returns <code>true</code> iff this DWPT has been flushed */
  boolean hasFlushed() {
    return hasFlushed.get() == Boolean.TRUE;
  }
}
