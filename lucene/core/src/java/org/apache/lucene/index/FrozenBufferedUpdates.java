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
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.IntConsumer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Holds buffered deletes and updates by term or query, once pushed. Pushed deletes/updates are
 * write-once, so we shift to more memory efficient data structure to hold them. We don't hold
 * docIDs because these are applied on flush.
 */// 是每个DWPT在进行flush时，对自己持有pendingUpdates做的一个快照。
final class FrozenBufferedUpdates {

  /* NOTE: we now apply this frozen packet immediately on creation, yet this process is heavy, and runs
   * in multiple threads, and this compression is sizable (~8.3% of the original size), so it's important
   * we run this before applying the deletes/updates. */

  /* Query we often undercount (say 24 bytes), plus int. */
  static final int BYTES_PER_DEL_QUERY =
      RamUsageEstimator.NUM_BYTES_OBJECT_REF + Integer.BYTES + 24;

  // Terms, in sorted order:
  final PrefixCodedTerms deleteTerms; // 转变为了字节流， deleteTerms也有deleteGen

  // Parallel array of deleted query, and the docIDUpto for each
  final Query[] deleteQueries;
  final int[] deleteQueryLimits;

  /** Counts down once all deletes/updates have been applied */
  public final CountDownLatch applied = new CountDownLatch(1);//若为0则表示已经apply过deletes/updates

  private final ReentrantLock applyLock = new ReentrantLock();
  private final Map<String, FieldUpdatesBuffer> fieldUpdates;// 对es来说，key只有一个，那就是__soft_deletes

  /** How many total documents were deleted/updated. */
  public long totalDelCount;

  private final int fieldUpdatesCount;

  final int bytesUsed;
  // 删除时，只是针对segState.delGen低于此值的。大于着删除对这个无效
  private long delGen = -1; // assigned by BufferedUpdatesStream once pushed
  // 每个DWPT都有一个FrozenBufferedUpdates，在写入刷盘前，已经通过内存结构将TermDelete给完成了，那么后面的TermDelete就可以不再执行了
  final SegmentCommitInfo privateSegment; // non-null iff this frozen packet represents
  // a segment private deletes. in that case is should
  // only have Queries and doc values updates
  private final InfoStream infoStream;
  // 第一次调用Thread.prepareFlush()刷新globalSlice时也会产生该对象
  public FrozenBufferedUpdates(
      InfoStream infoStream, BufferedUpdates updates, SegmentCommitInfo privateSegment) {
    this.infoStream = infoStream;
    this.privateSegment = privateSegment;//若privateSegment为null，说明该FrozenBufferedUpdates是作用全局剩余segment的
    assert privateSegment == null || updates.deleteTerms.isEmpty()
        : "segment private packet should only have del queries";

    PrefixCodedTerms.Builder builder = new PrefixCodedTerms.Builder();
    updates.deleteTerms.forEachOrdered((term, doc) -> builder.add(term));// 多个temr压缩存储
    deleteTerms = builder.finish();// 转变为了字节流

    deleteQueries = new Query[updates.deleteQueries.size()];// 可以存放TermQuery
    deleteQueryLimits = new int[updates.deleteQueries.size()];
    int upto = 0;
    for (Map.Entry<Query, Integer> ent : updates.deleteQueries.entrySet()) {
      deleteQueries[upto] = ent.getKey();
      deleteQueryLimits[upto] = ent.getValue();
      upto++;
    }
    // TODO if a Term affects multiple fields, we could keep the updates key'd by Term
    // so that it maps to all fields it affects, sorted by their docUpto, and traverse
    // that Term only once, applying the update to all fields that still need to be
    // updated.
    updates.fieldUpdates.values().forEach(FieldUpdatesBuffer::finish);// es只有fieldUpdate
    this.fieldUpdates = Map.copyOf(updates.fieldUpdates);
    this.fieldUpdatesCount = updates.numFieldUpdates.get();// 多少个文档被更新了

    bytesUsed =
        (int)
            ((deleteTerms.ramBytesUsed() + deleteQueries.length * (long) BYTES_PER_DEL_QUERY)
                + updates.fieldUpdatesBytesUsed.get());

    if (infoStream != null && infoStream.isEnabled("BD")) {
      infoStream.message(
          "BD",
          String.format(
              Locale.ROOT,
              "compressed %d to %d bytes (%.2f%%) for deletes/updates; private segment %s",
              updates.ramBytesUsed(),
              bytesUsed,
              100. * bytesUsed / updates.ramBytesUsed(),
              privateSegment));
    }
  }

  /**
   * Tries to lock this buffered update instance
   *
   * @return true if the lock was successfully acquired. otherwise false.
   */
  boolean tryLock() {
    return applyLock.tryLock();
  }

  /** locks this buffered update instance */
  void lock() {
    applyLock.lock();
  }

  /** Releases the lock of this buffered update instance */
  void unlock() {
    applyLock.unlock();
  }

  /** Returns true iff this buffered updates instance was already applied */
  boolean isApplied() {
    assert applyLock.isHeldByCurrentThread();
    return applied.getCount() == 0;
  }

  /**
   * Applies pending delete-by-term, delete-by-query and doc values updates to all segments in the
   * index, returning the number of new deleted or updated documents.
   */
  long apply(BufferedUpdatesStream.SegmentState[] segStates) throws IOException {
    assert applyLock.isHeldByCurrentThread();
    if (delGen == -1) {
      // we were not yet pushed
      throw new IllegalArgumentException(
          "gen is not yet set; call BufferedUpdatesStream.push first");
    }

    assert applied.getCount() != 0;

    if (privateSegment != null) {
      assert segStates.length == 1;
      assert privateSegment == segStates[0].reader.getOriginalSegmentInfo();
    }
    // 若是FlushedSegment中的updates，则不会包含term删除，因为已经在DWPT刷盘中删除了。
    totalDelCount += applyTermDeletes(segStates); // 存量semgent termDelete删除。不是__soft_deletes删除的term就进来
    totalDelCount += applyQueryDeletes(segStates); // 处理Query删除。query删除是在索引结构刷盘后才去删除，内存结构不太适合做query操作
    totalDelCount += applyDocValuesUpdates(segStates); // 处理的是一个segment的软删除，仅仅是将软删除文档缓存到内存变量DocValuesFieldUpdates中。真正发生的写dvd和dvm落盘，以及open是在IndexWriter中 prepareCommitInternal中的writeReaderPool

    return totalDelCount;
  }
    // 处理的是一个segment的软删除，仅仅是将软删除文档缓存到内存变量DocValuesFieldUpdates中。真正发生的写dvd和dvm落盘，以及open是在IndexWriter中 prepareCommitInternal中的writeReaderPool
  private long applyDocValuesUpdates(BufferedUpdatesStream.SegmentState[] segStates)
      throws IOException {

    if (fieldUpdates.isEmpty()) {
      return 0;
    }

    long startNS = System.nanoTime();

    long updateCount = 0;

    for (BufferedUpdatesStream.SegmentState segState : segStates) {// 遍历每个匹配的segment

      if (delGen < segState.delGen) {
        // segment is newer than this deletes packet
        continue;
      }

      if (segState.rld.refCount() == 1) {
        // This means we are the only remaining reference to this segment, meaning
        // it was merged away while we were running, so we can safely skip running
        // because we will run on the newly merged segment next:
        continue;
      }
      final boolean isSegmentPrivateDeletes = privateSegment != null;
      if (fieldUpdates.isEmpty() == false) {// 有软删除更新
        updateCount +=
            applyDocValuesUpdates(segState, fieldUpdates, delGen, isSegmentPrivateDeletes);// 主要计算被软删除的docId
      }
    }

    if (infoStream.isEnabled("BD")) {
      infoStream.message(
          "BD",
          String.format(
              Locale.ROOT,
              "applyDocValuesUpdates %.1f msec for %d segments, %d field updates; %d new updates",
              (System.nanoTime() - startNS) / (double) TimeUnit.MILLISECONDS.toNanos(1),
              segStates.length,
              fieldUpdatesCount,
              updateCount));
    }

    return updateCount;
  }

  private static long applyDocValuesUpdates(// 这里处理的是一个segment
      BufferedUpdatesStream.SegmentState segState,
      Map<String, FieldUpdatesBuffer> updates,
      long delGen,
      boolean segmentPrivateDeletes)
      throws IOException {

    // TODO: we can process the updates per DV field, from last to first so that
    // if multiple terms affect same document for the same field, we add an update
    // only once (that of the last term). To do that, we can keep a bitset which
    // marks which documents have already been updated. So e.g. if term T1
    // updates doc 7, and then we process term T2 and it updates doc 7 as well,
    // we don't apply the update since we know T1 came last and therefore wins
    // the update.
    // We can also use that bitset as 'liveDocs' to pass to TermEnum.docs(), so
    // that these documents aren't even returned.

    long updateCount = 0;// 这个segment总共更新的文档个数

    // We first write all our updates private, and only in the end publish to the ReadersAndUpdates
    // */
    final List<DocValuesFieldUpdates> resolvedUpdates = new ArrayList<>();
    for (Map.Entry<String, FieldUpdatesBuffer> fieldUpdate : updates.entrySet()) {// 对es来说，updates中只有一个值，那就是key是 __soft_deletes
      String updateField = fieldUpdate.getKey();// 是__soft_deletes
      DocValuesFieldUpdates dvUpdates = null; // 就将表示一个segment内的__soft_deletes:1的所有删除。核心
      FieldUpdatesBuffer value = fieldUpdate.getValue();
      boolean isNumeric = value.isNumeric();
      FieldUpdatesBuffer.BufferedUpdateIterator iterator = value.iterator();
      FieldUpdatesBuffer.BufferedUpdate bufferedUpdate;
      TermDocsIterator termDocsIterator =
          new TermDocsIterator(segState.reader, iterator.isSortedTerms());
      while ((bufferedUpdate = iterator.next()) != null) { // 对一个semgnt，遍历所有的变更（每个主键的更新）
        // TODO: we traverse the terms in update order (not term order) so that we
        // apply the updates in the correct order, i.e. if two terms update the
        // same document, the last one that came in wins, irrespective of the
        // terms lexical order.
        // we can apply the updates in terms order if we keep an updatesGen (and
        // increment it with every update) and attach it to each NumericUpdate. Note
        // that we cannot rely only on docIDUpto because an app may send two updates
        // which will get same docIDUpto, yet will still need to respect the order
        // those updates arrived.
        // TODO: we could at least *collate* by field?
        final DocIdSetIterator docIdSetIterator =// 获取这个terms的所有文档id列表。针对delete场景，field=_id，那么docIdlist只能有一个值
            termDocsIterator.nextTerm(bufferedUpdate.termField, bufferedUpdate.termValue);
        if (docIdSetIterator != null) {
          final int limit;
          if (delGen == segState.delGen) {// 若一样，就是私有segment删除，则docUpTo有效
            assert segmentPrivateDeletes;
            limit = bufferedUpdate.docUpTo;
          } else {
            limit = Integer.MAX_VALUE;
          }
          final BytesRef binaryValue;
          final long longValue;//longValue始终为1
          if (bufferedUpdate.hasValue == false) {
            longValue = -1;
            binaryValue = null;
          } else {
            longValue = bufferedUpdate.numericValue;
            binaryValue = bufferedUpdate.binaryValue;
          }
          if (dvUpdates == null) {// 一般进来
            if (isNumeric) {
              if (value.hasSingleValue()) {// 产生一个新的字段DocValuesFieldUpdates
                dvUpdates =//一般进来
                    new NumericDocValuesFieldUpdates.SingleValueNumericDocValuesFieldUpdates(
                        delGen, updateField, segState.reader.maxDoc(), value.getNumericValue(0));
              } else {
                dvUpdates =
                    new NumericDocValuesFieldUpdates(
                        delGen,
                        updateField,
                        value.getMinNumeric(),
                        value.getMaxNumeric(),
                        segState.reader.maxDoc());
              }
            } else {
              dvUpdates =
                  new BinaryDocValuesFieldUpdates(delGen, updateField, segState.reader.maxDoc());
            }
            resolvedUpdates.add(dvUpdates);// 核心，存起来，存放了新的待软删除掉的文档
          }
          final IntConsumer docIdConsumer;
          final DocValuesFieldUpdates update = dvUpdates;
          if (bufferedUpdate.hasValue == false) {
            docIdConsumer = doc -> update.reset(doc);
          } else if (isNumeric) {// 进来
            docIdConsumer = doc -> update.add(doc, longValue);// 会记录软删除的文档，longValue始终为1
          } else {
            docIdConsumer = doc -> update.add(doc, binaryValue);
          }
          final Bits acceptDocs = segState.rld.getLiveDocs();// 真正存活的文档（磁盘中live文件-软删除的doc）
          if (segState.rld.sortMap != null && segmentPrivateDeletes) {// 仅作用于本segment
            // This segment was sorted on flush; we must apply seg-private deletes carefully in this
            // case:
            int doc;
            while ((doc = docIdSetIterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {// 遍历匹配的主键_id的所有文档
              if (acceptDocs == null || acceptDocs.get(doc)) {// 这个文档时存活的
                // The limit is in the pre-sorted doc space:
                if (segState.rld.sortMap.newToOld(doc) < limit) {//那么就说明是需要被删除的
                  docIdConsumer.accept(doc); // 可以被删除，跑到 SingleValueDocValuesFieldUpdates.add
                  updateCount++;
                }
              }
            }
          } else {
            int doc;
            while ((doc = docIdSetIterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {// 遍历这个软删除条件匹配的所有的文档
              if (doc >= limit) {//过滤掉不能被删除的文档
                break; // no more docs that can be updated for this term
              }
              if (acceptDocs == null || acceptDocs.get(doc)) {// 看是否在live文件中，在的话，从live中删掉
                docIdConsumer.accept(doc);//将软删除的doc记录在update中
                updateCount++;
              }
            }
          }
        }
      }
    }
    // 处理的是一个segment的软删除，仅仅缓存到内存变量DocValuesFieldUpdates中。真正发生的写dvd和dvm落盘，以及open是在IndexWriter中 prepareCommitInternal中的writeReaderPool
    // now freeze & publish:
    for (DocValuesFieldUpdates update : resolvedUpdates) {
      if (update.any()) {
        update.finish();
        segState.rld.addDVUpdate(update);// 缓存到内存的 DocValuesFieldUpdates中
      }
    }

    return updateCount;
  }

  // Delete by query
  private long applyQueryDeletes(BufferedUpdatesStream.SegmentState[] segStates)
      throws IOException {

    if (deleteQueries.length == 0) {
      return 0;
    }

    long startNS = System.nanoTime();

    long delCount = 0;
    for (BufferedUpdatesStream.SegmentState segState : segStates) {// 
      
      if (delGen < segState.delGen) {
        // segment is newer than this deletes packet
        continue;
      }

      if (segState.rld.refCount() == 1) {
        // This means we are the only remaining reference to this segment, meaning
        // it was merged away while we were running, so we can safely skip running
        // because we will run on the newly merged segment next:
        continue;
      }

      final LeafReaderContext readerContext = segState.reader.getContext();
      for (int i = 0; i < deleteQueries.length; i++) { // 遍历每个deleteQuery
        Query query = deleteQueries[i];
        int limit;
        if (delGen == segState.delGen) {//是新产生的segment
          assert privateSegment != null;
          limit = deleteQueryLimits[i];
        } else {// 小于delGen
          limit = Integer.MAX_VALUE;
        }
        final IndexSearcher searcher = new IndexSearcher(readerContext.reader());// 单个segment的查询器
        searcher.setQueryCache(null);// 不需要queryCache
        query = searcher.rewrite(query); // 实际就是查询删除
        final Weight weight = searcher.createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 1); //TermQuery$TermWeight
        final Scorer scorer = weight.scorer(readerContext);//进行真正的条件查询
        if (scorer != null) {// 有文档匹配上
          final DocIdSetIterator it = scorer.iterator();
          if (segState.rld.sortMap != null && limit != Integer.MAX_VALUE) {
            assert privateSegment != null;
            // This segment was sorted on flush; we must apply seg-private deletes carefully in this
            // case:
            int docID;
            while ((docID = it.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
              // The limit is in the pre-sorted doc space:
              if (segState.rld.sortMap.newToOld(docID) < limit) {
                if (segState.rld.delete(docID)) {
                  delCount++;
                }
              }
            }
          } else {// 一般都跑到这里了
            int docID;
            while ((docID = it.nextDoc()) < limit) { // 只要doc小于阈值，全部删除
              if (segState.rld.delete(docID)) {// 
                delCount++;
              }
            }
          }
        }
      }
    }

    if (infoStream.isEnabled("BD")) {
      infoStream.message(
          "BD",
          String.format(
              Locale.ROOT,
              "applyQueryDeletes took %.2f msec for %d segments and %d queries; %d new deletions",
              (System.nanoTime() - startNS) / (double) TimeUnit.MILLISECONDS.toNanos(1),
              segStates.length,
              deleteQueries.length,
              delCount));
    }

    return delCount;
  }
// 对某个segment进行termDelete删除
  private long applyTermDeletes(BufferedUpdatesStream.SegmentState[] segStates) throws IOException {

    if (deleteTerms.size() == 0) {
      return 0;
    }

    // We apply segment-private deletes on flush:
    assert privateSegment == null;

    long startNS = System.nanoTime();

    long delCount = 0;// 存量segment的deleteTerm删除

    for (BufferedUpdatesStream.SegmentState segState : segStates) {// 循环每个segment
      assert segState.delGen != delGen
          : "segState.delGen=" + segState.delGen + " vs this.gen=" + delGen;
      if (segState.delGen > delGen) {// 大于目标
        // our deletes don't apply to this segment
        continue;
      }
      if (segState.rld.refCount() == 1) { // 已经没人引用了，已经被merged了，就放弃了。rld是专门获取的
        // This means we are the only remaining reference to this segment, meaning
        // it was merged away while we were running, so we can safely skip running
        // because we will run on the newly merged segment next:
        continue;
      }

      FieldTermIterator iter = deleteTerms.iterator();
      BytesRef delTerm;
      TermDocsIterator termDocsIterator = new TermDocsIterator(segState.reader, true);
      while ((delTerm = iter.next()) != null) {
        final DocIdSetIterator iterator = termDocsIterator.nextTerm(iter.field(), delTerm); // 找到包含字段delTerm的所有文档
        if (iterator != null) {
          int docID;
          while ((docID = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) { // 遍历每一个文档，然后删除掉
            // NOTE: there is no limit check on the docID
            // when deleting by Term (unlike by Query)
            // because on flush we apply all Term deletes to
            // each segment.  So all Term deleting here is
            // against prior segments:
            if (segState.rld.delete(docID)) { // 该删除会面向所有的文档
              delCount++;
            }
          }
        }
      }
    }

    if (infoStream.isEnabled("BD")) {
      infoStream.message(
          "BD",
          String.format(
              Locale.ROOT,
              "applyTermDeletes took %.2f msec for %d segments and %d del terms; %d new deletions",
              (System.nanoTime() - startNS) / (double) TimeUnit.MILLISECONDS.toNanos(1),
              segStates.length,
              deleteTerms.size(),
              delCount));
    }

    return delCount;
  }

  public void setDelGen(long delGen) {// 必须为-1，只能设置一次
    assert this.delGen == -1 : "delGen was already previously set to " + this.delGen;
    this.delGen = delGen;
    deleteTerms.setDelGen(delGen);// 仅仅对deleteTerms设置了delGen
  }

  public long delGen() {
    assert delGen != -1;
    return delGen;
  }

  @Override
  public String toString() {
    String s = "delGen=" + delGen;
    if (deleteTerms.size() != 0) {
      s += " unique deleteTerms=" + deleteTerms.size();
    }
    if (deleteQueries.length != 0) {
      s += " numDeleteQueries=" + deleteQueries.length;
    }
    if (fieldUpdates.size() > 0) {
      s += " fieldUpdates=" + fieldUpdatesCount;
    }
    if (bytesUsed != 0) {
      s += " bytesUsed=" + bytesUsed;
    }
    if (privateSegment != null) {
      s += " privateSegment=" + privateSegment;
    }

    return s;
  }

  boolean any() {
    return deleteTerms.size() > 0 || deleteQueries.length > 0 || fieldUpdatesCount > 0;
  }

  /**
   * This class helps iterating a term dictionary and consuming all the docs for each terms. It
   * accepts a field, value tuple and returns a {@link DocIdSetIterator} if the field has an entry
   * for the given value. It has an optimized way of iterating the term dictionary if the terms are
   * passed in sorted order and makes sure terms and postings are reused as much as possible.
   */
  static final class TermDocsIterator {
    private final TermsProvider provider; // 会跑到FreqProxFields$terms
    private String field;
    private TermsEnum termsEnum;// 
    private PostingsEnum postingsEnum;
    private final boolean sortedTerms; // 该字段所有term已经根据字母大小排好序了
    private BytesRef readerTerm; // 当前读取到的词
    private BytesRef lastTerm; // only set with asserts

    @FunctionalInterface
    interface TermsProvider {
      Terms terms(String field) throws IOException;
    }

    TermDocsIterator(Fields fields, boolean sortedTerms) {
      this(fields::terms, sortedTerms);// terms()= FreqProxFields$FreqProxTerms
    }

    TermDocsIterator(LeafReader reader, boolean sortedTerms) {
      this(reader::terms, sortedTerms);
    }

    private TermDocsIterator(TermsProvider provider, boolean sortedTerms) {
      this.sortedTerms = sortedTerms;
      this.provider = provider;
    }

    private void setField(String field) throws IOException {
      if (this.field == null || this.field.equals(field) == false) {
        this.field = field;

        Terms terms = provider.terms(field); // 找对对应需要删除的term，terms=FreqProxFields$FreqProxTerms
        if (terms != null) {
          termsEnum = terms.iterator(); // termsEnum=FreqProxFields$FreqProxTermsEnum
          if (sortedTerms) {    //   对待更新的主键值进行排序
            // need to reset otherwise we fail the assertSorted below since we sort per field
            assert (lastTerm = null) == null;
            readerTerm = termsEnum.next();// 读取到的第一个具体的词
          }
        } else {
          termsEnum = null;
        }
      }
    }
    // 查找field字段，valueterm的docId集合
    DocIdSetIterator nextTerm(String field, BytesRef term) throws IOException {
      setField(field); // 首先设置了field，读取到第一个词。看是否需要变更档案的fst结构（换了个字段）
      if (termsEnum != null) {
        if (sortedTerms && !termsEnum.preferSeekExact()) {
          assert assertSorted(term);
          // in the sorted case we can take advantage of the "seeking forward" property
          // this allows us depending on the term dict impl to reuse data-structures internally
          // which speed up iteration over terms and docs significantly.
          int cmp = term.compareTo(readerTerm); // 相比排序后的第一个terem，都小于这个term
          if (cmp < 0) {
            return null; // requested term does not exist in this segment
          } else if (cmp == 0) {
            return getDocs(); // 找到这个文档了
          } else {// 定位这个sort值
            TermsEnum.SeekStatus status = termsEnum.seekCeil(term);// 将跑到 FreqProxFields$FreqProxTermsEnum.seekCeil()
            switch (status) {
              case FOUND:
                return getDocs();
              case NOT_FOUND:
                readerTerm = termsEnum.term();
                return null;
              case END:
                // no more terms in this segment
                termsEnum = null;
                return null;
              default:
                throw new AssertionError("unknown status");
            }
          }
        } else if (termsEnum.seekExact(term)) {
          return getDocs();
        }
      }
      return null;
    }

    private boolean assertSorted(BytesRef term) {
      assert sortedTerms;
      assert lastTerm == null || term.compareTo(lastTerm) >= 0
          : "boom: " + term.utf8ToString() + " last: " + lastTerm.utf8ToString();
      lastTerm = BytesRef.deepCopyOf(term);
      return true;
    }

    private DocIdSetIterator getDocs() throws IOException {
      assert termsEnum != null;
      return postingsEnum = termsEnum.postings(postingsEnum, PostingsEnum.NONE);// 进入FreqProxFields$FreqProxTermsEnum.postings
    }
  }
}
