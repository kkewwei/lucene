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
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOSupplier;
// mergePolicy=ElasticsearchMergePolicy(ShuffleForcedMergePolicy(RecoverySourcePruneMergePolicy(SoftDeletesRetentionMergePolicy(PrunePostingsMergePolicy(EsTieredMergePolicy([TieredMergePolicy: maxMergeAtOnce=10, maxMergeAtOnceExplicit=30, maxMergedSegmentMB=5120.0, floorSegmentMB=2.0, forceMergeDeletesPctAllowed=10.0, segmentsPerTier=10.0, maxCFSSegmentSizeMB=8.796093022207999E12, noCFSRatio=0.1, deletesPctAllowed=33.0))))))， MergeTrigger=FULL_FLUSH
/**
 * This {@link MergePolicy} allows to carry over soft deleted documents across merges. The policy
 * wraps the merge reader and marks documents as "live" that have a value in the soft delete field
 * and match the provided query. This allows for instance to keep documents alive based on time or
 * any other constraint in the index. The main purpose for this merge policy is to implement
 * retention policies for document modification to vanish in the index. Using this merge policy
 * allows to control when soft deletes are claimed by merges.
 *
 * @lucene.experimental
 */
public final class SoftDeletesRetentionMergePolicy extends OneMergeWrappingMergePolicy {
  private final String field;
  private final Supplier<Query> retentionQuerySupplier;// 实际是 SoftDeletesPolicy::getRetentionQuery

  /**
   * Creates a new {@link SoftDeletesRetentionMergePolicy}
   *
   * @param field the soft deletes field
   * @param retentionQuerySupplier a query supplier for the retention query
   * @param in the wrapped MergePolicy
   */
  public SoftDeletesRetentionMergePolicy(
      String field, Supplier<Query> retentionQuerySupplier, MergePolicy in) {
    super(
        in,
        toWrap ->
            new MergePolicy.OneMerge(toWrap.segments) {
              @Override
              public CodecReader wrapForMerge(CodecReader reader) throws IOException {
                CodecReader wrapped = toWrap.wrapForMerge(reader);
                Bits liveDocs = reader.getLiveDocs();
                if (liveDocs == null) { // no deletes - just keep going
                  return wrapped;
                }
                return applyRetentionQuery(field, retentionQuerySupplier.get(), wrapped);// 将大于minRetainedSeqNo的文档也放入存活列表
              }
            });
    Objects.requireNonNull(field, "field must not be null");
    Objects.requireNonNull(retentionQuerySupplier, "retentionQuerySupplier must not be null");
    this.field = field;
    this.retentionQuerySupplier = retentionQuerySupplier;// 是SoftDeletesPolicy.getRetentionQuery()
  }

  @Override
  public boolean keepFullyDeletedSegment(IOSupplier<CodecReader> readerIOSupplier)
      throws IOException {
    CodecReader reader = readerIOSupplier.get();
    /* we only need a single hit to keep it no need for soft deletes to be checked*/
    Scorer scorer =
        getScorer(
            retentionQuerySupplier.get(),
            FilterCodecReader.wrapLiveDocs(reader, null, reader.maxDoc()));
    if (scorer != null) {
      DocIdSetIterator iterator = scorer.iterator();
      boolean atLeastOneHit = iterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS;// 只要有一个不需要删除，那么也不能测试删除
      return atLeastOneHit;// 那么就需要保存
    }
    return super.keepFullyDeletedSegment(readerIOSupplier);
  }

  // pkg private for testing
  static CodecReader applyRetentionQuery(
      String softDeleteField, Query retentionQuery, CodecReader reader) throws IOException {
    Bits liveDocs = reader.getLiveDocs();
    if (liveDocs == null) { // no deletes - just keep going
      return reader;
    }
    CodecReader wrappedReader =
        FilterCodecReader.wrapLiveDocs(
            reader,
            new Bits() { // only search deleted
              @Override
              public boolean get(int index) {
                return liveDocs.get(index) == false;
              }

              @Override
              public int length() {
                return liveDocs.length();
              }
            },
            reader.maxDoc() - reader.numDocs());
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    builder.add(new FieldExistsQuery(softDeleteField), BooleanClause.Occur.FILTER);//查找包含_soft_delete字段
    builder.add(retentionQuery, BooleanClause.Occur.FILTER);// 查找seqno大于minRetainedSeqNo的文档
    Scorer scorer = getScorer(builder.build(), wrappedReader);
    if (scorer != null) {
      FixedBitSet cloneLiveDocs = FixedBitSet.copyOf(liveDocs);// 真正存活+（seqno大于minRetainedSeqNo待删除）的文档
      DocIdSetIterator iterator = scorer.iterator();
      int numExtraLiveDocs = 0;
      while (iterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {// 遍历每个大于Retention的软删除文档，放入live中。
        if (cloneLiveDocs.getAndSet(iterator.docID()) == false) {//不在存活列表中，那么假装放到存活列表中
          // if we bring one back to live we need to account for it
          numExtraLiveDocs++;// 但是不在存活列表中，又在minRetainedSeqNo中的文档，那么就不能删除
        }// 说明软删除的doc，也被从live中迁移走了
      }
      assert reader.numDocs() + numExtraLiveDocs <= reader.maxDoc()
          : "numDocs: "
              + reader.numDocs()
              + " numExtraLiveDocs: "
              + numExtraLiveDocs
              + " maxDoc: "
              + reader.maxDoc();
      return FilterCodecReader.wrapLiveDocs(
          reader, cloneLiveDocs, reader.numDocs() + numExtraLiveDocs);
    } else {
      return reader;
    }
  }

  private static Scorer getScorer(Query query, CodecReader reader) throws IOException {
    IndexSearcher s = new IndexSearcher(reader);
    s.setQueryCache(null);
    Weight weight = s.createWeight(s.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
    return weight.scorer(reader.getContext());
  }
  // 这里统计numDeletesToMerge非常耗时间
  @Override
  public int numDeletesToMerge(// live文件中完全是纯粹活着的。软删除是已经删除的
      SegmentCommitInfo info, int delCount, IOSupplier<CodecReader> readerSupplier)
      throws IOException {
    final int numDeletesToMerge = super.numDeletesToMerge(info, delCount, readerSupplier);
    if (numDeletesToMerge != 0 && info.getSoftDelCount() > 0) {//这个标记有软删除SegmentCommitInfo
      final CodecReader reader = readerSupplier.get();// live文件中的软删除的文件
      if (reader.getLiveDocs() != null) {// SegmentReader，里面存放的是真正存活的doc(删除了软删除了)
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(new FieldExistsQuery(field), BooleanClause.Occur.FILTER); //包含__soft_deletes的字段
        builder.add(retentionQuerySupplier.get(), BooleanClause.Occur.FILTER);//过滤_seq_no大于minRetainedSeqNo的docId
        Scorer scorer =
            getScorer(
                builder.build(), FilterCodecReader.wrapLiveDocs(reader, null, reader.maxDoc()));// Segmetn删除认为为null
        if (scorer != null) {
          DocIdSetIterator iterator = scorer.iterator();
          Bits liveDocs = reader.getLiveDocs();//里面存放的是真正存活的doc(删除了软删除了)
          int numDeletedDocs = reader.numDeletedDocs();
          while (iterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            if (liveDocs.get(iterator.docID()) == false) {//不在存活列表的，但是大于retentionLease列表的，也认为为存活
              numDeletedDocs--;
            }
          }
          return numDeletedDocs;// 又要去统计多少不再live中的，也不能删除
        }
      }
    }
    assert numDeletesToMerge >= 0 : "numDeletesToMerge: " + numDeletesToMerge;
    assert numDeletesToMerge <= info.info.maxDoc()
        : "numDeletesToMerge: " + numDeletesToMerge + " maxDoc:" + info.info.maxDoc();
    return numDeletesToMerge;
  }
}
