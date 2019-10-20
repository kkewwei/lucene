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
package org.apache.lucene.search;


import java.io.IOException;
import java.util.Collection;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.MaxScoreAccumulator.DocAndScore;

/**
 * A {@link Collector} implementation that collects the top-scoring hits,
 * returning them as a {@link TopDocs}. This is used by {@link IndexSearcher} to
 * implement {@link TopDocs}-based search. Hits are sorted by score descending
 * and then (when the scores are tied) docID ascending. When you create an
 * instance of this collector you should know in advance whether documents are
 * going to be collected in doc Id order or not.
 *
 * <p><b>NOTE</b>: The values {@link Float#NaN} and
 * {@link Float#NEGATIVE_INFINITY} are not valid scores.  This
 * collector will not properly collect hits with such
 * scores.
 */ // TopScoreDocCollector 类的排序规则为 “先打分，后文档号”
public abstract class TopScoreDocCollector extends TopDocsCollector<ScoreDoc> {

  abstract static class ScorerLeafCollector implements LeafCollector {

    Scorable scorer;

    @Override
    public void setScorer(Scorable scorer) throws IOException {
      this.scorer = scorer; // ConstantScoreScorer
    }
  }
  //使用优先级队列 PriorityQueue 来存放满足搜索条件的文档信息（文档信息至少包含了文档打分 score 以及文档号 docId），分数最低的文档信息位于堆顶，堆的大小默认为段中的文档总数（用户也可以指定堆的大小，即用户期望的返回结果 TopN 的 N 值）。
  private static class SimpleTopScoreDocCollector extends TopScoreDocCollector {
     // 通过堆顶替换，来保持最大堆
    SimpleTopScoreDocCollector(int numHits, HitsThresholdChecker hitsThresholdChecker,
                               MaxScoreAccumulator minScoreAcc) {
      super(numHits, hitsThresholdChecker, minScoreAcc);
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      // reset the minimum competitive score
      docBase = context.docBase;
      return new ScorerLeafCollector() {

        @Override
        public void setScorer(Scorable scorer) throws IOException {
          super.setScorer(scorer);
          minCompetitiveScore = 0f;
          updateMinCompetitiveScore(scorer);
          if (minScoreAcc != null) {
            updateGlobalMinCompetitiveScore(scorer);
          }
        }
        // 开始遍历选中的文档，然后进行得分统计。
        @Override
        public void collect(int doc) throws IOException {
          float score = scorer.score();

          // This collector relies on the fact that scorers produce positive values:
          assert score >= 0; // NOTE: false for NaN
          //
          totalHits++;
          hitsThresholdChecker.incrementHitCount();

          if (minScoreAcc != null && (totalHits & minScoreAcc.modInterval) == 0) {
            updateGlobalMinCompetitiveScore(scorer);
          }
          // 得分最小的放在最上面
          if (score <= pqTop.score) {// 小于等于顶置。目前使用全部score为1，达到我们期望的值后，
            if (totalHitsRelation == TotalHits.Relation.EQUAL_TO) {
              // we just reached totalHitsThreshold, we can start setting the min
              // competitive score now
              updateMinCompetitiveScore(scorer);
            }
            // Since docs are returned in-order (i.e., increasing doc Id), a document
            // with equal score to pqTop.score cannot compete since HitQueue favors
            // documents with lower doc Ids. Therefore reject those docs too.
            return;
          }
          pqTop.doc = doc + docBase; //
          pqTop.score = score;
          pqTop = pq.updateTop(); // 重新更新堆顶元素
          updateMinCompetitiveScore(scorer); // 应该是调整
        }

      };
    }
  }

  private static class PagingTopScoreDocCollector extends TopScoreDocCollector {

    private final ScoreDoc after;
    private int collectedHits;

    PagingTopScoreDocCollector(int numHits, ScoreDoc after, HitsThresholdChecker hitsThresholdChecker,
                               MaxScoreAccumulator minScoreAcc) {
      super(numHits, hitsThresholdChecker, minScoreAcc);
      this.after = after;
      this.collectedHits = 0;
    }

    @Override
    protected int topDocsSize() {
      return collectedHits < pq.size() ? collectedHits : pq.size();
    }

    @Override
    protected TopDocs newTopDocs(ScoreDoc[] results, int start) {
      return results == null
          ? new TopDocs(new TotalHits(totalHits, totalHitsRelation), new ScoreDoc[0])
          : new TopDocs(new TotalHits(totalHits, totalHitsRelation), results);
    }


    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      docBase = context.docBase;
      final int afterDoc = after.doc - context.docBase;

      return new ScorerLeafCollector() {
        @Override
        public void collect(int doc) throws IOException {
          float score = scorer.score();

          // This collector relies on the fact that scorers produce positive values:
          assert score >= 0; // NOTE: false for NaN

          totalHits++;
          hitsThresholdChecker.incrementHitCount();

          if (minScoreAcc != null && (totalHits & minScoreAcc.modInterval) == 0) {
            updateGlobalMinCompetitiveScore(scorer);
          }

          if (score > after.score || (score == after.score && doc <= afterDoc)) {
            // hit was collected on a previous page
            if (totalHitsRelation == TotalHits.Relation.EQUAL_TO) {
              // we just reached totalHitsThreshold, we can start setting the min
              // competitive score now
              updateMinCompetitiveScore(scorer);
            }
            return;
          }

          if (score <= pqTop.score) {
            if (totalHitsRelation == TotalHits.Relation.EQUAL_TO) {
              // we just reached totalHitsThreshold, we can start setting the min
              // competitive score now
              updateMinCompetitiveScore(scorer);
            }

            // Since docs are returned in-order (i.e., increasing doc Id), a document
            // with equal score to pqTop.score cannot compete since HitQueue favors
            // documents with lower doc Ids. Therefore reject those docs too.
            return;
          }
          collectedHits++;
          pqTop.doc = doc + docBase;
          pqTop.score = score;
          pqTop = pq.updateTop();
          updateMinCompetitiveScore(scorer);
        }
      };
    }
  }

  /**
   * Creates a new {@link TopScoreDocCollector} given the number of hits to
   * collect and the number of hits to count accurately.
   *
   * <p><b>NOTE</b>: If the total hit count of the top docs is less than or exactly
   * {@code totalHitsThreshold} then this value is accurate. On the other hand,
   * if the {@link TopDocs#totalHits} value is greater than {@code totalHitsThreshold}
   * then its value is a lower bound of the hit count. A value of {@link Integer#MAX_VALUE}
   * will make the hit count accurate but will also likely make query processing slower.
   * <p><b>NOTE</b>: The instances returned by this method
   * pre-allocate a full array of length
   * <code>numHits</code>, and fill the array with sentinel
   * objects.
   */
  public static TopScoreDocCollector create(int numHits, int totalHitsThreshold) {
    return create(numHits, null, totalHitsThreshold);
  }

  /**
   * Creates a new {@link TopScoreDocCollector} given the number of hits to
   * collect, the bottom of the previous page, and the number of hits to count
   * accurately.
   *
   * <p><b>NOTE</b>: If the total hit count of the top docs is less than or exactly
   * {@code totalHitsThreshold} then this value is accurate. On the other hand,
   * if the {@link TopDocs#totalHits} value is greater than {@code totalHitsThreshold}
   * then its value is a lower bound of the hit count. A value of {@link Integer#MAX_VALUE}
   * will make the hit count accurate but will also likely make query processing slower.
   * <p><b>NOTE</b>: The instances returned by this method
   * pre-allocate a full array of length
   * <code>numHits</code>, and fill the array with sentinel
   * objects.
   */
  public static TopScoreDocCollector create(int numHits, ScoreDoc after, int totalHitsThreshold) {
    return create(numHits, after, HitsThresholdChecker.create(Math.max(totalHitsThreshold, numHits)), null);
  }

  static TopScoreDocCollector create(int numHits, ScoreDoc after, HitsThresholdChecker hitsThresholdChecker,
                                     MaxScoreAccumulator minScoreAcc) {

    if (numHits <= 0) { // 就是每次需要返回的个数
      throw new IllegalArgumentException("numHits must be > 0; please use TotalHitCountCollector if you just need the total hit count");
    }

    if (hitsThresholdChecker == null) {
      throw new IllegalArgumentException("hitsThresholdChecker must be non null");
    }

    if (after == null) { // ES跑到这里
      return new SimpleTopScoreDocCollector(numHits, hitsThresholdChecker, minScoreAcc);
    } else {
      return new PagingTopScoreDocCollector(numHits, after, hitsThresholdChecker, minScoreAcc);
    }
  }

  /**
   * Create a CollectorManager which uses a shared hit counter to maintain number of hits
   * and a shared {@link MaxScoreAccumulator} to propagate the minimum score accross segments
   */
  public static CollectorManager<TopScoreDocCollector, TopDocs> createSharedManager(int numHits, FieldDoc after,
                                                                                      int totalHitsThreshold) {
    return new CollectorManager<TopScoreDocCollector, TopDocs>() {

      private final HitsThresholdChecker hitsThresholdChecker = HitsThresholdChecker.createShared(Math.max(totalHitsThreshold, numHits));
      private final MaxScoreAccumulator minScoreAcc = new MaxScoreAccumulator();

      @Override
      public TopScoreDocCollector newCollector() throws IOException {
        return TopScoreDocCollector.create(numHits, after, hitsThresholdChecker, minScoreAcc);
      }

      @Override
      public TopDocs reduce(Collection<TopScoreDocCollector> collectors) throws IOException {
        final TopDocs[] topDocs = new TopDocs[collectors.size()];
        int i = 0;
        for (TopScoreDocCollector collector : collectors) {
          topDocs[i++] = collector.topDocs();
        }
        return TopDocs.merge(numHits, topDocs);
      }

    };
  }

  int docBase; // 这个叶子的起始文档
  ScoreDoc pqTop;
  final HitsThresholdChecker hitsThresholdChecker;
  final MaxScoreAccumulator minScoreAcc;
  float minCompetitiveScore;

  // prevents instantiation
  TopScoreDocCollector(int numHits, HitsThresholdChecker hitsThresholdChecker,
                       MaxScoreAccumulator minScoreAcc) {
    super(new HitQueue(numHits, true)); // numHits:每次需要返回的个数
    assert hitsThresholdChecker != null;

    // HitQueue implements getSentinelObject to return a ScoreDoc, so we know
    // that at this point top() is already initialized.
    pqTop = pq.top();// qp优先级队列就是由new HitQueue构成，返回的就是堆顶元素
    this.hitsThresholdChecker = hitsThresholdChecker;
    this.minScoreAcc = minScoreAcc;
  }

  @Override
  protected TopDocs newTopDocs(ScoreDoc[] results, int start) {
    if (results == null) {
      return EMPTY_TOPDOCS;
    }

    return new TopDocs(new TotalHits(totalHits, totalHitsRelation), results);
  }

  @Override
  public ScoreMode scoreMode() {
    return hitsThresholdChecker.scoreMode();
  }

  protected void updateGlobalMinCompetitiveScore(Scorable scorer) throws IOException {
    assert minScoreAcc != null;
    DocAndScore maxMinScore = minScoreAcc.get();
    if (maxMinScore != null) {
      // since we tie-break on doc id and collect in doc id order we can require
      // the next float if the global minimum score is set on a document id that is
      // smaller than the ids in the current leaf
      float score = docBase > maxMinScore.docID ? Math.nextUp(maxMinScore.score) : maxMinScore.score;
      if (score > minCompetitiveScore) {
        assert hitsThresholdChecker.isThresholdReached();
        scorer.setMinCompetitiveScore(score);
        minCompetitiveScore = score;
        totalHitsRelation = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
      }
    }
  }

  protected void updateMinCompetitiveScore(Scorable scorer) throws IOException {
    if (hitsThresholdChecker.isThresholdReached() // 统计当前计算得分的文档个数是否超过10000个文档
          && pqTop != null
          && pqTop.score != Float.NEGATIVE_INFINITY) { // -Infinity is the score of sentinels
      // since we tie-break on doc id and collect in doc id order, we can require
      // the next float
      float localMinScore = Math.nextUp(pqTop.score);
      if (localMinScore > minCompetitiveScore) {
        scorer.setMinCompetitiveScore(localMinScore);
        totalHitsRelation = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
        minCompetitiveScore = localMinScore;
        if (minScoreAcc != null) {
          // we don't use the next float but we register the document
          // id so that other leaves can require it if they are after
          // the current maximum
          minScoreAcc.accumulate(pqTop.doc, pqTop.score);
        }
      }
    }
  }
}
