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
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.LeafReaderContext;

/**
 * A query that uses either an index structure (points or terms) or doc values in order to run a
 * query, depending which one is more efficient. This is typically useful for range queries, whose
 * {@link Weight#scorer} is costly to create since it usually needs to sort large lists of doc ids.
 * For instance, for a field that both indexed {@link LongPoint}s and {@link
 * SortedNumericDocValuesField}s with the same values, an efficient range query could be created by
 * doing:
 *
 * <pre class="prettyprint"> // 一个查询既可以使用pointQuery来查询，也可以使用dvQuery来查询，如果Range的代价小，可以用来引领合并过程，就走PointRangeQuery，直接构造bitset来进行迭代------ 如果range的代价高，构造bitset太慢，就使用SortedSetDocValuesRangeQuery，利用DocValues的全局docID序，并包含每个docid对应value的数据结构来做文档的匹配
 *   String field;
 *   long minValue, maxValue;
 *   Query pointQuery = LongPoint.newRangeQuery(field, minValue, maxValue);
 *   Query dvQuery = SortedNumericDocValuesField.newSlowRangeQuery(field, minValue, maxValue);
 *   Query query = new IndexOrDocValuesQuery(pointQuery, dvQuery);
 * </pre>
 *
 * The above query will be efficient as it will use points in the case that they perform better, ie.
 * when we need a good lead iterator that will be almost entirely consumed; and doc values
 * otherwise, ie. in the case that another part of the query is already leading iteration but we
 * still need the ability to verify that some documents match.
 *
 * <p>Some field types that work well with {@link IndexOrDocValuesQuery} are {@link
 * org.apache.lucene.document.IntField}, {@link org.apache.lucene.document.LongField}, {@link
 * org.apache.lucene.document.FloatField}, {@link org.apache.lucene.document.DoubleField}, and
 * {@link org.apache.lucene.document.KeywordField}. These fields provide both an indexed structure
 * and doc values.
 *
 * <p><b>NOTE</b>This query currently only works well with point range/exact queries and their
 * equivalent doc values queries.
 *
 * @lucene.experimental
 */ // 只要设置为keyword的话，都会默认创建DocValue字段（可参考KeywordFieldMapper和NumberFieldMapper）
public final class IndexOrDocValuesQuery extends Query {
 // 为什么会设计IndexOrDocValuesQuery，可以看下这篇文档https://www.amazingkoala.com.cn/Lucene/Search/2021/0701/196.html
  private final Query indexQuery, dvQuery; //可以分别是LongPoint$1，SortedNumericDocValuesRangeQuery$1

  /**
   * Create an {@link IndexOrDocValuesQuery}. Both provided queries must match the same documents
   * and give the same scores.
   *
   * @param indexQuery a query that has a good iterator but whose scorer may be costly to create
   * @param dvQuery a query whose scorer is cheap to create that can quickly check whether a given
   *     document matches
   */
  public IndexOrDocValuesQuery(Query indexQuery, Query dvQuery) {
    this.indexQuery = indexQuery;
    this.dvQuery = dvQuery;
  }

  /** Return the wrapped query that may be costly to initialize but has a good iterator. */
  public Query getIndexQuery() {
    return indexQuery;
  }

  /**
   * Return the wrapped query that may be slow at identifying all matching documents, but which is
   * cheap to initialize and can efficiently verify that some documents match.
   */
  public Query getRandomAccessQuery() {
    return dvQuery;
  }

  @Override
  public String toString(String field) {
    return "IndexOrDocValuesQuery(indexQuery="
        + indexQuery.toString(field)
        + ", dvQuery="
        + dvQuery.toString(field)
        + ")";
  }

  @Override
  public boolean equals(Object obj) {
    if (sameClassAs(obj) == false) {
      return false;
    }
    IndexOrDocValuesQuery that = (IndexOrDocValuesQuery) obj;
    return indexQuery.equals(that.indexQuery) && dvQuery.equals(that.dvQuery);
  }

  @Override
  public int hashCode() {
    int h = classHash();
    h = 31 * h + indexQuery.hashCode();
    h = 31 * h + dvQuery.hashCode();
    return h;
  }
  //range的会变成2部分
  @Override
  public Query rewrite(IndexSearcher indexSearcher) throws IOException {
    Query indexRewrite = indexQuery.rewrite(indexSearcher);// indexQuery可以是LongPoint$1
    Query dvRewrite = dvQuery.rewrite(indexSearcher);// dvQuery可以是SortedNumericDocValuesField$1
    if (indexRewrite.getClass() == MatchAllDocsQuery.class
        || dvRewrite.getClass() == MatchAllDocsQuery.class) {
      return MatchAllDocsQuery.INSTANCE;
    }
    if (indexRewrite.getClass() == MatchNoDocsQuery.class
        || dvRewrite.getClass() == MatchNoDocsQuery.class) {
      return MatchNoDocsQuery.INSTANCE;
    }
    if (indexQuery != indexRewrite || dvQuery != dvRewrite) {
      return new IndexOrDocValuesQuery(indexRewrite, dvRewrite);
    }
    return this;
  }

  @Override
  public void visit(QueryVisitor visitor) {
    QueryVisitor v = visitor.getSubVisitor(BooleanClause.Occur.MUST, this);
    indexQuery.visit(v);
    dvQuery.visit(v);
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    final Weight indexWeight = indexQuery.createWeight(searcher, scoreMode, boost); // indexWeight=PointRangeQuery$1,   indexQuery=LatLonPoint$1或者fst里面查找。 terms：[xxxx]
    final Weight dvWeight = dvQuery.createWeight(searcher, scoreMode, boost);// dvWeight=LatLonDocValuesBoxQuery$1 , dvQuery=LatLonDocValuesBoxQuery   或者在dvd里面找
    return new Weight(this) {
      @Override
      public Matches matches(LeafReaderContext context, int doc) throws IOException {// 单个文档的匹配，使用docValue更合适，而不是二叉树遍历。批量处理，使用Point处理更合适
        // We need to check a single doc, so the dv query should perform better
        return dvWeight.matches(context, doc);
      }

      @Override
      public Explanation explain(LeafReaderContext context, int doc) throws IOException {
        // We need to check a single doc, so the dv query should perform better
        return dvWeight.explain(context, doc);
      }

      @Override
      public int count(LeafReaderContext context) throws IOException {
        final int count = indexWeight.count(context);
        if (count != -1) {
          return count;
        }
        return dvWeight.count(context);
      }

      @Override
      public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        final ScorerSupplier indexScorerSupplier = indexWeight.scorerSupplier(context);//更适合少量匹配的文档  在fst里面找
        final ScorerSupplier dvScorerSupplier = dvWeight.scorerSupplier(context);  // 在dvd里面查找
        if (indexScorerSupplier == null || dvScorerSupplier == null) {
          return null;
        }
        return new ScorerSupplier() {
          @Override // 看起来是不始终不会跑这里的
          public Scorer get(long leadCost) throws IOException {
            // At equal costs, doc values tend to be worse than points since they 若cost相同，
            // still need to perform one comparison per document while points can
            // do much better than that given how values are organized. So we give
            // an arbitrary 8x penalty to doc values.
            final long threshold = cost() >>> 3; // docValue需要逐个比对，成本更大，故docValue增加8倍并发
            if (threshold <= leadCost) {
              return indexScorerSupplier.get(leadCost);
            } else { // 若leader就是indexScorerSupplier，那么肯定跑到indexScorerSupplier中了。若leader不是indexScorerSupplier，dv最多leadCost次对比，若indexScorerSupplier本来就小，就直接选择indexScorerSupplier了
              return dvScorerSupplier.get(leadCost);
            }
          }

          @Override
          public BulkScorer bulkScorer() throws IOException {
            // Bulk scorers need to consume the entire set of docs, so using an
            // index structure should perform better
            return indexScorerSupplier.bulkScorer();// 需要消耗所有的文档，所以只能indexScorerSupplier。比如query只有一个number 的term查询，没有leader，我们不可能在用dvScorerSupplier全量匹配
          }

          @Override
          public long cost() {
            return indexScorerSupplier.cost();// point的预估耗时
          }
        };
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        // Both index and dv query should return the same values, so we can use
        // the index query's cachehelper here
        return indexWeight.isCacheable(ctx);
      }
    };
  }
}
