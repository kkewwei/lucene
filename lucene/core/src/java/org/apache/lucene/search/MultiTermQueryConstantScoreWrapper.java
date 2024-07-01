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
import java.util.List;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermStates;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.DocIdSetBuilder;

/**
 * This class provides the functionality behind {@link MultiTermQuery#CONSTANT_SCORE_REWRITE}. It
 * tries to rewrite per-segment as a boolean query that returns a constant score and otherwise fills
 * a bit set with matches and builds a Scorer on top of this bit set.
 */
final class MultiTermQueryConstantScoreWrapper<Q extends MultiTermQuery>
    extends AbstractMultiTermQueryConstantScoreWrapper<Q> {

  MultiTermQueryConstantScoreWrapper(Q query) {
    super(query);
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    return new RewritingWeight(query, boost, scoreMode, searcher) {
      // 在TermInSetQuery.createWeight()中同样的实现。
      @Override
      protected WeightOrDocIdSetIterator rewriteInner(// multiTerms查询中，是在创建scorerSupplier阶段就会调用。是在leader与follow过滤之前。
          LeafReaderContext context,
          int fieldDocCount,
          Terms terms,
          TermsEnum termsEnum,
          List<TermAndState> collectedTerms,
          long leadCost)
          throws IOException {
        DocIdSetBuilder builder = new DocIdSetBuilder(context.reader().maxDoc(), terms);
        PostingsEnum docs = null;

        // Handle the already-collected terms:
        if (collectedTerms.isEmpty() == false) {
          TermsEnum termsEnum2 = terms.iterator();
          for (TermAndState t : collectedTerms) {/// 转化成实际的terms，然后进行should匹配
            termsEnum2.seekExact(t.term, t.state);
            docs = termsEnum2.postings(docs, PostingsEnum.NONE);
            builder.add(docs);
          }
        }

        // Then keep filling the bit set with remaining terms:
        do {//剩余词继续构建bitset
          docs = termsEnum.postings(docs, PostingsEnum.NONE);
          // If a term contains all docs with a value for the specified field, we can discard the
          // other terms and just use the dense term's postings:
          int docFreq = termsEnum.docFreq();
          if (fieldDocCount == docFreq) {
            TermStates termStates = new TermStates(searcher.getTopReaderContext());
            termStates.register(
                termsEnum.termState(), context.ord, docFreq, termsEnum.totalTermFreq());
            Query q =
                new ConstantScoreQuery(
                    new TermQuery(new Term(query.field, termsEnum.term()), termStates));
            Weight weight = searcher.rewrite(q).createWeight(searcher, scoreMode, score());
            return new WeightOrDocIdSetIterator(weight);
          }
          builder.add(docs);// 这里直接将所有文档都收集起来，性能非常差
        } while (termsEnum.next() != null);

        return new WeightOrDocIdSetIterator(builder.build().iterator());
      }
    };
  }
}
