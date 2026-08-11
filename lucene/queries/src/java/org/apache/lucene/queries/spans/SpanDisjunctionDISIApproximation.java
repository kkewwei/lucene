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
package org.apache.lucene.queries.spans;

import java.io.IOException;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.FixedBitSet;

/**
 * A {@link DocIdSetIterator} which is a disjunction of the approximations of the provided
 * iterators.
 *
 * @lucene.internal
 */
class SpanDisjunctionDISIApproximation extends DocIdSetIterator {

  final SpanDisiPriorityQueue subIterators;
  final long cost;

  public SpanDisjunctionDISIApproximation(SpanDisiPriorityQueue subIterators) {
    this.subIterators = subIterators;
    long cost = 0;
    for (SpanDisiWrapper w : subIterators) {
      cost += w.cost;
    }
    this.cost = cost;
  }

  @Override
  public long cost() {
    return cost;
  }

  @Override
  public int docID() {
    return subIterators.top().doc;
  }

  @Override
  public int nextDoc() throws IOException {
    SpanDisiWrapper top = subIterators.top();
    final int doc = top.doc;
    do {
      top.doc = top.approximation.nextDoc();
      top = subIterators.updateTop();
    } while (top.doc == doc);

    return top.doc;
  }

  @Override
  public int advance(int target) throws IOException {
    SpanDisiWrapper top = subIterators.top();
    do {
      top.doc = top.approximation.advance(target);
      top = subIterators.updateTop();
    } while (top.doc < target);

    return top.doc;
  }

  @Override
  public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
    // 把每个 sub-iterator 直至 upTo 之前的命中批量写入同一个 bitSet。
    // 仍位于 doc < upTo 的 entries 会反复触发 intoBitSet，直到所有 sub 都越过 upTo。
    SpanDisiWrapper top = subIterators.top();
    while (top.doc < upTo) {
      top.approximation.intoBitSet(upTo, bitSet, offset);
      top.doc = top.approximation.docID();
      top = subIterators.updateTop();
    }
  }

  @Override
  public int docIDRunEnd() throws IOException {
    // 只看停在当前 doc 上的 sub-iterator，它们的 run 给出整个 disjunction run 的下界。
    int curDoc = docID();
    int maxRunEnd = curDoc + 1;
    for (SpanDisiWrapper w : subIterators) {
      if (w.doc == curDoc) {
        maxRunEnd = Math.max(maxRunEnd, w.approximation.docIDRunEnd());
      }
    }
    return maxRunEnd;
  }
}
