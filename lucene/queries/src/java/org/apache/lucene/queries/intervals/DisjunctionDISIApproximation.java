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
package org.apache.lucene.queries.intervals;

import java.io.IOException;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.FixedBitSet;

/**
 * A {@link DocIdSetIterator} which is a disjunction of the approximations of the provided
 * iterators.
 *
 * @lucene.internal
 */
class DisjunctionDISIApproximation extends DocIdSetIterator {

  final DisiPriorityQueue subIterators;
  final long cost;

  public DisjunctionDISIApproximation(DisiPriorityQueue subIterators) {
    this.subIterators = subIterators;
    long cost = 0;
    for (DisiWrapper w : subIterators) {
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
    DisiWrapper top = subIterators.top();
    final int doc = top.doc;
    do {
      top.doc = top.approximation.nextDoc();
      top = subIterators.updateTop();
    } while (top.doc == doc);

    return top.doc;
  }

  @Override
  public int advance(int target) throws IOException {
    DisiWrapper top = subIterators.top();
    do {
      top.doc = top.approximation.advance(target);
      top = subIterators.updateTop();
    } while (top.doc < target);

    return top.doc;
  }

  @Override
  public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
    // Disjunction over approximations: load each sub-iterator's matches in bulk into the same
    // bitSet. Stale entries with doc < upTo will repeatedly load (and overwrite the same bits)
    // until every sub-iterator has advanced past upTo, mirroring the core implementation.
    DisiWrapper top = subIterators.top();
    while (top.doc < upTo) {
      top.approximation.intoBitSet(upTo, bitSet, offset);
      top.doc = top.approximation.docID();
      top = subIterators.updateTop();
    }
  }

  @Override
  public int docIDRunEnd() throws IOException {
    // Only consider sub-iterators that are positioned on the current doc; their runs lower-bound
    // the disjunction's run.
    int curDoc = docID();
    int maxRunEnd = curDoc + 1;
    for (DisiWrapper w : subIterators) {
      if (w.doc == curDoc) {
        maxRunEnd = Math.max(maxRunEnd, w.approximation.docIDRunEnd());
      }
    }
    return maxRunEnd;
  }
}
