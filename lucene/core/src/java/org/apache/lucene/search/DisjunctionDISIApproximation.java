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

/**
 * A {@link DocIdSetIterator} which is a disjunction of the approximations of
 * the provided iterators.
 * @lucene.internal
 */
public class DisjunctionDISIApproximation extends DocIdSetIterator {

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
      top.doc = top.approximation.nextDoc(); // 更换subIterators上栈顶找到的下一个文档
      top = subIterators.updateTop(); // 对subIterators根据当前查找的文档Id进行排序
    } while (top.doc == doc); // 要是相同的话，一般都是有问题的，。

    return top.doc;
  }

  @Override
  public int advance(int target) throws IOException { 
    DisiWrapper top = subIterators.top(); // 取出第一个
    do {
      top.doc = top.approximation.advance(target); // 求取下一个docId
      top = subIterators.updateTop(); // 再次取出新的
    } while (top.doc < target);// 找一个doc，使 top.doc < target

    return top.doc;
  }
}


