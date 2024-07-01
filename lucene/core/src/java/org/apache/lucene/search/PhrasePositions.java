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
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;

/** Position of a term in a document that takes into account the term offset within the phrase. */
final class PhrasePositions {
  int position; // position in doc    注意，这个position是我们前面说的PhrasePos，也就是文档中的pos - offset
  int count; // remaining pos in this doc     term剩余的匹配位置个数
  int offset; // position in phrase term在PhraseQuery中position    // PhraseQuery中的第几个term组
  final int ord; // unique across all PhrasePositions instances    PhraseQuery中的第几个term组
  final PostingsEnum postings; // stream of docs & positions  // term的倒排
  PhrasePositions next; // used to make lists 指向下一个  PhrasePositions
  int rptGroup = -1; // >=0 indicates that this is a repeating PP      rptGroup >= 0表示当前position的term集合和其他position的term集合有重叠，  有重叠的PhrasePositions属于同一组，rptGroup标识当前PhrasePositions的组号
  int rptInd; // index in the rptGroup    一个组的PhrasePositions是一个数组，rptInd表示PhrasePositions在组中的下标
  final Term[] terms; // for repetitions initialization   当前position的term集合
  int freq; // cached frequency for the current document

  PhrasePositions(PostingsEnum postings, int o, int ord, Term[] terms) {
    this.postings = postings;
    offset = o;
    this.ord = ord;
    this.terms = terms;
  }

  final void firstPosition() throws IOException {
    count = freq; // use cached frequency
    nextPosition();
  }

  /**
   * Go to next location of this term current document, and set <code>position</code> as <code>
   * location - offset</code>, so that a matching exact phrase is easily identified when all
   * PhrasePositions have exactly the same <code>position</code>.
   */
  final boolean nextPosition() throws IOException {
    if (count-- > 0) { // read subsequent pos's
      position = postings.nextPosition() - offset; //  这个position是我们前面说的PhrasePos，也就是文档中的pos - offset
      return true;
    } else {
      return false;
    }
  }

  /** for debug purposes */
  @Override
  public String toString() {
    String s = "o:" + offset + " p:" + position + " c:" + count;
    if (rptGroup >= 0) {
      s += " rpt:" + rptGroup + ",i" + rptInd;
    }
    return s;
  }
}
