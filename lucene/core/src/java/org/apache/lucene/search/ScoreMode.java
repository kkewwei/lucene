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

/** Different modes of search. */
public enum ScoreMode {

  /** Produced scorers will allow visiting all matches and get their score. */
  COMPLETE(true, true),// complete,  则需要为所有候选文档都进行评分

  /** Produced scorers will allow visiting all matches but scores won't be available. */
  COMPLETE_NO_SCORES(true, false),//complete_no_scores,  完全遍历但是不打分

  /**
   * Produced scorers will optionally allow skipping over non-competitive hits using the {@link
   * Scorer#setMinCompetitiveScore(float)} API.
   */
  TOP_SCORES(false, true),//最常用的方式，即按文档得分取查询结果集的TopK

  /**
   * ScoreMode for top field collectors that can provide their own iterators, to optionally allow to
   * skip for non-competitive docs
   */
  TOP_DOCS(false, false),// 不需要反问全量数据，不需要打分

  /**
   * ScoreMode for top field collectors that can provide their own iterators, to optionally allow to
   * skip for non-competitive docs. This mode is used when there is a secondary sort by _score.
   */
  TOP_DOCS_WITH_SCORES(false, true);

  private final boolean needsScores;
  private final boolean isExhaustive;

  ScoreMode(boolean isExhaustive, boolean needsScores) {
    this.isExhaustive = isExhaustive;
    this.needsScores = needsScores;
  }

  /** Whether this {@link ScoreMode} needs to compute scores. */
  public boolean needsScores() {
    return needsScores;
  }

  /**
   * Returns {@code true} if for this {@link ScoreMode} it is necessary to process all documents, or
   * {@code false} if is enough to go through top documents only.
   */
  public boolean isExhaustive() {
    return isExhaustive;
  }
}
