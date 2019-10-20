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
package org.apache.lucene.codecs.lucene50;


import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.lucene.codecs.CompetitiveImpactAccumulator;
import org.apache.lucene.codecs.MultiLevelSkipListWriter;
import org.apache.lucene.index.Impact;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMOutputStream;

/**
 * Write skip lists with multiple levels, and support skip within block ints.
 *
 * Assume that docFreq = 28, skipInterval = blockSize = 12
 *
 *  |       block#0       | |      block#1        | |vInts|
 *  d d d d d d d d d d d d d d d d d d d d d d d d d d d d (posting list)
 *                          ^                       ^       (level 0 skip point)
 *
 * Note that skipWriter will ignore first document in block#0, since 
 * it is useless as a skip point.  Also, we'll never skip into the vInts
 * block, only record skip data at the start its start point(if it exist).
 *
 * For each skip point, we will record: 
 * 1. docID in former position, i.e. for position 12, record docID[11], etc.
 * 2. its related file points(position, payload), 
 * 3. related numbers or uptos(position, payload).
 * 4. start offset.
 *
 */
final class Lucene50SkipWriter extends MultiLevelSkipListWriter {
  private int[] lastSkipDoc;  // 长度为10
  private long[] lastSkipDocPointer; // 长度为10   doc的跳表指针
  private long[] lastSkipPosPointer;  // 长度为10  // pos的跳表指针
  private long[] lastSkipPayPointer; // 长度为10   offset/pay的跳表指针
  private int[] lastPayloadByteUpto; // 长度为10

  private final IndexOutput docOut;
  private final IndexOutput posOut;
  private final IndexOutput payOut;

  private int curDoc;
  private long curDocPointer;   // doc文档当前FP。写完一个block 128文件的docId,freq时候的使用
  private long curPosPointer;
  private long curPayPointer;  // 当前
  private int curPosBufferUpto; //
  private int curPayloadByteUpto;
  private CompetitiveImpactAccumulator[] curCompetitiveFreqNorms;
  private boolean fieldHasPositions; // 为true
  private boolean fieldHasOffsets; // 为true
  private boolean fieldHasPayloads;//
 //
  public Lucene50SkipWriter(int maxSkipLevels, int blockSize, int docCount, IndexOutput docOut, IndexOutput posOut, IndexOutput payOut) {
    super(blockSize, 8, maxSkipLevels, docCount); // 本segment的文档数
    this.docOut = docOut;
    this.posOut = posOut;
    this.payOut = payOut;
    
    lastSkipDoc = new int[maxSkipLevels];
    lastSkipDocPointer = new long[maxSkipLevels]; // 默认数组长度为10
    if (posOut != null) {
      lastSkipPosPointer = new long[maxSkipLevels];
      if (payOut != null) {
        lastSkipPayPointer = new long[maxSkipLevels];
      }
      lastPayloadByteUpto = new int[maxSkipLevels];
    }
    curCompetitiveFreqNorms = new CompetitiveImpactAccumulator[maxSkipLevels];
    for (int i = 0; i < maxSkipLevels; ++i) { // 默认为10
      curCompetitiveFreqNorms[i] = new CompetitiveImpactAccumulator();
    }
  }

  public void setField(boolean fieldHasPositions, boolean fieldHasOffsets, boolean fieldHasPayloads) {
    this.fieldHasPositions = fieldHasPositions;
    this.fieldHasOffsets = fieldHasOffsets;
    this.fieldHasPayloads = fieldHasPayloads;
  }
  
  // tricky: we only skip data for blocks (terms with more than 128 docs), but re-init'ing the skipper 
  // is pretty slow for rare terms in large segments as we have to fill O(log #docs in segment) of junk.
  // this is the vast majority of terms (worst case: ID field or similar).  so in resetSkip() we save 
  // away the previous pointers, and lazy-init only if we need to buffer skip data for the term.
  private boolean initialized; // 延迟初始化，仅需要写入的时候再初始化
  long lastDocFP;
  long lastPosFP;
  long lastPayFP;

  @Override
  public void resetSkip() {
    lastDocFP = docOut.getFilePointer();
    if (fieldHasPositions) {
      lastPosFP = posOut.getFilePointer();
      if (fieldHasOffsets || fieldHasPayloads) {
        lastPayFP = payOut.getFilePointer();
      }
    }
    if (initialized) {
      for (CompetitiveImpactAccumulator acc : curCompetitiveFreqNorms) {
        acc.clear();
      }
    }
    initialized = false;
  }
  
  private void initSkip() {
    if (!initialized) {
      super.resetSkip(); // 置位
      Arrays.fill(lastSkipDoc, 0);
      Arrays.fill(lastSkipDocPointer, lastDocFP); // doc的FilePoint。还没有写docId和freq时候的PF
      if (fieldHasPositions) { // 为true
        Arrays.fill(lastSkipPosPointer, lastPosFP);
        if (fieldHasPayloads) { // 跳过
          Arrays.fill(lastPayloadByteUpto, 0);
        }
        if (fieldHasOffsets || fieldHasPayloads) {
          Arrays.fill(lastSkipPayPointer, lastPayFP);
        }
      }
      // sets of competitive freq,norm pairs should be empty at this point
      assert Arrays.stream(curCompetitiveFreqNorms)
          .map(CompetitiveImpactAccumulator::getCompetitiveFreqNormPairs)
          .mapToInt(Collection::size)
          .sum() == 0;
      initialized = true;
    }
  }

  /**
   * Sets the values for the current skip data. 
   */
  public void bufferSkip(int doc, CompetitiveImpactAccumulator competitiveFreqNorms,
      int numDocs, long posFP, long payFP, int posBufferUpto, int payloadByteUpto) throws IOException {
    initSkip(); // 初始化
    this.curDoc = doc; // 目前处理的文档数，是128的倍数
    this.curDocPointer = docOut.getFilePointer();
    this.curPosPointer = posFP;
    this.curPayPointer = payFP;
    this.curPosBufferUpto = posBufferUpto;
    this.curPayloadByteUpto = payloadByteUpto;
    this.curCompetitiveFreqNorms[0].addAll(competitiveFreqNorms);
    bufferSkip(numDocs); // 为当前文档建立了跳跃表结构
  }

  private final RAMOutputStream freqNormOut = new RAMOutputStream();

  @Override // 在level层建立一个跳表节点放入内存中skipBuffer中。
  protected void writeSkipData(int level, IndexOutput skipBuffer) throws IOException {

    int delta = curDoc - lastSkipDoc[level];// 计算当前level层，docId的delta值

    skipBuffer.writeVInt(delta); // 写入deltaDocId
    lastSkipDoc[level] = curDoc; // 该层级上一次的文档Id
    // 写入 doc文件的偏移量的 delta值
    skipBuffer.writeVLong(curDocPointer - lastSkipDocPointer[level]); // 记录下该跳跃点在doc使用的大小
    lastSkipDocPointer[level] = curDocPointer; // 记录当前doc占用的其实内存

    if (fieldHasPositions) { // 向skipBuffer写入pos,doc,pay偏移量

      skipBuffer.writeVLong(curPosPointer - lastSkipPosPointer[level]); // 记录下该跳跃点在pos使用的大小
      lastSkipPosPointer[level] = curPosPointer;
      skipBuffer.writeVInt(curPosBufferUpto); // 记录下当前缓存的文档数

      if (fieldHasPayloads) {
        skipBuffer.writeVInt(curPayloadByteUpto);
      }

      if (fieldHasOffsets || fieldHasPayloads) { // payload和offset都写入了同一个文档
        skipBuffer.writeVLong(curPayPointer - lastSkipPayPointer[level]); // 记录下该跳跃点在pay使用的大小
        lastSkipPayPointer[level] = curPayPointer;
      }
    }

    CompetitiveImpactAccumulator competitiveFreqNorms = curCompetitiveFreqNorms[level];
    assert competitiveFreqNorms.getCompetitiveFreqNormPairs().size() > 0;
    if (level + 1 < numberOfSkipLevels) {
      curCompetitiveFreqNorms[level + 1].addAll(competitiveFreqNorms);
    }
    writeImpacts(competitiveFreqNorms, freqNormOut); // 向freqNormOut中写入影响因子
    skipBuffer.writeVInt(Math.toIntExact(freqNormOut.getFilePointer()));
    freqNormOut.writeTo(skipBuffer); // 把freqNormOut数据向skipBuffer中写入
    freqNormOut.reset();
    competitiveFreqNorms.clear();
  }
  // 写入影响因子
  static void writeImpacts(CompetitiveImpactAccumulator acc, IndexOutput out) throws IOException {
    Collection<Impact> impacts = acc.getCompetitiveFreqNormPairs();
    Impact previous = new Impact(0, 0);
    for (Impact impact : impacts) {
      assert impact.freq > previous.freq;
      assert Long.compareUnsigned(impact.norm, previous.norm) > 0;
      int freqDelta = impact.freq - previous.freq - 1;
      long normDelta = impact.norm - previous.norm - 1;
      if (normDelta == 0) {
        // most of time, norm only increases by 1, so we can fold everything in a single byte
        out.writeVInt(freqDelta << 1);
      } else {
        out.writeVInt((freqDelta << 1) | 1);
        out.writeZLong(normDelta);
      }
      previous = impact;
    }
  }
}
