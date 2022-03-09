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
// Postings就是正常的倒排索引结果，work->docId1->docId2
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.TermFrequencyAttribute;
import org.apache.lucene.util.BytesRef;
// 介绍过Lucene的倒排索引有两种格式，一种是用于搜索的Postings，在源码中由FreqProxTermsWriterPerField负责；另一种是TermsVectors，由TermsVectorsWriterPerField实现。
// TODO: break into separate freq and prox writers as
// codecs; make separate container (tii/tis/skip/*) that can
// be configured as any number of files 1..N
final class FreqProxTermsWriterPerField extends TermsHashPerField {// segment内共享，segment完成后就清空

  private FreqProxPostingsArray freqProxPostingsArray; //  是在TermsHashPerField的bytesHash构建时初始化的，其值=FreqProxPostingsArray=TermsHashPerField.postingsArray
  private final FieldInvertState fieldState;
  private final FieldInfo fieldInfo;

  final boolean hasFreq;  // 是否存储词频
  final boolean hasProx;  //位置信息，在字段中第1、2个词
  final boolean hasOffsets; ///
  PayloadAttribute payloadAttribute;  //
  OffsetAttribute offsetAttribute;
  TermFrequencyAttribute termFreqAtt;

  /** Set to true if any token had a payload in the current
   *  segment. */
  boolean sawPayloads;

  FreqProxTermsWriterPerField(FieldInvertState invertState, TermsHash termsHash, FieldInfo fieldInfo, TermsHashPerField nextPerField) {
    super(fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0 ? 2 : 1,
        termsHash.intPool, termsHash.bytePool, termsHash.termBytePool, termsHash.bytesUsed, nextPerField, fieldInfo.name, fieldInfo.getIndexOptions());
    this.fieldState = invertState;
    this.fieldInfo = fieldInfo;
    hasFreq = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS) >= 0; //词频
    hasProx = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;// true  位置信息
    hasOffsets = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0; // 偏移
  }
   // 每个文档写完之后就会进来
  @Override
  void finish() throws IOException {
    super.finish(); // 先去处理TermVectorsConsumerPerField，放入TermVectorsConsumer中
    if (sawPayloads) {
      fieldInfo.setStorePayloads();
    }
  }

  @Override
  boolean start(IndexableField f, boolean first) {
    super.start(f, first);
    termFreqAtt = fieldState.termFreqAttribute;
    payloadAttribute = fieldState.payloadAttribute; // PackedTokenAttributeImpl
    offsetAttribute = fieldState.offsetAttribute; // PackedTokenAttributeImpl, 哥俩是同一个对象
    return true;
  }
  // 写入哪个stream, 写入的postion
  void writeProx(int termID, int proxCode) {   // proxCode = position, 这里termId没有用上
    if (payloadAttribute == null) {
      writeVInt(1, proxCode<<1); // 跑到这里
    } else {
      BytesRef payload = payloadAttribute.getPayload();
      if (payload != null && payload.length > 0) {
        writeVInt(1, (proxCode<<1)|1); //
        writeVInt(1, payload.length);
        writeBytes(1, payload.bytes, payload.offset, payload.length);
        sawPayloads = true;
      } else {
        writeVInt(1, proxCode<<1);
      }
    }

    assert postingsArray == freqProxPostingsArray;
    freqProxPostingsArray.lastPositions[termID] = fieldState.position;
  }
  // 写入offset,也是向stream=1写入的
  void writeOffsets(int termID, int offsetAccum) {
    final int startOffset = offsetAccum + offsetAttribute.startOffset(); // 词在整个域中的的偏移量
    final int endOffset = offsetAccum + offsetAttribute.endOffset();
    assert startOffset - freqProxPostingsArray.lastOffsets[termID] >= 0;
    writeVInt(1, startOffset - freqProxPostingsArray.lastOffsets[termID]); // deltaStartOffset
    writeVInt(1, endOffset - startOffset);
    freqProxPostingsArray.lastOffsets[termID] = startOffset; //更新下最后出现的起始位置
  }

  @Override// 针对这个termId进行统计
  void newTerm(final int termID, final int docID) {
    // First time we're seeing this term since the last
    // flush
    final FreqProxPostingsArray postings = freqProxPostingsArray; //

    postings.lastDocIDs[termID] = docID;
    if (!hasFreq) { //DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS 按序增加
      assert postings.termFreqs == null;
      postings.lastDocCodes[termID] = docID;
      fieldState.maxTermFrequency = Math.max(1, fieldState.maxTermFrequency);
    } else { // 需要存储词频信息
      postings.lastDocCodes[termID] = docID << 1;
      postings.termFreqs[termID] = getTermFreq();// 只是先记录下来，但是位置和偏移量信息，已经存入缓存结构中了
      if (hasProx) { // TermVectorsConsumerPerField和FreqProxTermsWriterPerField都会调用newTerm()存储 offset, proc三样
        writeProx(termID, fieldState.position); // 向stream1中存储了proxCode
        if (hasOffsets) {
          writeOffsets(termID, fieldState.offset); // 向stream1中存储了offserCode,
        }
      } else {
        assert !hasOffsets;
      }
      fieldState.maxTermFrequency = Math.max(postings.termFreqs[termID], fieldState.maxTermFrequency);
    }
    fieldState.uniqueTermCount++;
  }
  // https://www.iteye.com/blog/hxraid-642737
  @Override
  void addTerm(final int termID, final int docID) {
    final FreqProxPostingsArray postings = freqProxPostingsArray;
    assert !hasFreq || postings.termFreqs[termID] > 0;

    if (!hasFreq) {//不需要词频
      assert postings.termFreqs == null;
      if (termFreqAtt.getTermFrequency() != 1) {
        throw new IllegalStateException("field \"" + getFieldName() + "\": must index term freq while using custom TermFrequencyAttribute");
      }
      if (docID != postings.lastDocIDs[termID]) {// 上个文档写完了，那么写入上一个文档号
        // New document; now encode docCode for previous doc:
        assert docID > postings.lastDocIDs[termID];
        writeVInt(0, postings.lastDocCodes[termID]);
        postings.lastDocCodes[termID] = docID - postings.lastDocIDs[termID];
        postings.lastDocIDs[termID] = docID;
        fieldState.uniqueTermCount++;
      }
    } else if (docID != postings.lastDocIDs[termID]) {// 不是同一个文档了了，那么开始进行收集词频等信息。并将词频信息落盘
      assert docID > postings.lastDocIDs[termID]:"id: "+docID + " postings ID: "+ postings.lastDocIDs[termID] + " termID: "+termID;
      // Term not yet seen in the current doc but previously
      // seen in other doc(s) since the last flush

      // Now that we know doc freq for previous doc,
      // write it & lastDocCode
      if (1 == postings.termFreqs[termID]) {  // 词频为1
        writeVInt(0, postings.lastDocCodes[termID]|1); // 注意最后一位是1，就存了一位。所以存储时需要左移动一位
      } else {
        writeVInt(0, postings.lastDocCodes[termID]); // 看newTerms里面的 docID << 1
        writeVInt(0, postings.termFreqs[termID]);
      }

      // Init freq for the current document
      postings.termFreqs[termID] = getTermFreq(); //初始化当前文档的词频
      fieldState.maxTermFrequency = Math.max(postings.termFreqs[termID], fieldState.maxTermFrequency);
      postings.lastDocCodes[termID] = (docID - postings.lastDocIDs[termID]) << 1;//  这次出现文档-上次出现文档
      postings.lastDocIDs[termID] = docID;
      if (hasProx) {
        writeProx(termID, fieldState.position);
        if (hasOffsets) {
          postings.lastOffsets[termID] = 0;
          writeOffsets(termID, fieldState.offset);
        }
      } else {
        assert !hasOffsets;
      }
      fieldState.uniqueTermCount++;
    } else { // 一个doc还没有写完，那么始终是统计词频
      postings.termFreqs[termID] = Math.addExact(postings.termFreqs[termID], getTermFreq()); // 词频想加
      fieldState.maxTermFrequency = Math.max(fieldState.maxTermFrequency, postings.termFreqs[termID]);
      if (hasProx) { // 继续统计post
        writeProx(termID, fieldState.position-postings.lastPositions[termID]);
        if (hasOffsets) { // 统计offser
          writeOffsets(termID, fieldState.offset);
        }
      }
    }
  }

  private int getTermFreq() {
    int freq = termFreqAtt.getTermFrequency(); // 默认获取词频,每次一定是1
    if (freq != 1) {
      if (hasProx) {
        throw new IllegalStateException("field \"" + getFieldName() + "\": cannot index positions while using custom TermFrequencyAttribute");
      }
    }

    return freq;
  }

  @Override
  public void newPostingsArray() { // 在
    freqProxPostingsArray = (FreqProxPostingsArray) postingsArray;
  }

  @Override
  ParallelPostingsArray createPostingsArray(int size) {
    boolean hasFreq = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
    boolean hasProx = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
    boolean hasOffsets = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
    return new FreqProxPostingsArray(size, hasFreq, hasProx, hasOffsets);
  }
   // 文档参考：https://blog.csdn.net/zteny/article/details/83547164#22__93
  static final class FreqProxPostingsArray extends ParallelPostingsArray {//这个结构只保留每个TermID最后出现的情况， 对于TermID每次出现的具体信息则是需要存在其它的结构之中。它们就是IntBlockPool&ByteBlockPool
    public FreqProxPostingsArray(int size, boolean writeFreqs, boolean writeProx, boolean writeOffsets) {
      super(size); // 整个
      if (writeFreqs) {
        termFreqs = new int[size];
      }
      lastDocIDs = new int[size];
      lastDocCodes = new int[size];
      if (writeProx) {
        lastPositions = new int[size];
        if (writeOffsets) {
          lastOffsets = new int[size];
        }
      } else {
        assert !writeOffsets;
      }
      //System.out.println("PA init freqs=" + writeFreqs + " pos=" + writeProx + " offs=" + writeOffsets);
    }
    // 每个termId就占一位
    int termFreqs[];     // # times this term occurs in the current doc  统计每个词的词频，每出现一个词，就会不同的累加
    int lastDocIDs[];   // Last docID where this term occurred  这个term最后出现在那个docId中的，
    int lastDocCodes[];  // Code for prior doc  当前(docId-lastDocId)<<1  是为了缓存的，下次向文档中写入
    int lastPositions[];                               // Last position where this term occurred  记录的是是当前词最后出现的position，在writeProx会更新
    int lastOffsets[];                                 // Last endOffset where this term occurred  记录的是当前词最后出现的起始offset,在writeOffsets会更新

    @Override
    ParallelPostingsArray newInstance(int size) {
      return new FreqProxPostingsArray(size, termFreqs != null, lastPositions != null, lastOffsets != null);
    }

    @Override
    void copyTo(ParallelPostingsArray toArray, int numToCopy) {
      assert toArray instanceof FreqProxPostingsArray;
      FreqProxPostingsArray to = (FreqProxPostingsArray) toArray;

      super.copyTo(toArray, numToCopy);

      System.arraycopy(lastDocIDs, 0, to.lastDocIDs, 0, numToCopy);
      System.arraycopy(lastDocCodes, 0, to.lastDocCodes, 0, numToCopy);
      if (lastPositions != null) {
        assert to.lastPositions != null;
        System.arraycopy(lastPositions, 0, to.lastPositions, 0, numToCopy);
      }
      if (lastOffsets != null) {
        assert to.lastOffsets != null;
        System.arraycopy(lastOffsets, 0, to.lastOffsets, 0, numToCopy);
      }
      if (termFreqs != null) {
        assert to.termFreqs != null;
        System.arraycopy(termFreqs, 0, to.termFreqs, 0, numToCopy);
      }
    }

    @Override
    int bytesPerPosting() {
      int bytes = ParallelPostingsArray.BYTES_PER_POSTING + 2 * Integer.BYTES;
      if (lastPositions != null) {
        bytes += Integer.BYTES;
      }
      if (lastOffsets != null) {
        bytes += Integer.BYTES;
      }
      if (termFreqs != null) {
        bytes += Integer.BYTES;
      }

      return bytes;
    }
  }
}
