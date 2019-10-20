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

import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.TermFrequencyAttribute;
import org.apache.lucene.codecs.TermVectorsWriter;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
// Lucene的倒排索引有两种格式，一种是用于搜索的Postings，在源码中由FreqProxTermsWriterPerField负责；另一种是TermsVectors，由TermsVectorsWriterPerField实现。
final class TermVectorsConsumerPerField extends TermsHashPerField { // segment内共享，segment完成后就清空。和FreqProxTermsWriterPerField一一对应

  private TermVectorsPostingsArray termVectorsPostingsArray;

  private final TermVectorsConsumer termsWriter;// TermVectorsConsumer
  private final FieldInvertState fieldState;
  private final FieldInfo fieldInfo;

  private boolean doVectors; // 默认为true
  private boolean doVectorPositions; // 默认为true
  private boolean doVectorOffsets; // 默认为true
  private boolean doVectorPayloads; // 默认为true

  private OffsetAttribute offsetAttribute;
  private PayloadAttribute payloadAttribute;
  private TermFrequencyAttribute termFreqAtt;
  private final ByteBlockPool termBytePool;

  private boolean hasPayloads; // if enabled, and we actually saw any for this field

  TermVectorsConsumerPerField(FieldInvertState invertState, TermVectorsConsumer termsHash, FieldInfo fieldInfo) {
    super(2, termsHash.intPool, termsHash.bytePool, termsHash.termBytePool, termsHash.bytesUsed, null, fieldInfo.name, fieldInfo.getIndexOptions());
    this.termsWriter = termsHash;
    this.fieldInfo = fieldInfo;
    this.fieldState = invertState;
    termBytePool = termsHash.termBytePool;
  }

  /** Called once per field per document if term vectors
   *  are enabled, to write the vectors to
   *  RAMOutputStream, which is then quickly flushed to
   *  the real term vectors files in the Directory. */  @Override
  void finish() {// 所有域处理完后会调用，将自己放在termsWriter中
    if (!doVectors || getNumTerms() == 0) {
      return;
    }
    termsWriter.addFieldToFlush(this); // 把产生的TermVectorsConsumerPerField放入termsWriter里面
  }
  // 是nextField.finsh()函数, 当前文档每个域都调用的
  void finishDocument() throws IOException {
    if (doVectors == false) {
      return;
    }

    doVectors = false; // 把状态改了

    final int numPostings = getNumTerms(); //存放着一个域的distinct(词)个数

    final BytesRef flushTerm = termsWriter.flushTerm; // 

    assert numPostings >= 0;

    // This is called once, after inverting all occurrences
    // of a given field in the doc.  At this point we flush
    // our hash into the DocWriter.

    TermVectorsPostingsArray postings = termVectorsPostingsArray;
    final TermVectorsWriter tv = termsWriter.writer;// CompressingTermVectorsWriter，一个segment共享一个，落盘完成后就置空了

    sortTerms();
    final int[] termIDs = getSortedTermIDs();// 一个域内termId的个数
    // numPostings 每个distinct(词), 这个域加一个
    tv.startField(fieldInfo, numPostings, doVectorPositions, doVectorOffsets, hasPayloads);
    // 整个链共享一个
    final ByteSliceReader posReader = doVectorPositions ? termsWriter.vectorSliceReaderPos : null;//ByteSliceReader, 为了读取某一个词
    final ByteSliceReader offReader = doVectorOffsets ? termsWriter.vectorSliceReaderOff : null; // ByteSliceReader  为了读取某一个词
    //以每个词为粒度, 开始依次初始化处理这个域的每一个单词,
    for(int j=0;j<numPostings;j++) { // 扫描该域每个distinct(词)。放入tv中
      final int termID = termIDs[j]; // 从词排序最小的那个开始, 得到词Id
      final int freq = postings.freqs[termID];  // 获取该词的词频

      // Get BytesRef
      termBytePool.setBytesRef(flushTerm, postings.textStarts[termID]); // 获取该词，放在flushTerm中
      tv.startTerm(flushTerm, freq);  // value存储使用了压缩发则，主要是存储词字符
      
      if (doVectorPositions || doVectorOffsets) { // 主要是nextFiled中的东西
        if (posReader != null) {
          initReader(posReader, termID, 0); // 读取这个词在0个stream上的所有长度
        }
        if (offReader != null) {
          initReader(offReader, termID, 1); //  读取这个词在1个stream上的所有长度
        }
        tv.addProx(freq, posReader, offReader); // 把nextTermVector中byteBuffer stream0/1都给读取出来，放到CompressingTermVectorsWriter的buffer中
      }
      tv.finishTerm(); // 啥事都不做
    }
    tv.finishField(); // 啥事都不做，仅仅清空curField=null

    reset(); // 这里会清空TermVectorsConsumerPerField里面的BytesRefHash里面ids等值

    fieldInfo.setStoreTermVectors(); // 啥都不干
  }
 // 在写入每个域时候，会对TermVectorsConsumerPerField进行初始化，
  @Override
  boolean start(IndexableField field, boolean first) {
    super.start(field, first);
    termFreqAtt = fieldState.termFreqAttribute;
    assert field.fieldType().indexOptions() != IndexOptions.NONE;

    if (first) {

      if (getNumTerms() != 0) {
        // Only necessary if previous doc hit a
        // non-aborting exception while writing vectors in
        // this field:
        reset();
      }

      reinitHash();// 清空TermVectorsConsumerPerField.TermVectorsPostingsArray

      hasPayloads = false;

      doVectors = field.fieldType().storeTermVectors(); // 存储termVector

      if (doVectors) { // 若设置了doVectors

        termsWriter.hasVectors = true;

        doVectorPositions = field.fieldType().storeTermVectorPositions();  // true

        // Somewhat confusingly, unlike postings, you are
        // allowed to index TV offsets without TV positions:
        doVectorOffsets = field.fieldType().storeTermVectorOffsets(); // true

        if (doVectorPositions) {
          doVectorPayloads = field.fieldType().storeTermVectorPayloads(); // true
        } else {
          doVectorPayloads = false;
          if (field.fieldType().storeTermVectorPayloads()) {
            // TODO: move this check somewhere else, and impl the other missing ones
            throw new IllegalArgumentException("cannot index term vector payloads without term vector positions (field=\"" + field.name() + "\")");
          }
        }
        
      } else {
        if (field.fieldType().storeTermVectorOffsets()) {
          throw new IllegalArgumentException("cannot index term vector offsets when term vectors are not indexed (field=\"" + field.name() + "\")");
        }
        if (field.fieldType().storeTermVectorPositions()) {
          throw new IllegalArgumentException("cannot index term vector positions when term vectors are not indexed (field=\"" + field.name() + "\")");
        }
        if (field.fieldType().storeTermVectorPayloads()) {
          throw new IllegalArgumentException("cannot index term vector payloads when term vectors are not indexed (field=\"" + field.name() + "\")");
        }
      }
    } else {
      if (doVectors != field.fieldType().storeTermVectors()) {
        throw new IllegalArgumentException("all instances of a given field name must have the same term vectors settings (storeTermVectors changed for field=\"" + field.name() + "\")");
      }
      if (doVectorPositions != field.fieldType().storeTermVectorPositions()) {
        throw new IllegalArgumentException("all instances of a given field name must have the same term vectors settings (storeTermVectorPositions changed for field=\"" + field.name() + "\")");
      }
      if (doVectorOffsets != field.fieldType().storeTermVectorOffsets()) {
        throw new IllegalArgumentException("all instances of a given field name must have the same term vectors settings (storeTermVectorOffsets changed for field=\"" + field.name() + "\")");
      }
      if (doVectorPayloads != field.fieldType().storeTermVectorPayloads()) {
        throw new IllegalArgumentException("all instances of a given field name must have the same term vectors settings (storeTermVectorPayloads changed for field=\"" + field.name() + "\")");
      }
    }

    if (doVectors) {
      if (doVectorOffsets) {
        offsetAttribute = fieldState.offsetAttribute;
        assert offsetAttribute != null;
      }

      if (doVectorPayloads) {
        // Can be null:
        payloadAttribute = fieldState.payloadAttribute; // null
      } else {
        payloadAttribute = null;
      }
    }

    return doVectors; // true
  }

  void writeProx(TermVectorsPostingsArray postings, int termID) {
    if (doVectorOffsets) {
      int startOffset = fieldState.offset + offsetAttribute.startOffset();
      int endOffset = fieldState.offset + offsetAttribute.endOffset();
      // 压缩存储
      writeVInt(1, startOffset - postings.lastOffsets[termID]); // 也是为了减少存储的大小，在CompressingTermVectorsWriter的addProx那里通过累加复原了
      writeVInt(1, endOffset - startOffset);
      postings.lastOffsets[termID] = endOffset;
    }

    if (doVectorPositions) { // // 没看懂记录着有啥用
      final BytesRef payload;
      if (payloadAttribute == null) {
        payload = null;
      } else {
        payload = payloadAttribute.getPayload();
      } // 这样做的目的是为了减少存储的size大小，使用增量方式，减少了position的大小。后面在实际放入positionsBuf时给还原出来了
      // 若是某个词是第一次lastPositions，那么lastPositions[termID]直接等于0
      final int pos = fieldState.position - postings.lastPositions[termID]; // 若来了一个新的termId，那么postings.lastPositions[termID]一定为0。压缩存储
      if (payload != null && payload.length > 0) {
        writeVInt(0, (pos<<1)|1);
        writeVInt(0, payload.length);
        writeBytes(0, payload.bytes, payload.offset, payload.length);
        hasPayloads = true;
      } else {
        writeVInt(0, pos<<1); // 存储0
      }
      postings.lastPositions[termID] = fieldState.position;
    }
  }

  @Override
  void newTerm(final int termID, final int docID) {
    TermVectorsPostingsArray postings = termVectorsPostingsArray;

    postings.freqs[termID] = getTermFreq();
    postings.lastOffsets[termID] = 0;
    postings.lastPositions[termID] = 0;
    
    writeProx(postings, termID);
  }

  @Override
  void addTerm(final int termID, final int docID) {
    TermVectorsPostingsArray postings = termVectorsPostingsArray;

    postings.freqs[termID] += getTermFreq(); // 词频用这个对象保留的

    writeProx(postings, termID);
  }

  private int getTermFreq() {
    int freq = termFreqAtt.getTermFrequency();
    if (freq != 1) {
      if (doVectorPositions) {
        throw new IllegalArgumentException("field \"" + getFieldName() + "\": cannot index term vector positions while using custom TermFrequencyAttribute");
      }
      if (doVectorOffsets) {
        throw new IllegalArgumentException("field \"" + getFieldName() + "\": cannot index term vector offsets while using custom TermFrequencyAttribute");
      }
    }

    return freq;
  }

  @Override
  public void newPostingsArray() {
    termVectorsPostingsArray = (TermVectorsPostingsArray) postingsArray;
  }

  @Override
  ParallelPostingsArray createPostingsArray(int size) {
    return new TermVectorsPostingsArray(size);
  }

  static final class TermVectorsPostingsArray extends ParallelPostingsArray {
     TermVectorsPostingsArray(int size) {
      super(size);
      freqs = new int[size];
      lastOffsets = new int[size];
      lastPositions = new int[size];
    }

    int[] freqs;   // How many times this term occurred in the current doc  都有，这个词并没有立马存入byteBuffer中，在后面finish时候直接使用的
    int[] lastOffsets;  // Last offset we saw   该词出现的最后一次的endoffset
    int[] lastPositions;      // Last position where this term occurred  都有，每次进入一个新词，就清空旧词的参数

    @Override
    ParallelPostingsArray newInstance(int size) {
      return new TermVectorsPostingsArray(size);
    }

    @Override
    void copyTo(ParallelPostingsArray toArray, int numToCopy) {
      assert toArray instanceof TermVectorsPostingsArray;
      TermVectorsPostingsArray to = (TermVectorsPostingsArray) toArray;

      super.copyTo(toArray, numToCopy);

      System.arraycopy(freqs, 0, to.freqs, 0, size);
      System.arraycopy(lastOffsets, 0, to.lastOffsets, 0, size);
      System.arraycopy(lastPositions, 0, to.lastPositions, 0, size);
    }

    @Override
    int bytesPerPosting() {
      return super.bytesPerPosting() + 3 * Integer.BYTES;
    }
  }
}
