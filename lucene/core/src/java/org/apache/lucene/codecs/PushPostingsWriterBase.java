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
package org.apache.lucene.codecs;


import java.io.IOException;

import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;

/**
 * Extension of {@link PostingsWriterBase}, adding a push
 * API for writing each element of the postings.  This API
 * is somewhat analogous to an XML SAX API, while {@link
 * PostingsWriterBase} is more like an XML DOM API.
 * 
 * @see PostingsReaderBase
 * @lucene.experimental
 */
// TODO: find a better name; this defines the API that the
// terms dict impls use to talk to a postings impl.
// TermsDict + PostingsReader/WriterBase == PostingsConsumer/Producer
public abstract class PushPostingsWriterBase extends PostingsWriterBase {

  // Reused in writeTerm
  private PostingsEnum postingsEnum;
  private int enumFlags; // freq/position/offset

  /** {@link FieldInfo} of current field being written. */
  protected FieldInfo fieldInfo;

  /** {@link IndexOptions} of current field being
      written */
  protected IndexOptions indexOptions;

  /** True if the current field writes freqs. */
  protected boolean writeFreqs;

  /** True if the current field writes positions. */
  protected boolean writePositions;

  /** True if the current field writes payloads. */
  protected boolean writePayloads;

  /** True if the current field writes offsets. */
  protected boolean writeOffsets;

  /** Sole constructor. (For invocation by subclass 
   *  constructors, typically implicit.) */
  protected PushPostingsWriterBase() {
  }

  /** Return a newly created empty TermState */
  public abstract BlockTermState newTermState() throws IOException;

  /** Start a new term.  Note that a matching call to {@link
   *  #finishTerm(BlockTermState)} is done, only if the term has at least one
   *  document. */  // 还没有开始读取每个词词频时候调用的
  public abstract void startTerm(NumericDocValues norms) throws IOException;

  /** Finishes the current term.  The provided {@link
   *  BlockTermState} contains the term's summary statistics, 
   *  and will holds metadata from PBF when returned */
  public abstract void finishTerm(BlockTermState state) throws IOException;

  /** 
   * Sets the current field for writing, and returns the
   * fixed length of long[] metadata (which is fixed per
   * field), called when the writing switches to another field. */
  @Override
  public void setField(FieldInfo fieldInfo) {// 当前正在写的域
    this.fieldInfo = fieldInfo;
    indexOptions = fieldInfo.getIndexOptions();

    writeFreqs = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;//确认是否存储了
    writePositions = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
    writeOffsets = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;        
    writePayloads = fieldInfo.hasPayloads();

    if (writeFreqs == false) {
      enumFlags = 0;
    } else if (writePositions == false) {
      enumFlags = PostingsEnum.FREQS;
    } else if (writeOffsets == false) {
      if (writePayloads) {
        enumFlags = PostingsEnum.PAYLOADS;
      } else {
        enumFlags = PostingsEnum.POSITIONS;
      }
    } else {
      if (writePayloads) {
        enumFlags = PostingsEnum.PAYLOADS | PostingsEnum.OFFSETS;
      } else {
        enumFlags = PostingsEnum.OFFSETS; // 将跑到这里，全部都有
      }
    }
  }
  // 处理的是该词在每个文档中的词频及startOffset&endOffset，存储到doc(跳表)、pos，pay文件中
  @Override
  public final BlockTermState writeTerm(BytesRef term, TermsEnum termsEnum, FixedBitSet docsSeen, NormsProducer norms) throws IOException {
    NumericDocValues normValues;
    if (fieldInfo.hasNorms() == false) {
      normValues = null;
    } else {
      normValues = norms.getNorms(fieldInfo);
    }
    startTerm(normValues); // 设置doc,pos等文件的起始位置
    postingsEnum = termsEnum.postings(postingsEnum, enumFlags); // 读取了该词的stream1,stream2   FreqProxTermsEnum
    assert postingsEnum != null;

    int docFreq = 0; // 该词在多少个文档中出现过
    long totalTermFreq = 0; //该文档在该segment中出现的总词频
    while (true) { // 从内存中的倒排索引获取此term的所有数据，按doc的维度遍历处理该词所在的每个doc的词频及offset
      int docID = postingsEnum.nextDoc();// 读取这个term的第一个docID。每次读取一批文档
      if (docID == PostingsEnum.NO_MORE_DOCS) {
        break;
      }
      docFreq++;
      docsSeen.set(docID); // 在这个文档中可见
      int freq;
      if (writeFreqs) {
        freq = postingsEnum.freq(); // 该文档该词的词频。已经在postingsEnum.nextDoc()中给解析出来了
        totalTermFreq += freq;
      } else {
        freq = -1;
      } // 首先检查上一批block是否处理完了，处理完了则建立调表节点。然后检查是否达到一个block，达到了，则将DocId、freq压缩到doc文件中
      startDoc(docID, freq); // 会检查上批block是否已经处理完成了。保存freq。只会对doc文件建立跳表

      if (writePositions) {
        for(int i=0;i<freq;i++) { // 对每个词的每个position都读取出来
          int pos = postingsEnum.nextPosition(); // 第几个position，将startOffset和endOffset都解析出来了
          BytesRef payload = writePayloads ? postingsEnum.getPayload() : null;
          int startOffset;
          int endOffset;
          if (writeOffsets) {
            startOffset = postingsEnum.startOffset();
            endOffset = postingsEnum.endOffset();
          } else {
            startOffset = -1;
            endOffset = -1;
          } // 检查是否达到一个block的词个数，达到就开始存储DeltaPostion、DeltaStartOffset、length
          addPosition(pos, payload, startOffset, endOffset); // 保存增量的freq。跑到Lucene84PostingsWriter
        }
      }
      //
      finishDoc(); // 检查文档个数是否达到block，写到后，更新本地保存的FilePointer，清空缓存的文档数。
    }

    if (docFreq == 0) { // 总文档数
      return null;
    } else { // 开始落文件
      BlockTermState state = newTermState(); // 给这个block赋予一些元数据
      state.docFreq = docFreq; // 该词在多少文档中出现过
      state.totalTermFreq = writeFreqs ? totalTermFreq : -1; // 该词总共出现的次数
      finishTerm(state); // 当前term读取完了。将跳表信息填入doc
      return state;
    }
  }

  /** Adds a new doc in this term. 
   * <code>freq</code> will be -1 when term frequencies are omitted
   * for the field. */
  public abstract void startDoc(int docID, int freq) throws IOException;

  /** Add a new position and payload, and start/end offset.  A
   *  null payload means no payload; a non-null payload with
   *  zero length also means no payload.  Caller may reuse
   *  the {@link BytesRef} for the payload between calls
   *  (method must fully consume the payload). <code>startOffset</code>
   *  and <code>endOffset</code> will be -1 when offsets are not indexed. */
  public abstract void addPosition(int position, BytesRef payload, int startOffset, int endOffset) throws IOException;

  /** Called when we are done adding positions and payloads
   *  for each doc. */
  public abstract void finishDoc() throws IOException;
}
