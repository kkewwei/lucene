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
package org.apache.lucene.codecs.lucene84;

import static org.apache.lucene.codecs.lucene84.ForUtil.BLOCK_SIZE;
import static org.apache.lucene.codecs.lucene84.Lucene84PostingsFormat.DOC_CODEC;
import static org.apache.lucene.codecs.lucene84.Lucene84PostingsFormat.MAX_SKIP_LEVELS;
import static org.apache.lucene.codecs.lucene84.Lucene84PostingsFormat.PAY_CODEC;
import static org.apache.lucene.codecs.lucene84.Lucene84PostingsFormat.POS_CODEC;
import static org.apache.lucene.codecs.lucene84.Lucene84PostingsFormat.TERMS_CODEC;
import static org.apache.lucene.codecs.lucene84.Lucene84PostingsFormat.VERSION_CURRENT;

import java.io.IOException;
import java.nio.ByteOrder;

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.CompetitiveImpactAccumulator;
import org.apache.lucene.codecs.PushPostingsWriterBase;
import org.apache.lucene.codecs.lucene84.Lucene84PostingsFormat.IntBlockTermState;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

/**
 * Concrete class that writes docId(maybe frq,pos,offset,payloads) list
 * with postings format.
 *
 * Postings list for each term will be stored separately. 
 *
 * @see Lucene84SkipWriter for details about skipping setting and postings layout.
 * @lucene.experimental
 */ //每个索引文件的作用：https://www.shenyanchao.cn/blog/2018/12/04/lucene-index-files/
public final class Lucene84PostingsWriter extends PushPostingsWriterBase {

  IndexOutput docOut;// 保留包含每个Term的文档列表
  IndexOutput posOut;// Term在文章中出现的位置信息，merge阶段，也是RateLimitedIndexOutput
  IndexOutput payOut;// offset偏移和payload附加信息

  final static IntBlockTermState emptyState = new IntBlockTermState();
  IntBlockTermState lastState;

  // Holds starting file pointers for current term:
  private long docStartFP;// 这个单词在写入doc文件前，记录的doc绝对起始位置
  private long posStartFP;// 每个单词写入时pos文件最开始的绝对起始位置
  private long payStartFP;// 每个单词写入时pay文件最开始的绝对起始位置

  final long[] docDeltaBuffer;// 缓存的文档id增量
  final long[] freqBuffer;// 缓存的词频
  private int docBufferUpto;// 缓存的文档数。128个文档即为一个block

  final long[] posDeltaBuffer;// position增量
  final long[] payloadLengthBuffer;
  final long[] offsetStartDeltaBuffer; // 增量offset
  final long[] offsetLengthBuffer; // 长度
  private int posBufferUpto;// 词的存放个数，128个词即为一个block

  private byte[] payloadBytes;
  private int payloadByteUpto;

  private int lastBlockDocID;//  该block所有文档&freq写入建立跳表一个节点时会去更新doc
  private long lastBlockPosFP;//  该block所有文档&freq写入建立跳表一个节点时会去更新position
  private long lastBlockPayFP;//  该block所有文档&freq写入建立跳表一个节点时会去更新payload&&offset
  private int lastBlockPosBufferUpto;
  private int lastBlockPayloadByteUpto;

  private int lastDocID;
  private int lastPosition;
  private int lastStartOffset;
  private int docCount; // 总共的文档数

  private final PForUtil pforUtil;
  private final ForDeltaUtil forDeltaUtil;
  private final Lucene84SkipWriter skipWriter;// Lucene84SkipWriter

  private boolean fieldHasNorms;
  private NumericDocValues norms;
  private final CompetitiveImpactAccumulator competitiveFreqNormAccumulator = new CompetitiveImpactAccumulator();

  /** Creates a postings writer */
  public Lucene84PostingsWriter(SegmentWriteState state) throws IOException {
    // _0_Lucene50_0.doc
    String docFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, Lucene84PostingsFormat.DOC_EXTENSION);
    docOut = state.directory.createOutput(docFileName, state.context);
    IndexOutput posOut = null;
    IndexOutput payOut = null;
    boolean success = false;
    try {
      CodecUtil.writeIndexHeader(docOut, DOC_CODEC, VERSION_CURRENT,// 创建doc文件
                                   state.segmentInfo.getId(), state.segmentSuffix);
      ByteOrder byteOrder = ByteOrder.nativeOrder();
      if (byteOrder == ByteOrder.BIG_ENDIAN) {
        docOut.writeByte((byte) 'B');
      } else if (byteOrder == ByteOrder.LITTLE_ENDIAN) {// 小端模式
        docOut.writeByte((byte) 'L');
      } else {
        throw new Error();
      }
      final ForUtil forUtil = new ForUtil();
      forDeltaUtil = new ForDeltaUtil(forUtil);
      pforUtil = new PForUtil(forUtil);
      if (state.fieldInfos.hasProx()) { // 如果设置了需要位置信息
        posDeltaBuffer = new long[BLOCK_SIZE];// _0_Lucene50_0.pos
        String posFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, Lucene84PostingsFormat.POS_EXTENSION);
        posOut = state.directory.createOutput(posFileName, state.context);// 创建pos文件
        CodecUtil.writeIndexHeader(posOut, POS_CODEC, VERSION_CURRENT,
                                     state.segmentInfo.getId(), state.segmentSuffix);

        if (state.fieldInfos.hasPayloads()) {
          payloadBytes = new byte[128];
          payloadLengthBuffer = new long[BLOCK_SIZE];
        } else { // 默认跑这里
          payloadBytes = null;
          payloadLengthBuffer = null;
        }

        if (state.fieldInfos.hasOffsets()) {
          offsetStartDeltaBuffer = new long[BLOCK_SIZE];
          offsetLengthBuffer = new long[BLOCK_SIZE];
        } else {
          offsetStartDeltaBuffer = null;
          offsetLengthBuffer = null;
        }
        //创建_0_Lucene50_0.pay
        if (state.fieldInfos.hasPayloads() || state.fieldInfos.hasOffsets()) {
          String payFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, Lucene84PostingsFormat.PAY_EXTENSION);
          payOut = state.directory.createOutput(payFileName, state.context);
          CodecUtil.writeIndexHeader(payOut, PAY_CODEC, VERSION_CURRENT,
                                       state.segmentInfo.getId(), state.segmentSuffix);
        }
      } else {
        posDeltaBuffer = null;
        payloadLengthBuffer = null;
        offsetStartDeltaBuffer = null;
        offsetLengthBuffer = null;
        payloadBytes = null;
      }
      this.payOut = payOut;
      this.posOut = posOut;
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(docOut, posOut, payOut);
      }
    }

    docDeltaBuffer = new long[BLOCK_SIZE];
    freqBuffer = new long[BLOCK_SIZE];
    // 跳表相关
    // TODO: should we try skipping every 2/4 blocks...?
    skipWriter = new Lucene84SkipWriter(MAX_SKIP_LEVELS,
                                        BLOCK_SIZE, 
                                        state.segmentInfo.maxDoc(),
                                        docOut,
                                        posOut,
                                        payOut);
  }

  @Override
  public IntBlockTermState newTermState() {
    return new IntBlockTermState();
  }

  @Override
  public void init(IndexOutput termsOut, SegmentWriteState state) throws IOException {
    CodecUtil.writeIndexHeader(termsOut, TERMS_CODEC, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
    termsOut.writeVInt(BLOCK_SIZE);// tim.
  }

  @Override
  public void setField(FieldInfo fieldInfo) {
    super.setField(fieldInfo);
    skipWriter.setField(writePositions, writeOffsets, writePayloads);
    lastState = emptyState;
    fieldHasNorms = fieldInfo.hasNorms();
  }
  // 还没有开始读取每个词词频时候调用的
  @Override
  public void startTerm(NumericDocValues norms) {
    docStartFP = docOut.getFilePointer();// doc文件
    if (writePositions) {
      posStartFP = posOut.getFilePointer();
      if (writePayloads || writeOffsets) {
        payStartFP = payOut.getFilePointer();
      }
    }
    lastDocID = 0;
    lastBlockDocID = -1;
    skipWriter.resetSkip();// 初始化doc文档
    this.norms = norms;
    competitiveFreqNormAccumulator.clear();
  }
  // docID： 最新的文档id
  @Override
  public void startDoc(int docID, int termDocFreq) throws IOException {
    // Have collected a block of docs, and get a new doc. 
    // Should write skip data as well as postings list for
    // current block. // 针对每个block(128个文档)建立索引结构
    if (lastBlockDocID != -1 && docBufferUpto == 0) {// 上一批block的docId,freq已经被压缩到了doc文件中。则开始针对上一批数据建立跳表
      skipWriter.bufferSkip(lastBlockDocID, competitiveFreqNormAccumulator, docCount, // 把当前文档当做一个跳跃表节点
          lastBlockPosFP, lastBlockPayFP, lastBlockPosBufferUpto, lastBlockPayloadByteUpto);
      competitiveFreqNormAccumulator.clear();
    }

    final int docDelta = docID - lastDocID;

    if (docID < 0 || (docCount > 0 && docDelta <= 0)) {
      throw new CorruptIndexException("docs out of order (" + docID + " <= " + lastDocID + " )", docOut);
    }

    docDeltaBuffer[docBufferUpto] = docDelta;
    if (writeFreqs) {// 进来
      freqBuffer[docBufferUpto] = termDocFreq;// 存储词频
    }
    
    docBufferUpto++;
    docCount++;

    if (docBufferUpto == BLOCK_SIZE) { //每128个作为一个Block，不能凑整的情况下，再按VIntBlock进行存储
      forDeltaUtil.encodeDeltas(docDeltaBuffer, docOut);// 将128个缓存的doc压缩到doc文件中
      if (writeFreqs) {
        pforUtil.encode(freqBuffer, docOut);// 把128个缓存的文档freq缓存到doc中
      }
      // NOTE: don't set docBufferUpto back to 0 here;
      // finishDoc will do so (because it needs to see that
      // the block was filled so it can save skip data)
    }


    lastDocID = docID;
    lastPosition = 0;
    lastStartOffset = 0;

    long norm;
    if (fieldHasNorms) {
      boolean found = norms.advanceExact(docID);
      if (found == false) {
        // This can happen if indexing hits a problem after adding a doc to the
        // postings but before buffering the norm. Such documents are written
        // deleted and will go away on the first merge.
        norm = 1L;
      } else {
        norm = norms.longValue();
        assert norm != 0 : docID;
      }
    } else {
      norm = 1L;
    }

    competitiveFreqNormAccumulator.add(writeFreqs ? termDocFreq : 1, norm);
  }

  @Override
  public void addPosition(int position, BytesRef payload, int startOffset, int endOffset) throws IOException {
    if (position > IndexWriter.MAX_POSITION) {
      throw new CorruptIndexException("position=" + position + " is too large (> IndexWriter.MAX_POSITION=" + IndexWriter.MAX_POSITION + ")", docOut);
    }
    if (position < 0) {
      throw new CorruptIndexException("position=" + position + " is < 0", docOut);
    }
    posDeltaBuffer[posBufferUpto] = position - lastPosition;// 增量
    if (writePayloads) { // 忽略过
      if (payload == null || payload.length == 0) {
        // no payload
        payloadLengthBuffer[posBufferUpto] = 0;
      } else {
        payloadLengthBuffer[posBufferUpto] = payload.length;
        if (payloadByteUpto + payload.length > payloadBytes.length) {
          payloadBytes = ArrayUtil.grow(payloadBytes, payloadByteUpto + payload.length);
        }
        System.arraycopy(payload.bytes, payload.offset, payloadBytes, payloadByteUpto, payload.length);
        payloadByteUpto += payload.length;
      }
    }

    if (writeOffsets) {// 写入offset
      assert startOffset >= lastStartOffset;
      assert endOffset >= startOffset;
      offsetStartDeltaBuffer[posBufferUpto] = startOffset - lastStartOffset;// 增量offset
      offsetLengthBuffer[posBufferUpto] = endOffset - startOffset; // 长度
      lastStartOffset = startOffset;
    }
    
    posBufferUpto++;
    lastPosition = position;
    if (posBufferUpto == BLOCK_SIZE) {// 检查position是否写了128个词
      pforUtil.encode(posDeltaBuffer, posOut);//将128个position压缩写入pos文件中

      if (writePayloads) {
        pforUtil.encode(payloadLengthBuffer, payOut);
        payOut.writeVInt(payloadByteUpto);
        payOut.writeBytes(payloadBytes, 0, payloadByteUpto);
        payloadByteUpto = 0;
      }
      if (writeOffsets) {
        pforUtil.encode(offsetStartDeltaBuffer, payOut);// 将128个词的startOffset压缩写入pay文件中。offset以每个单词为基准。
        pforUtil.encode(offsetLengthBuffer, payOut);// 压缩写入词的长度
      }
      posBufferUpto = 0;
    }
  }
  //  读完完一个文档的词频信息后
  @Override
  public void finishDoc() throws IOException {
    // Since we don't know df for current term, we had to buffer
    // those skip data for each block, and when a new doc comes, 
    // write them to skip file.
    if (docBufferUpto == BLOCK_SIZE) { // 文档个数达到128个了
      lastBlockDocID = lastDocID;// 才会建立跳表
      if (posOut != null) {
        if (payOut != null) {
          lastBlockPayFP = payOut.getFilePointer();
        }
        lastBlockPosFP = posOut.getFilePointer();// 建立跳表的时候使用
        lastBlockPosBufferUpto = posBufferUpto;
        lastBlockPayloadByteUpto = payloadByteUpto;
      }
      docBufferUpto = 0;
    }
  }

  /** Called when we are done adding docs to this term */
  @Override
  public void finishTerm(BlockTermState _state) throws IOException {
    IntBlockTermState state = (IntBlockTermState) _state;
    assert state.docFreq > 0;

    // TODO: wasteful we are counting this (counting # docs
    // for this term) in two places?
    assert state.docFreq == docCount: state.docFreq + " vs " + docCount;
    
    // docFreq == 1, don't write the single docid/freq to a separate file along with a pointer to it.
    final int singletonDocID;
    if (state.docFreq == 1) {
      // pulse the singleton docid into the term dictionary, freq is implicitly totalTermFreq
      singletonDocID = (int) docDeltaBuffer[0];
    } else {
      singletonDocID = -1;
      // vInt encode the remaining doc deltas and freqs:
      for(int i=0;i<docBufferUpto;i++) {/// 将还在缓存中的文件给写到doc文件中。不能组成一个block了
        final int docDelta = (int) docDeltaBuffer[i];
        final int freq = (int) freqBuffer[i];
        if (!writeFreqs) {
          docOut.writeVInt(docDelta);
        } else if (freq == 1) {
          docOut.writeVInt((docDelta<<1)|1);
        } else {
          docOut.writeVInt(docDelta<<1);
          docOut.writeVInt(freq);
        }
      }
    }

    final long lastPosBlockOffset;// 计算下这个词在pos中占用的文件大小

    if (writePositions) {
      // totalTermFreq is just total number of positions(or payloads, or offsets)
      // associated with current term.
      assert state.totalTermFreq != -1;
      if (state.totalTermFreq > BLOCK_SIZE) { // 若词的已经大于了一个block
        // record file offset for last pos in last block
        lastPosBlockOffset = posOut.getFilePointer() - posStartFP;// 保存的这个词在pos文件中写入的长度
      } else {
        lastPosBlockOffset = -1;
      }
      if (posBufferUpto > 0) {       
        // TODO: should we send offsets/payloads to
        // .pay...?  seems wasteful (have to store extra
        // vLong for low (< BLOCK_SIZE) DF terms = vast vast
        // majority)

        // vInt encode the remaining positions/payloads/offsets:
        int lastPayloadLength = -1;  // force first payload length to be written
        int lastOffsetLength = -1;   // force first offset length to be written
        int payloadBytesReadUpto = 0;
        for(int i=0;i<posBufferUpto;i++) { // 词的个数
          final int posDelta = (int) posDeltaBuffer[i]; // 只是把需要的前几个取出来
          if (writePayloads) {
            final int payloadLength = (int) payloadLengthBuffer[i];
            if (payloadLength != lastPayloadLength) {
              lastPayloadLength = payloadLength;
              posOut.writeVInt((posDelta<<1)|1);
              posOut.writeVInt(payloadLength);
            } else {
              posOut.writeVInt(posDelta<<1);
            }

            if (payloadLength != 0) {
              posOut.writeBytes(payloadBytes, payloadBytesReadUpto, payloadLength);
              payloadBytesReadUpto += payloadLength;
            }
          } else {
            posOut.writeVInt(posDelta);// 向pos文件写入
          }

          if (writeOffsets) {// 写入offset
            int delta = (int) offsetStartDeltaBuffer[i];
            int length = (int) offsetLengthBuffer[i];
            if (length == lastOffsetLength) {// 这里做了压缩处理
              posOut.writeVInt(delta << 1);// 和上一个相等的话，最后一位为0
            } else {
              posOut.writeVInt(delta << 1 | 1);//不等的话，最后一位为1，
              posOut.writeVInt(length);// 继续存储length长度。
              lastOffsetLength = length;
            }
          }
        }

        if (writePayloads) {
          assert payloadBytesReadUpto == payloadByteUpto;
          payloadByteUpto = 0;
        }
      }
    } else {
      lastPosBlockOffset = -1;
    }

    long skipOffset;//统计了doc装的跳跃表大小
    if (docCount > BLOCK_SIZE) {// 大于一个block后，就产生的有跳跃表了。
      skipOffset = skipWriter.writeSkip(docOut) - docStartFP;//skipWriter.writeSkip会将跳跃表信息写入doc中。
    } else {
      skipOffset = -1;
    }

    state.docStartFP = docStartFP;
    state.posStartFP = posStartFP;
    state.payStartFP = payStartFP;
    state.singletonDocID = singletonDocID;
    state.skipOffset = skipOffset; // 该term存储docId和词频占用doc文件的长度,有了长度+doc起始位置，就可以算出跳表起始位置
    state.lastPosBlockOffset = lastPosBlockOffset;
    docBufferUpto = 0;
    posBufferUpto = 0;
    lastDocID = 0;
    docCount = 0;
  }
  
  @Override // out：RAMOutputStream
  public void encodeTerm(DataOutput out, FieldInfo fieldInfo, BlockTermState _state, boolean absolute) throws IOException {
    IntBlockTermState state = (IntBlockTermState)_state;
    if (absolute) {
      lastState = emptyState;
      assert lastState.docStartFP == 0;
    }

    if (lastState.singletonDocID != -1 && state.singletonDocID != -1 && state.docStartFP == lastState.docStartFP) {
      // With runs of rare values such as ID fields, the increment of pointers in the docs file is often 0.
      // Furthermore some ID schemes like auto-increment IDs or Flake IDs are monotonic, so we encode the delta
      // between consecutive doc IDs to save space.
      final long delta = (long) state.singletonDocID - lastState.singletonDocID;
      out.writeVLong((BitUtil.zigZagEncode(delta) << 1) | 0x01);
    } else {
      out.writeVLong((state.docStartFP - lastState.docStartFP) << 1);// 两个词在doc中起始位置差值
      if (state.singletonDocID != -1) {
        out.writeVInt(state.singletonDocID);
      }
    }

    if (writePositions) {
      out.writeVLong(state.posStartFP - lastState.posStartFP);
      if (writePayloads || writeOffsets) {
        out.writeVLong(state.payStartFP - lastState.payStartFP);
      }
    }
    if (writePositions) {
      if (state.lastPosBlockOffset != -1) {
        out.writeVLong(state.lastPosBlockOffset);// 保存的是最后不足一个block(128个词)时，pos中文件位置
      }
    }
    if (state.skipOffset != -1) {
      out.writeVLong(state.skipOffset);
    }
    lastState = state;
  }

  @Override
  public void close() throws IOException {
    // TODO: add a finish() at least to PushBase? DV too...?
    boolean success = false;
    try {
      if (docOut != null) {
        CodecUtil.writeFooter(docOut);
      }
      if (posOut != null) {
        CodecUtil.writeFooter(posOut);
      }
      if (payOut != null) {
        CodecUtil.writeFooter(payOut);
      }
      success = true;
    } finally {
      if (success) {
        IOUtils.close(docOut, posOut, payOut);
      } else {
        IOUtils.closeWhileHandlingException(docOut, posOut, payOut);
      }
      docOut = posOut = payOut = null;
    }
  }
}
