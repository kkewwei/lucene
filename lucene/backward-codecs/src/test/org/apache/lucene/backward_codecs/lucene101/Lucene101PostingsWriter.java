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
package org.apache.lucene.backward_codecs.lucene101;

import static org.apache.lucene.backward_codecs.lucene101.Lucene101PostingsFormat.*;
import static org.apache.lucene.backward_codecs.lucene101.Lucene101PostingsFormat.DOC_CODEC;
import static org.apache.lucene.backward_codecs.lucene101.Lucene101PostingsFormat.LEVEL1_MASK;
import static org.apache.lucene.backward_codecs.lucene101.Lucene101PostingsFormat.META_CODEC;
import static org.apache.lucene.backward_codecs.lucene101.Lucene101PostingsFormat.PAY_CODEC;
import static org.apache.lucene.backward_codecs.lucene101.Lucene101PostingsFormat.POS_CODEC;
import static org.apache.lucene.backward_codecs.lucene101.Lucene101PostingsFormat.TERMS_CODEC;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.lucene.backward_codecs.lucene101.Lucene101PostingsFormat.IntBlockTermState;
import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.CompetitiveImpactAccumulator;
import org.apache.lucene.codecs.Impact;
import org.apache.lucene.codecs.PushPostingsWriterBase;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;

/** Writer for {@link Lucene101PostingsFormat}. */
public class Lucene101PostingsWriter extends PushPostingsWriterBase {

  static final IntBlockTermState EMPTY_STATE = new IntBlockTermState();

  private final int version;

  IndexOutput metaOut; // fieldname for small metadata
  IndexOutput docOut;// 保留包含每个Term的文档列表
  IndexOutput posOut; //
  IndexOutput payOut;// 主要放若有.pay或者offset才进来

  IntBlockTermState lastState;

  // Holds starting file pointers for current term:
  private long docStartFP;// 这个单词在写入doc文件前，记录的doc绝对起始位置
  private long posStartFP;// 这个单词在写入doc文件前，记录的doc绝对起始位置
  private long payStartFP;// 这个单词在写入doc文件前，记录的doc绝对起始位置

  final int[] docDeltaBuffer;// 存放docDelta值
  final int[] freqBuffer;// 词频
  private int docBufferUpto;// 缓存的文档数。128个文档即为一个block

  final int[] posDeltaBuffer;
  final int[] payloadLengthBuffer;
  final int[] offsetStartDeltaBuffer;
  final int[] offsetLengthBuffer;
  private int posBufferUpto;

  private byte[] payloadBytes;
  private int payloadByteUpto;

  private int level0LastDocID;
  private long level0LastPosFP;
  private long level0LastPayFP;

  private int level1LastDocID;
  private long level1LastPosFP;
  private long level1LastPayFP;

  private int docID;// 当前处理到一个field在这个segment的哪个文档了
  private int lastDocID;// 这个segment粒度的
  private int lastPosition;
  private int lastStartOffset;
  private int docCount; // 一个term的总共的文档数

  private final PForUtil pforUtil;
  private final ForDeltaUtil forDeltaUtil;

  private boolean fieldHasNorms;
  private NumericDocValues norms;
  private final CompetitiveImpactAccumulator level0FreqNormAccumulator =
      new CompetitiveImpactAccumulator();
  private final CompetitiveImpactAccumulator level1CompetitiveFreqNormAccumulator =
      new CompetitiveImpactAccumulator();

  private int maxNumImpactsAtLevel0;// 统计最大个数的maxNumImpactsAtLevel0
  private int maxImpactNumBytesAtLevel0;// 统计内存占用最大的长度
  private int maxNumImpactsAtLevel1;
  private int maxImpactNumBytesAtLevel1;
 // 实际会写入level0Output
  /** Scratch output that we use to be able to prepend the encoded length, e.g. impacts. */
  private final ByteBuffersDataOutput scratchOutput = ByteBuffersDataOutput.newResettableInstance();
  // 存储的数据会转到level1Output中
  /**
   * Output for a single block. This is useful to be able to prepend skip data before each block,
   * which can only be computed once the block is encoded. The content is then typically copied to
   * {@link #level1Output}.
   */
  private final ByteBuffersDataOutput level0Output = ByteBuffersDataOutput.newResettableInstance();
   //存储的数据会转到doc中，一个doc结束了才会往doc中写，level1
  /**
   * Output for groups of 32 blocks. This is useful to prepend skip data for these 32 blocks, which
   * can only be done once we have encoded these 32 blocks. The content is then typically copied to
   * {@link #docCount}.
   */
  private final ByteBuffersDataOutput level1Output = ByteBuffersDataOutput.newResettableInstance();

  /**
   * Reusable FixedBitSet, for dense blocks that are more efficiently stored by storing them as a
   * bit set than as packed deltas.
   */
  // Since we use a bit set when it's more storage-efficient, the bit set cannot have more than
  // BLOCK_SIZE*32 bits, which is the maximum possible storage requirement with FOR.
  private final FixedBitSet spareBitSet = new FixedBitSet(BLOCK_SIZE * Integer.SIZE);//

  /** Sole public constructor. */
  public Lucene101PostingsWriter(SegmentWriteState state) throws IOException {
    this(state, Lucene101PostingsFormat.VERSION_CURRENT);
  }

  /** Constructor that takes a version. */
  Lucene101PostingsWriter(SegmentWriteState state, int version) throws IOException {
    this.version = version;
    String metaFileName =
        IndexFileNames.segmentFileName(// _48_Lucene101_0.psm
            state.segmentInfo.name, state.segmentSuffix, Lucene101PostingsFormat.META_EXTENSION);
    String docFileName =// _48_Lucene101_0.doc
        IndexFileNames.segmentFileName(
            state.segmentInfo.name, state.segmentSuffix, Lucene101PostingsFormat.DOC_EXTENSION);
    metaOut = state.directory.createOutput(metaFileName, state.context);
    IndexOutput posOut = null;
    IndexOutput payOut = null;
    boolean success = false;
    try {
      docOut = state.directory.createOutput(docFileName, state.context);
      CodecUtil.writeIndexHeader(
          metaOut, META_CODEC, version, state.segmentInfo.getId(), state.segmentSuffix);
      CodecUtil.writeIndexHeader(
          docOut, DOC_CODEC, version, state.segmentInfo.getId(), state.segmentSuffix);
      forDeltaUtil = new ForDeltaUtil();
      pforUtil = new PForUtil();
      if (state.fieldInfos.hasProx()) {// 若有_pos
        posDeltaBuffer = new int[BLOCK_SIZE];
        String posFileName =
            IndexFileNames.segmentFileName(
                state.segmentInfo.name, state.segmentSuffix, Lucene101PostingsFormat.POS_EXTENSION);
        posOut = state.directory.createOutput(posFileName, state.context);// _48_Lucene101_0.pos
        CodecUtil.writeIndexHeader(
            posOut, POS_CODEC, version, state.segmentInfo.getId(), state.segmentSuffix);

        if (state.fieldInfos.hasPayloads()) {
          payloadBytes = new byte[128];
          payloadLengthBuffer = new int[BLOCK_SIZE];
        } else {
          payloadBytes = null;
          payloadLengthBuffer = null;
        }

        if (state.fieldInfos.hasOffsets()) {
          offsetStartDeltaBuffer = new int[BLOCK_SIZE];
          offsetLengthBuffer = new int[BLOCK_SIZE];
        } else {
          offsetStartDeltaBuffer = null;
          offsetLengthBuffer = null;
        }

        if (state.fieldInfos.hasPayloads() || state.fieldInfos.hasOffsets()) {// 若有.pay或者offset才进来
          String payFileName =
              IndexFileNames.segmentFileName(
                  state.segmentInfo.name,
                  state.segmentSuffix,
                  Lucene101PostingsFormat.PAY_EXTENSION);
          payOut = state.directory.createOutput(payFileName, state.context);
          CodecUtil.writeIndexHeader(
              payOut, PAY_CODEC, version, state.segmentInfo.getId(), state.segmentSuffix);
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
        IOUtils.closeWhileHandlingException(metaOut, docOut, posOut, payOut);
      }
    }

    docDeltaBuffer = new int[BLOCK_SIZE];
    freqBuffer = new int[BLOCK_SIZE];
  }

  @Override
  public IntBlockTermState newTermState() {
    return new IntBlockTermState();
  }

  @Override
  public void init(IndexOutput termsOut, SegmentWriteState state) throws IOException {
    CodecUtil.writeIndexHeader(
        termsOut, TERMS_CODEC, version, state.segmentInfo.getId(), state.segmentSuffix);
    termsOut.writeVInt(BLOCK_SIZE);
  }

  @Override
  public void setField(FieldInfo fieldInfo) {
    super.setField(fieldInfo);
    lastState = EMPTY_STATE;
    fieldHasNorms = fieldInfo.hasNorms();
  }

  @Override
  public void startTerm(NumericDocValues norms) {//代表的是某个field的一个term的开始
    docStartFP = docOut.getFilePointer();
    if (writePositions) {
      posStartFP = posOut.getFilePointer();// pos文件起始位置
      level1LastPosFP = level0LastPosFP = posStartFP;
      if (writePayloads || writeOffsets) {
        payStartFP = payOut.getFilePointer();
        level1LastPayFP = level0LastPayFP = payStartFP;
      }
    }
    lastDocID = -1;
    level0LastDocID = -1;
    level1LastDocID = -1;
    this.norms = norms;
    if (writeFreqs) {
      level0FreqNormAccumulator.clear();
    }
  }
    // termDocFreq: 该文档在该词的词频
  @Override
  public void startDoc(int docID, int termDocFreq) throws IOException {
    if (docBufferUpto == BLOCK_SIZE) {//达到128个term，可以组建一个跳表了
      flushDocBlock(false);
      docBufferUpto = 0;
    }

    final int docDelta = docID - lastDocID;

    if (docID < 0 || docDelta <= 0) {
      throw new CorruptIndexException(
          "docs out of order (" + docID + " <= " + lastDocID + " )", docOut);
    }

    docDeltaBuffer[docBufferUpto] = docDelta;
    if (writeFreqs) {
      freqBuffer[docBufferUpto] = termDocFreq;
    }

    this.docID = docID;
    lastPosition = 0;
    lastStartOffset = 0;

    if (writeFreqs) {
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

      level0FreqNormAccumulator.add(termDocFreq, norm);
    }
  }

  @Override
  public void addPosition(int position, BytesRef payload, int startOffset, int endOffset)
      throws IOException {
    if (position > IndexWriter.MAX_POSITION) {
      throw new CorruptIndexException(
          "position="
              + position
              + " is too large (> IndexWriter.MAX_POSITION="
              + IndexWriter.MAX_POSITION
              + ")",
          docOut);
    }
    if (position < 0) {
      throw new CorruptIndexException("position=" + position + " is < 0", docOut);
    }
    posDeltaBuffer[posBufferUpto] = position - lastPosition;
    if (writePayloads) {
      if (payload == null || payload.length == 0) {
        // no payload
        payloadLengthBuffer[posBufferUpto] = 0;
      } else {
        payloadLengthBuffer[posBufferUpto] = payload.length;
        if (payloadByteUpto + payload.length > payloadBytes.length) {
          payloadBytes = ArrayUtil.grow(payloadBytes, payloadByteUpto + payload.length);
        }
        System.arraycopy(
            payload.bytes, payload.offset, payloadBytes, payloadByteUpto, payload.length);
        payloadByteUpto += payload.length;
      }
    }

    if (writeOffsets) {
      assert startOffset >= lastStartOffset;
      assert endOffset >= startOffset;
      offsetStartDeltaBuffer[posBufferUpto] = startOffset - lastStartOffset;
      offsetLengthBuffer[posBufferUpto] = endOffset - startOffset;
      lastStartOffset = startOffset;
    }

    posBufferUpto++;
    lastPosition = position;
    if (posBufferUpto == BLOCK_SIZE) {// 也会检查position是否到了
      pforUtil.encode(posDeltaBuffer, posOut);// 将128个position编码放入.pos文件

      if (writePayloads) {
        pforUtil.encode(payloadLengthBuffer, payOut);
        payOut.writeVInt(payloadByteUpto);
        payOut.writeBytes(payloadBytes, 0, payloadByteUpto);
        payloadByteUpto = 0;
      }
      if (writeOffsets) {
        pforUtil.encode(offsetStartDeltaBuffer, payOut);
        pforUtil.encode(offsetLengthBuffer, payOut);
      }
      posBufferUpto = 0;
    }
  }

  @Override
  public void finishDoc() {
    docBufferUpto++;
    docCount++;

    lastDocID = docID;
  }

  /**
   * Special vints that are encoded on 2 bytes if they require 15 bits or less. VInt becomes
   * especially slow when the number of bytes is variable, so this special layout helps in the case
   * when the number likely requires 15 bits or less
   */
  static void writeVInt15(DataOutput out, int v) throws IOException {
    assert v >= 0;
    writeVLong15(out, v);
  }

  /**
   * @see #writeVInt15(DataOutput, int)
   */
  static void writeVLong15(DataOutput out, long v) throws IOException {
    assert v >= 0;
    if ((v & ~0x7FFFL) == 0) {
      out.writeShort((short) v);
    } else {
      out.writeShort((short) (0x8000 | (v & 0x7FFF)));
      out.writeVLong(v >> 15);
    }
  }
  // 同一个term下面的128个doc
  private void flushDocBlock(boolean finishTerm) throws IOException {
    assert docBufferUpto != 0;

    if (docBufferUpto < BLOCK_SIZE) {//说明是最后的term
      assert finishTerm;
      PostingsUtil.writeVIntBlock(//把docDeltaBuffer往level0Output写入
          level0Output, docDeltaBuffer, freqBuffer, docBufferUpto, writeFreqs);
    } else {// 文档个数128个doc
      if (writeFreqs) {
        List<Impact> impacts = level0FreqNormAccumulator.getCompetitiveFreqNormPairs();// 查找递增的词频
        if (impacts.size() > maxNumImpactsAtLevel0) {
          maxNumImpactsAtLevel0 = impacts.size();// 统计最大的maxNumImpactsAtLevel0
        }// 此时scratchOutput是空的
        writeImpacts(impacts, scratchOutput);//第一次scratchOutput写入了List<Impact>
        assert level0Output.size() == 0;
        if (scratchOutput.size() > maxImpactNumBytesAtLevel0) {
          maxImpactNumBytesAtLevel0 = Math.toIntExact(scratchOutput.size());
        }//此时level0Output也是空闲的
        level0Output.writeVLong(scratchOutput.size());//第一次将scratchOutput转移到level0：impacts的byte个数
        scratchOutput.copyTo(level0Output);// impacts每个具体的值
        scratchOutput.reset();
        if (writePositions) {// 有positions
          level0Output.writeVLong(posOut.getFilePointer() - level0LastPosFP);//第二次scratchOutput写入pos的位移量
          level0Output.writeByte((byte) posBufferUpto);// 第二次scratchOutput写入pos缓存量
          level0LastPosFP = posOut.getFilePointer();

          if (writeOffsets || writePayloads) {
            level0Output.writeVLong(payOut.getFilePointer() - level0LastPayFP);
            level0Output.writeVInt(payloadByteUpto);
            level0LastPayFP = payOut.getFilePointer();
          }
        }
      }//可以跳过position+offset的长度
      long numSkipBytes = level0Output.size();//记录第二个次level0Output的长度
      // Now we need to decide whether to encode block deltas as packed integers (FOR) or unary
      // codes (bit set). FOR makes #nextDoc() a bit faster while the bit set approach makes
      // #advance() usually faster and #intoBitSet() much faster. In the end, we make the decision
      // based on storage requirements, picking the bit set approach whenever it's more
      // storage-efficient than the next number of bits per value (which effectively slightly biases
      // towards the bit set approach).
      int bitsPerValue = forDeltaUtil.bitsRequired(docDeltaBuffer);
      int sum = Math.toIntExact(Arrays.stream(docDeltaBuffer).sum());// 求和， 最大文档Id
      int numBitSetLongs = FixedBitSet.bits2words(sum);//根据最大文档，需要多少个long来存储
      int numBitsNextBitsPerValue = Math.min(Integer.SIZE, bitsPerValue + 1) * BLOCK_SIZE;
      if (sum == BLOCK_SIZE) {// 文档个数是连续的
        level0Output.writeByte((byte) 0);
      } else if (version < VERSION_DENSE_BLOCKS_AS_BITSETS || numBitsNextBitsPerValue <= sum) { // perPervalaue方式存储更划算
        level0Output.writeByte((byte) bitsPerValue);// 使用bitsPerValue存储
        forDeltaUtil.encodeDeltas(bitsPerValue, docDeltaBuffer, level0Output);
      } else {// 使用bitsit来装
        // Storing doc deltas is more efficient using unary coding (ie. storing doc IDs as a bit
        // set)
        spareBitSet.clear(0, numBitSetLongs << 6);
        int s = -1;
        for (int i : docDeltaBuffer) {
          s += i;
          spareBitSet.set(s);
        }
        // We never use the bit set encoding when it requires more than Integer.SIZE=32 bits per
        // value. So the bit set cannot have more than BLOCK_SIZE * Integer.SIZE / Long.SIZE = 64
        // longs, which fits on a byte.
        assert numBitSetLongs <= BLOCK_SIZE / 2;
        level0Output.writeByte((byte) -numBitSetLongs);
        for (int i = 0; i < numBitSetLongs; ++i) {
          level0Output.writeLong(spareBitSet.getBits()[i]);
        }
      }

      if (writeFreqs) {
        pforUtil.encode(freqBuffer, level0Output);// 频率也写入level0Output
      }

      // docID - lastBlockDocID is at least 128, so it can never fit a single byte with a vint
      // Even if we subtracted 128, only extremely dense blocks would be eligible to a single byte
      // so let's go with 2 bytes right away
      writeVInt15(scratchOutput, docID - level0LastDocID);// 此时scratchOutput还是空的，第一次记录deltaDocId
      writeVLong15(scratchOutput, level0Output.size());//把level0Output的size放入scratchOutput
      numSkipBytes += scratchOutput.size();
      level1Output.writeVLong(numSkipBytes);// 通过numSkipBytes可以跳过这个block大部分
      scratchOutput.copyTo(level1Output);// 写入level1的
      scratchOutput.reset();// 用一次清一次
    }

    level0Output.copyTo(level1Output);// 此时level0Output包含：
    level0Output.reset();// 清了level0Output
    level0LastDocID = docID;
    if (writeFreqs) {
      level1CompetitiveFreqNormAccumulator.addAll(level0FreqNormAccumulator);// 转移到level1Competitive
      level0FreqNormAccumulator.clear();// 转移下
    }
    // 4096个文档了
    if ((docCount & LEVEL1_MASK) == 0) { // true every 32 blocks (4,096 docs) 每隔32个block，构建一次
      writeLevel1SkipData();
      level1LastDocID = docID;
      level1CompetitiveFreqNormAccumulator.clear();
    } else if (finishTerm) {// 这个term结束了？
      level1Output.copyTo(docOut);// 写到了docOut中了
      level1Output.reset();
      level1CompetitiveFreqNormAccumulator.clear();
    }// 128个doc一般都跳过了
  }
  // 每隔4096个doc，构建一次level1
  private void writeLevel1SkipData() throws IOException {
    docOut.writeVInt(docID - level1LastDocID);// 写到了docOut中了，二级索引docId
    final long level1End;
    if (writeFreqs) {
      List<Impact> impacts = level1CompetitiveFreqNormAccumulator.getCompetitiveFreqNormPairs();
      if (impacts.size() > maxNumImpactsAtLevel1) {//统计level最大的num的Impacts
        maxNumImpactsAtLevel1 = impacts.size();
      }
      writeImpacts(impacts, scratchOutput);// 写下全局的可以匹配的impacts的
      long numImpactBytes = scratchOutput.size();// 记录下impacts的长度
      if (numImpactBytes > maxImpactNumBytesAtLevel1) {
        maxImpactNumBytesAtLevel1 = Math.toIntExact(numImpactBytes);
      }
      if (writePositions) {//一般包含
        scratchOutput.writeVLong(posOut.getFilePointer() - level1LastPosFP);// pos文件偏移量，最后会写到doc中
        scratchOutput.writeByte((byte) posBufferUpto);
        level1LastPosFP = posOut.getFilePointer();
        if (writeOffsets || writePayloads) {
          scratchOutput.writeVLong(payOut.getFilePointer() - level1LastPayFP);
          scratchOutput.writeVInt(payloadByteUpto);
          level1LastPayFP = payOut.getFilePointer();
        }
      }// 2个short分别指下面的numImpactBytes和scratchOutput.size() + Short.BYTES)
      final long level1Len = 2 * Short.BYTES + scratchOutput.size() + level1Output.size();// 32个level0的长度+（二级索引结构）
      docOut.writeVLong(level1Len);// level1的文件偏移量
      level1End = docOut.getFilePointer() + level1Len;
      // There are at most 128 impacts, that require at most 2 bytes each
      assert numImpactBytes <= Short.MAX_VALUE;
      // Like impacts plus a few vlongs, still way under the max short value
      assert scratchOutput.size() + Short.BYTES <= Short.MAX_VALUE;
      docOut.writeShort((short) (scratchOutput.size() + Short.BYTES));  // 就是  Impact+positions等内容
      docOut.writeShort((short) numImpactBytes);
      scratchOutput.copyTo(docOut);
      scratchOutput.reset();// 清了临时变量
    } else {
      docOut.writeVLong(level1Output.size());
      level1End = docOut.getFilePointer() + level1Output.size();
    }
    level1Output.copyTo(docOut);
    level1Output.reset();// 也清了临时变量
    assert docOut.getFilePointer() == level1End : docOut.getFilePointer() + " " + level1End;
  }

  static void writeImpacts(Collection<Impact> impacts, DataOutput out) throws IOException {
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
   // 一个term的所有doc处理完了
  /** Called when we are done adding docs to this term */
  @Override
  public void finishTerm(BlockTermState _state) throws IOException {
    IntBlockTermState state = (IntBlockTermState) _state;
    assert state.docFreq > 0;

    // TODO: wasteful we are counting this (counting # docs
    // for this term) in two places?
    assert state.docFreq == docCount : state.docFreq + " vs " + docCount;

    // docFreq == 1, don't write the single docid/freq to a separate file along with a pointer to
    // it.
    final int singletonDocID;
    if (state.docFreq == 1) {// 这个term只在一个doc中出现过
      // pulse the singleton docid into the term dictionary, freq is implicitly totalTermFreq
      singletonDocID = docDeltaBuffer[0] - 1;// 那么这个保存的是这个docId
    } else {
      singletonDocID = -1;
      flushDocBlock(true);// 未刷盘的文档给刷盘了
    }

    final long lastPosBlockOffset;

    if (writePositions) {
      // totalTermFreq is just total number of positions(or payloads, or offsets)
      // associated with current term.
      assert state.totalTermFreq != -1;
      if (state.totalTermFreq > BLOCK_SIZE) {
        // record file offset for last pos in last block
        lastPosBlockOffset = posOut.getFilePointer() - posStartFP;
      } else {
        lastPosBlockOffset = -1;
      }
      if (posBufferUpto > 0) {
        assert posBufferUpto < BLOCK_SIZE;
        // TODO: should we send offsets/payloads to
        // .pay...?  seems wasteful (have to store extra
        // vLong for low (< BLOCK_SIZE) DF terms = vast vast
        // majority)

        // vInt encode the remaining positions/payloads/offsets:
        int lastPayloadLength = -1; // force first payload length to be written
        int lastOffsetLength = -1; // force first offset length to be written
        int payloadBytesReadUpto = 0;
        for (int i = 0; i < posBufferUpto; i++) {// 缓存的pos
          final int posDelta = posDeltaBuffer[i];
          if (writePayloads) {
            final int payloadLength = payloadLengthBuffer[i];
            if (payloadLength != lastPayloadLength) {
              lastPayloadLength = payloadLength;
              posOut.writeVInt((posDelta << 1) | 1);
              posOut.writeVInt(payloadLength);
            } else {
              posOut.writeVInt(posDelta << 1);
            }

            if (payloadLength != 0) {
              posOut.writeBytes(payloadBytes, payloadBytesReadUpto, payloadLength);
              payloadBytesReadUpto += payloadLength;
            }
          } else {
            posOut.writeVInt(posDelta);
          }

          if (writeOffsets) {
            int delta = offsetStartDeltaBuffer[i];
            int length = offsetLengthBuffer[i];
            if (length == lastOffsetLength) {
              posOut.writeVInt(delta << 1);
            } else {
              posOut.writeVInt(delta << 1 | 1);
              posOut.writeVInt(length);
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

    state.docStartFP = docStartFP; //记录下此时的doc，pos，pay等位置信息
    state.posStartFP = posStartFP;
    state.payStartFP = payStartFP;
    state.singletonDocID = singletonDocID;

    state.lastPosBlockOffset = lastPosBlockOffset;
    docBufferUpto = 0;
    posBufferUpto = 0;
    lastDocID = -1;
    docCount = 0;
  }

  @Override
  public void encodeTerm(
      DataOutput out, FieldInfo fieldInfo, BlockTermState _state, boolean absolute)
      throws IOException {
    IntBlockTermState state = (IntBlockTermState) _state;
    if (absolute) {// 是第一个
      lastState = EMPTY_STATE;
      assert lastState.docStartFP == 0;
    }

    if (lastState.singletonDocID != -1
        && state.singletonDocID != -1
        && state.docStartFP == lastState.docStartFP) {
      // With runs of rare values such as ID fields, the increment of pointers in the docs file is
      // often 0.
      // Furthermore some ID schemes like auto-increment IDs or Flake IDs are monotonic, so we
      // encode the delta
      // between consecutive doc IDs to save space.
      final long delta = (long) state.singletonDocID - lastState.singletonDocID;
      out.writeVLong((BitUtil.zigZagEncode(delta) << 1) | 0x01);
    } else {
      out.writeVLong((state.docStartFP - lastState.docStartFP) << 1);// 记录这个doc的起始位置
      if (state.singletonDocID != -1) {// 这个termId只出现在一个doc中
        out.writeVInt(state.singletonDocID);// 将doc本身写入tim文件
      }
    }

    if (writePositions) {
      out.writeVLong(state.posStartFP - lastState.posStartFP);// 记录上一个term的pos长度
      if (writePayloads || writeOffsets) {
        out.writeVLong(state.payStartFP - lastState.payStartFP);
      }
    }
    if (writePositions) {
      if (state.lastPosBlockOffset != -1) {
        out.writeVLong(state.lastPosBlockOffset);
      }
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
      if (metaOut != null) {
        metaOut.writeInt(maxNumImpactsAtLevel0);
        metaOut.writeInt(maxImpactNumBytesAtLevel0);
        metaOut.writeInt(maxNumImpactsAtLevel1);
        metaOut.writeInt(maxImpactNumBytesAtLevel1);
        metaOut.writeLong(docOut.getFilePointer());
        if (posOut != null) {
          metaOut.writeLong(posOut.getFilePointer());
          if (payOut != null) {
            metaOut.writeLong(payOut.getFilePointer());
          }
        }
        CodecUtil.writeFooter(metaOut);
      }
      success = true;
    } finally {
      if (success) {
        IOUtils.close(metaOut, docOut, posOut, payOut);
      } else {
        IOUtils.closeWhileHandlingException(metaOut, docOut, posOut, payOut);
      }
      metaOut = docOut = posOut = payOut = null;
    }
  }
}
