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
package org.apache.lucene.codecs.compressing;


import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.codecs.TermVectorsWriter;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.GrowableByteArrayDataOutput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.packed.BlockPackedWriter;
import org.apache.lucene.util.packed.PackedInts;

/**
 * {@link TermVectorsWriter} for {@link CompressingTermVectorsFormat}.
 * @lucene.experimental // 单个文档形式的，tvd和fdt都是一样存储方式
 */ // 对应Term vector索引的Writer，底层是压缩Block格式。每次刷新一次就清空一次，下次再写入时，再重新生成
public final class CompressingTermVectorsWriter extends TermVectorsWriter {
   // 会产生tvd、tvm、tvx。和CompressingStoredFieldsWriter 的fdt、fdm、fdx的作用是一样的。
  // hard limit on the maximum number of documents per chunk
  static final int MAX_DOCUMENTS_PER_CHUNK = 128;

  static final String VECTORS_EXTENSION = "tvd";
  static final String VECTORS_INDEX_EXTENSION_PREFIX = "tv";
  static final String VECTORS_INDEX_CODEC_NAME = "Lucene85TermVectorsIndex";

  static final int VERSION_START = 1;
  static final int VERSION_OFFHEAP_INDEX = 2;
  static final int VERSION_CURRENT = VERSION_OFFHEAP_INDEX;

  static final int PACKED_BLOCK_SIZE = 64;

  static final int POSITIONS = 0x01;
  static final int   OFFSETS = 0x02;
  static final int  PAYLOADS = 0x04;
  static final int FLAGS_BITS = PackedInts.bitsRequired(POSITIONS | OFFSETS | PAYLOADS);

  private final String segment;
  private FieldsIndexWriter indexWriter; // 就是FieldsIndexWriter
  private IndexOutput vectorsStream;// _0.tvd

  private final CompressionMode compressionMode;
  private final Compressor compressor;
  private final int chunkSize;  // 4096
  
  private long numChunks; // number of compressed blocks written
  private long numDirtyChunks; // number of incomplete compressed blocks written

  /** a pending doc */
  private class DocData {
    final int numFields; // 每个文档的域个数
    final Deque<FieldData> fields; // 每个域都有一个
    final int posStart, offStart, payStart;
    DocData(int numFields, int posStart, int offStart, int payStart) {
      this.numFields = numFields;
      this.fields = new ArrayDeque<>(numFields);
      this.posStart = posStart; // 文档最开始全部为0, 所有文档拉开，上个域的结束位置，自然也是本域的开始位置
      this.offStart = offStart;// 文档最开始全部为0
      this.payStart = payStart;// 文档最开始全部为0
    }
    FieldData addField(int fieldNum, int numTerms, boolean positions, boolean offsets, boolean payloads) {
      final FieldData field;
      if (fields.isEmpty()) {
        field = new FieldData(fieldNum, numTerms, positions, offsets, payloads, posStart, offStart, payStart);
      } else {
        final FieldData last = fields.getLast(); // 该文档上个域
        final int posStart = last.posStart + (last.hasPositions ? last.totalPositions : 0); // 该文档上一个域
        final int offStart = last.offStart + (last.hasOffsets ? last.totalPositions : 0);  //
        final int payStart = last.payStart + (last.hasPayloads ? last.totalPositions : 0); // 为0
        field = new FieldData(fieldNum, numTerms, positions, offsets, payloads, posStart, offStart, payStart);
      }
      fields.add(field);
      return field;
    }
  }

  private DocData addDocData(int numVectorFields) { // 把文档加入到了pendingDocs中
    FieldData last = null;
    for (Iterator<DocData> it = pendingDocs.descendingIterator(); it.hasNext(); ) {
      final DocData doc = it.next();
      if (!doc.fields.isEmpty()) {
        last = doc.fields.getLast(); // 最后一个文档最后一个域
        break;
      }
    }
    final DocData doc;
    if (last == null) { // 是新的docData, 存放
      doc = new DocData(numVectorFields, 0, 0, 0);
    } else {
      final int posStart = last.posStart + (last.hasPositions ? last.totalPositions : 0); // 当前position最开始是在上个存储的最低+上个的词个数
      final int offStart = last.offStart + (last.hasOffsets ? last.totalPositions : 0);
      final int payStart = last.payStart + (last.hasPayloads ? last.totalPositions : 0);
      doc = new DocData(numVectorFields, posStart, offStart, payStart);
    }
    pendingDocs.add(doc); // 尾插法，仅仅放的是空壳
    return doc;
  }

  /** a pending field */  // 每个文档每个域都有一个
  private class FieldData {
    final boolean hasPositions, hasOffsets, hasPayloads;
    final int fieldNum, flags, numTerms; // 字段编号， 该域的标志位（POSITIONS|OFFSETS|PAYLOADS），域里面distinct字段个数
    final int[] freqs, prefixLengths, suffixLengths; // segment打通后所有单词（相同词算两个的长度。每个单独的词都对应一个元素，相同的前缀长度，不同的后缀长度
    final int posStart, offStart, payStart; //  segment打通后所有文档所有域所有单词（相同词算两个）总个数，实际posStart、offStart值都是相等的
    int totalPositions; // 当前文档当前域所有词（相同词算两个）编号。每处理一个position就+1
    int ord; // 第几个词
    FieldData(int fieldNum, int numTerms, boolean positions, boolean offsets, boolean payloads,
        int posStart, int offStart, int payStart) {
      this.fieldNum = fieldNum; // 域编号
      this.numTerms = numTerms;
      this.hasPositions = positions;
      this.hasOffsets = offsets;
      this.hasPayloads = payloads;
      this.flags = (positions ? POSITIONS : 0) | (offsets ? OFFSETS : 0) | (payloads ? PAYLOADS : 0); // 3
      this.freqs = new int[numTerms];
      this.prefixLengths = new int[numTerms];
      this.suffixLengths = new int[numTerms];
      this.posStart = posStart;
      this.offStart = offStart;
      this.payStart = payStart;
      totalPositions = 0;
      ord = 0; // 排序后好的第几个词
    } // 存储字段内容， 在finish阶段
    void addTerm(int freq, int prefixLength, int suffixLength) {
      freqs[ord] = freq;
      prefixLengths[ord] = prefixLength; // 相同的前缀长度
      suffixLengths[ord] = suffixLength; // 不同的后缀长度
      ++ord;
    } // 存储position
    void addPosition(int position, int startOffset, int length, int payloadLength) {
      if (hasPositions) {
        if (posStart + totalPositions == positionsBuf.length) {
          positionsBuf = ArrayUtil.grow(positionsBuf);
        }
        positionsBuf[posStart + totalPositions] = position;
      }
      if (hasOffsets) {
        if (offStart + totalPositions == startOffsetsBuf.length) {
          final int newLength = ArrayUtil.oversize(offStart + totalPositions, 4);
          startOffsetsBuf = ArrayUtil.growExact(startOffsetsBuf, newLength);
          lengthsBuf = ArrayUtil.growExact(lengthsBuf, newLength);
        }
        startOffsetsBuf[offStart + totalPositions] = startOffset;
        lengthsBuf[offStart + totalPositions] = length;
      }
      if (hasPayloads) {
        if (payStart + totalPositions == payloadLengthsBuf.length) {
          payloadLengthsBuf = ArrayUtil.grow(payloadLengthsBuf);
        }
        payloadLengthsBuf[payStart + totalPositions] = payloadLength;
      }
      ++totalPositions;
    }
  }

  private int numDocs; // total number of docs seen ，目前总共写入的文档数
  private final Deque<DocData> pendingDocs; // pending docs  将tv写入别的对象中了的个数，每128个文档会清空一次
  private DocData curDoc; // current document, 能放很多FieldData。
  private FieldData curField; // current field, 每个文档每个域都会生成一个
  private final BytesRef lastTerm; // 当前文档当前域的上一个词，写完了这个域就清空了
  private int[] positionsBuf, startOffsetsBuf, lengthsBuf, payloadLengthsBuf; // 存放所有词（相同词算两次）的位置信息，起始位置offset、词的长度，。。。。。。一个segment共享一个
  private final GrowableByteArrayDataOutput termSuffixes; // buffered term suffixes // 只存储这个词不相同的后缀，一个链共享一个，每128个文档会清空一次
  private final GrowableByteArrayDataOutput payloadBytes; // buffered term payloads
  private final BlockPackedWriter writer;  // BlockPackedWriter
  //是在写入完成时，还没有调用commit时调该函数
  /** Sole constructor. */ // 对应Term vector索引的Writer，底层是压缩Block格式。
  public CompressingTermVectorsWriter(Directory directory, SegmentInfo si, String segmentSuffix, IOContext context,
      String formatName, CompressionMode compressionMode, int chunkSize, int blockShift) throws IOException {
    assert directory != null;
    this.segment = si.name;
    this.compressionMode = compressionMode;
    this.compressor = compressionMode.newCompressor();
    this.chunkSize = chunkSize; // 4096

    numDocs = 0;
    pendingDocs = new ArrayDeque<>();
    termSuffixes = new GrowableByteArrayDataOutput(ArrayUtil.oversize(chunkSize, 1));
    payloadBytes = new GrowableByteArrayDataOutput(ArrayUtil.oversize(1, 1));
    lastTerm = new BytesRef(ArrayUtil.oversize(30, 1));

    boolean success = false;
    try { //  产生 _0.tvd文件
      vectorsStream = directory.createOutput(IndexFileNames.segmentFileName(segment, segmentSuffix, VECTORS_EXTENSION),
                                                     context);
      CodecUtil.writeIndexHeader(vectorsStream, formatName, VERSION_CURRENT, si.getId(), segmentSuffix);
      assert CodecUtil.indexHeaderLength(formatName, segmentSuffix) == vectorsStream.getFilePointer();
      //blockSize 1024
      indexWriter = new FieldsIndexWriter(directory, segment, segmentSuffix, VECTORS_INDEX_EXTENSION_PREFIX, VECTORS_INDEX_CODEC_NAME, si.getId(), blockShift, context);

      vectorsStream.writeVInt(PackedInts.VERSION_CURRENT);
      vectorsStream.writeVInt(chunkSize);
      writer = new BlockPackedWriter(vectorsStream, PACKED_BLOCK_SIZE);

      positionsBuf = new int[1024];
      startOffsetsBuf = new int[1024];
      lengthsBuf = new int[1024];
      payloadLengthsBuf = new int[1024];

      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(vectorsStream, indexWriter, indexWriter);
      }
    }
  }

  @Override
  public void close() throws IOException {
    try {
      IOUtils.close(vectorsStream, indexWriter);
    } finally {
      vectorsStream = null;
      indexWriter = null;
    }
  }

  @Override
  public void startDocument(int numVectorFields) throws IOException {
    curDoc = addDocData(numVectorFields);
  }

  @Override // 在将每个词的nextField的每个stream0/1都解析完了只有
  public void finishDocument() throws IOException {
    // append the payload bytes of the doc after its terms
    termSuffixes.writeBytes(payloadBytes.getBytes(), payloadBytes.getPosition());
    payloadBytes.reset();
    ++numDocs;
    if (triggerFlush()) { //触发的标准是大小4K和128个文档
      flush();
    }
    curDoc = null;
  }

  @Override
  public void startField(FieldInfo info, int numTerms, boolean positions,
      boolean offsets, boolean payloads) throws IOException {
    curField = curDoc.addField(info.number, numTerms, positions, offsets, payloads);
    lastTerm.length = 0; //新开始的一个域都会清空
  }

  @Override
  public void finishField() throws IOException {
    curField = null;
  }
  // finish时，使用了压缩阀，路径始终减少了。主要是存储词字符。每个域每个distinct词在finish时都会进来。
  @Override  //
  public void startTerm(BytesRef term, int freq) throws IOException {
    assert freq >= 1;
    final int prefix;
    if (lastTerm.length == 0) { // 最开始，每个域都是重新开始时，都会长度置为0
      // no previous term: no bytes to write
      prefix = 0; // 与上个词相同的前缀长度  ，仅仅是当前域所有字段的压缩
    } else {
      prefix = StringHelper.bytesDifference(lastTerm, term); // 获取两个term相同的前缀长度
    } // 两个结合就能找到词的内容了
    curField.addTerm(freq, prefix, term.length - prefix); // 存储词频
    termSuffixes.writeBytes(term.bytes, term.offset + prefix, term.length - prefix); // 存储这个词到termSuffixes，后缀存储。相当于已经压缩。。
    // copy last term ， 替换旧词
    if (lastTerm.bytes.length < term.length) { //后面的词长的话，申请大点的位置来存放当前词
      lastTerm.bytes = new byte[ArrayUtil.oversize(term.length, 1)];
    }
    lastTerm.offset = 0;
    lastTerm.length = term.length;
    System.arraycopy(term.bytes, term.offset, lastTerm.bytes, 0, term.length);
  }

  @Override
  public void addPosition(int position, int startOffset, int endOffset,
      BytesRef payload) throws IOException {
    assert curField.flags != 0;
    curField.addPosition(position, startOffset, endOffset - startOffset, payload == null ? 0 : payload.length);
    if (curField.hasPayloads && payload != null) {
      payloadBytes.writeBytes(payload.bytes, payload.offset, payload.length);
    }
  }

  private boolean triggerFlush() {
    return termSuffixes.getPosition() >= chunkSize // 不相同的尾长累加>4k
        || pendingDocs.size() >= MAX_DOCUMENTS_PER_CHUNK;  // 每个term个数达到128个
  }
  // 如果一个chunk满了(128个分片或者16kb)，会向磁盘中刷, 刷新的东西还是蛮多的，很重要的函数
  private void flush() throws IOException {
    final int chunkDocs = pendingDocs.size();
    assert chunkDocs > 0 : chunkDocs;

    // write the index file， 写入_ids_0.tmp  _pointers_1.tmp中
    indexWriter.writeIndex(chunkDocs, vectorsStream.getFilePointer());

    final int docBase = numDocs - chunkDocs;
    vectorsStream.writeVInt(docBase); // 写入tvd
    vectorsStream.writeVInt(chunkDocs);

    // total number of fields of the chunk
    final int totalFields = flushNumFields(chunkDocs);// 该chunk所有文档域个数的累加。一个chunk内所有域累加值

    if (totalFields > 0) {
      // unique field numbers (sorted)
      final int[] fieldNums = flushFieldNums(); //存储该chunk的distinct的域编号，并返回这些distinct
      // offsets in the array of unique field numbers
      flushFields(totalFields, fieldNums);// 存储每个文档每个域在distinct中的序号
      // flags (does the field have positions, offsets, payloads?)
      flushFlags(totalFields, fieldNums); // 写入一个block每个的字段标志位，该域的标志位（POSITIONS|OFFSETS|PAYLOADS）。若字段标志位相同，只用写一次的
      // number of terms of each field
      flushNumTerms(totalFields); // 写入每个文档每个域distinct(term)个数
      // prefix and suffix lengths for each field
      flushTermLengths();// 写入每个文档每个域每个distinct(term)的相同前缀，后缀
      // term freqs - 1 (because termFreq is always >=1) for each term
      flushTermFreqs();// 每个文档每个域每个distinct(term)出现的频次
      // positions for all terms, when enabled
      flushPositions(); // // 每个文档每个域每个词(相同次算两次)的位置信息
      // offsets for all terms, when enabled
      flushOffsets(fieldNums);
      // payload lengths for all terms, when enabled
      flushPayloadLengths();

      // compress terms and payloads and write them to the output
      compressor.compress(termSuffixes.getBytes(), 0, termSuffixes.getPosition(), vectorsStream);
    }

    // reset
    pendingDocs.clear();
    curDoc = null;
    curField = null;
    termSuffixes.reset();
    numChunks++;
  }

  private int flushNumFields(int chunkDocs) throws IOException {
    if (chunkDocs == 1) { //该chunk只有一个文档
      final int numFields = pendingDocs.getFirst().numFields;
      vectorsStream.writeVInt(numFields);
      return numFields;
    } else {
      writer.reset(vectorsStream); // 从头开始统计
      int totalFields = 0; // 该chunk所有文档域个数的累加
      for (DocData dd : pendingDocs) {
        writer.add(dd.numFields);
        totalFields += dd.numFields;
      }
      writer.finish(); // 本次将内存中的一个chunk写入_0.tvd
      return totalFields;// 该chunk所有文档域个数的累加
    }
  }

  /** Returns a sorted array containing unique field numbers */
  private int[] flushFieldNums() throws IOException {
    SortedSet<Integer> fieldNums = new TreeSet<>(); // 所有文档，所有涉及到的域num全部弄出来
    for (DocData dd : pendingDocs) {
      for (FieldData fd : dd.fields) {
        fieldNums.add(fd.fieldNum); // 每个文档域的编号，按照红黑树存储distint的节点
      }
    }

    final int numDistinctFields = fieldNums.size(); // 这一堆文档中，distinct域的个数
    assert numDistinctFields > 0;
    final int bitsRequired = PackedInts.bitsRequired(fieldNums.last()); // 获取的是这个chunk中最大的那个域的编号
    final int token = (Math.min(numDistinctFields - 1, 0x07) << 5) | bitsRequired;
    vectorsStream.writeByte((byte) token);
    if (numDistinctFields - 1 >= 0x07) { // 如果大了，还得重新写入
      vectorsStream.writeVInt(numDistinctFields - 1 - 0x07);
    }
    final PackedInts.Writer writer = PackedInts.getWriterNoHeader(vectorsStream, PackedInts.Format.PACKED, fieldNums.size(), bitsRequired, 1);
    for (Integer fieldNum : fieldNums) { // 将distinct域的编号也写入tvd中
      writer.add(fieldNum);
    }
    writer.finish(); // 完成了这批数据的写入

    int[] fns = new int[fieldNums.size()];
    int i = 0;
    for (Integer key : fieldNums) { // 读取也是按小到大排序的
      fns[i++] = key;
    }
    return fns; // 返回所有distinct的域编码
  }

  private void flushFields(int totalFields, int[] fieldNums) throws IOException {
    final PackedInts.Writer writer = PackedInts.getWriterNoHeader(vectorsStream, PackedInts.Format.PACKED, totalFields, PackedInts.bitsRequired(fieldNums.length - 1), 1);
    for (DocData dd : pendingDocs) { // 遍历每个文档
      for (FieldData fd : dd.fields) {// 遍历每个域
        final int fieldNumIndex = Arrays.binarySearch(fieldNums, fd.fieldNum); // 二叉搜索这个第几个数，fieldNums从小到大排序
        assert fieldNumIndex >= 0;
        writer.add(fieldNumIndex);
      }
    }
    writer.finish(); // 这里才真正写向tvd文件
  }

  private void flushFlags(int totalFields, int[] fieldNums) throws IOException {
    // check if fields always have the same flags
    boolean nonChangingFlags = true; // 检查所有相同的域是否有相同的字段配置，
    int[] fieldFlags = new int[fieldNums.length];
    Arrays.fill(fieldFlags, -1);
    outer:
    for (DocData dd : pendingDocs) {
      for (FieldData fd : dd.fields) {
        final int fieldNumOff = Arrays.binarySearch(fieldNums, fd.fieldNum);
        assert fieldNumOff >= 0;
        if (fieldFlags[fieldNumOff] == -1) {
          fieldFlags[fieldNumOff] = fd.flags;
        } else if (fieldFlags[fieldNumOff] != fd.flags) { // 若flags不一样，说明有问题了
          nonChangingFlags = false;
          break outer;
        }
      }
    }

    if (nonChangingFlags) { //
      // write one flag per field num
      vectorsStream.writeVInt(0); // 标志一样
      final PackedInts.Writer writer = PackedInts.getWriterNoHeader(vectorsStream, PackedInts.Format.PACKED, fieldFlags.length, FLAGS_BITS, 1);
      for (int flags : fieldFlags) {
        assert flags >= 0;
        writer.add(flags); // 只用写一次
      }
      assert writer.ord() == fieldFlags.length - 1;
      writer.finish();
    } else { // 若一个域有不同的字段配置的话
      // write one flag for every field instance
      vectorsStream.writeVInt(1); // 标志不一样
      final PackedInts.Writer writer = PackedInts.getWriterNoHeader(vectorsStream, PackedInts.Format.PACKED, totalFields, FLAGS_BITS, 1);
      for (DocData dd : pendingDocs) {
        for (FieldData fd : dd.fields) {
          writer.add(fd.flags); // 需要单独写入每个字段配置
        }
      }
      assert writer.ord() == totalFields - 1;
      writer.finish();
    }
  }
  // 写入每个文档每个字段独立term的个数
  private void flushNumTerms(int totalFields) throws IOException {
    int maxNumTerms = 0;
    for (DocData dd : pendingDocs) {
      for (FieldData fd : dd.fields) {
        maxNumTerms |= fd.numTerms;
      }
    }
    final int bitsRequired = PackedInts.bitsRequired(maxNumTerms);
    vectorsStream.writeVInt(bitsRequired);
    final PackedInts.Writer writer = PackedInts.getWriterNoHeader(
        vectorsStream, PackedInts.Format.PACKED, totalFields, bitsRequired, 1);
    for (DocData dd : pendingDocs) {
      for (FieldData fd : dd.fields) {
        writer.add(fd.numTerms);
      }
    }
    assert writer.ord() == totalFields - 1;
    writer.finish();
  }
  // 写入每个文档每个域里面distinct词的前缀长度，后缀长度
  private void flushTermLengths() throws IOException {
    writer.reset(vectorsStream);
    for (DocData dd : pendingDocs) {
      for (FieldData fd : dd.fields) {
        for (int i = 0; i < fd.numTerms; ++i) { // 该域有几个单独的词
          writer.add(fd.prefixLengths[i]); //
        }
      }
    }
    writer.finish();
    writer.reset(vectorsStream); // 重置
    for (DocData dd : pendingDocs) {
      for (FieldData fd : dd.fields) {
        for (int i = 0; i < fd.numTerms; ++i) {// 该域有几个单独的词
          writer.add(fd.suffixLengths[i]);
        }
      }
    }
    writer.finish();
  }
  // 每个文档每个域每个distinct(term)出现的次数
  private void flushTermFreqs() throws IOException {
    writer.reset(vectorsStream);
    for (DocData dd : pendingDocs) {
      for (FieldData fd : dd.fields) {
        for (int i = 0; i < fd.numTerms; ++i) {
          writer.add(fd.freqs[i] - 1);
        }
      }
    }
    writer.finish();
  }
  // 在写满一个chunk时候就会刷新， 存放每个词的position相对信息
  private void flushPositions() throws IOException {
    writer.reset(vectorsStream);
    for (DocData dd : pendingDocs) {
      for (FieldData fd : dd.fields) {
        if (fd.hasPositions) {
          int pos = 0;
          for (int i = 0; i < fd.numTerms; ++i) { // 有几个词
            int previousPosition = 0;
            for (int j = 0; j < fd.freqs[i]; ++j) { // 每个词都扫描下
              final int position = positionsBuf[fd .posStart + pos++]; //
              writer.add(position - previousPosition); // 按顺序把所有的词都存储起来了
              previousPosition = position;
            }
          }
          assert pos == fd.totalPositions;
        }
      }
    }
    writer.finish();
  }
   // 刷新所有词的offset延迟信息
  private void flushOffsets(int[] fieldNums) throws IOException {
    boolean hasOffsets = false;
    long[] sumPos = new long[fieldNums.length]; // 所有字段的个数
    long[] sumOffsets = new long[fieldNums.length]; // 某个域的
    for (DocData dd : pendingDocs) {
      for (FieldData fd : dd.fields) { // 遍历每个词
        hasOffsets |= fd.hasOffsets;
        if (fd.hasOffsets && fd.hasPositions) { // 有offset或者position的话
          final int fieldNumOff = Arrays.binarySearch(fieldNums, fd.fieldNum); // 每个域的编号偏移量
          int pos = 0;
          for (int i = 0; i < fd.numTerms; ++i) {
            sumPos[fieldNumOff] += positionsBuf[fd.posStart + fd.freqs[i]-1 + pos];// 每个文档每个域 distin(term)的最大的起始position相加
            sumOffsets[fieldNumOff] += startOffsetsBuf[fd.offStart + fd.freqs[i]-1 + pos]; // 每个文档每个域 distin(term)的最大的起始offset相加
            pos += fd.freqs[i];
          }
          assert pos == fd.totalPositions;
        }
      }
    }

    if (!hasOffsets) {
      // nothing to do
      return;
    }
    // 估算每个域每个词的平均长度
    final float[] charsPerTerm = new float[fieldNums.length];
    for (int i = 0; i < fieldNums.length; ++i) {
      charsPerTerm[i] = (sumPos[i] <= 0 || sumOffsets[i] <= 0) ? 0 : (float) ((double) sumOffsets[i] / sumPos[i]);
    }

    // start offsets
    for (int i = 0; i < fieldNums.length; ++i) {
      vectorsStream.writeInt(Float.floatToRawIntBits(charsPerTerm[i])); // 写入平均每个域的浮点型表现形式
    }

    writer.reset(vectorsStream);  // 确定writer里面写入是本身
    for (DocData dd : pendingDocs) {
      for (FieldData fd : dd.fields) { // 每个域
        if ((fd.flags & OFFSETS) != 0) {
          final int fieldNumOff = Arrays.binarySearch(fieldNums, fd.fieldNum);
          final float cpt = charsPerTerm[fieldNumOff];
          int pos = 0;
          for (int i = 0; i < fd.numTerms; ++i) { // 每个域有几个distinct(term)
            int previousPos = 0;
            int previousOff = 0;
            for (int j = 0; j < fd.freqs[i]; ++j) {
              final int position = fd.hasPositions ? positionsBuf[fd.posStart + pos] : 0;
              final int startOffset = startOffsetsBuf[fd.offStart + pos];
              writer.add(startOffset - previousOff - (int) (cpt * (position - previousPos))); // 统计词的长度-所有词的平均长度*位置
              previousPos = position;
              previousOff = startOffset;
              ++pos;
            }
          }
        }
      }
    }
    writer.finish();

    // lengths
    writer.reset(vectorsStream);
    for (DocData dd : pendingDocs) {
      for (FieldData fd : dd.fields) {
        if ((fd.flags & OFFSETS) != 0) {
          int pos = 0;
          for (int i = 0; i < fd.numTerms; ++i) {
            for (int j = 0; j < fd.freqs[i]; ++j) { // 每个域的每个distinct(词)
              writer.add(lengthsBuf[fd.offStart + pos++] - fd.prefixLengths[i] - fd.suffixLengths[i]);
            } // 每个词的长度-前缀-后缀
          }
          assert pos == fd.totalPositions;
        }
      }
    }
    writer.finish();
  }
  // 没有取值，直接略过
  private void flushPayloadLengths() throws IOException {
    writer.reset(vectorsStream);
    for (DocData dd : pendingDocs) {
      for (FieldData fd : dd.fields) {
        if (fd.hasPayloads) {
          for (int i = 0; i < fd.totalPositions; ++i) {
            writer.add(payloadLengthsBuf[fd.payStart + i]);
          }
        }
      }
    }
    writer.finish();
  }

  @Override // 在flush时候会调用
  public void finish(FieldInfos fis, int numDocs) throws IOException {
    if (!pendingDocs.isEmpty()) {
      flush(); // 很重要，刷新一些列termvector相关的文件
      numDirtyChunks++; // incomplete: we had to force this flush
    }
    if (numDocs != this.numDocs) {
      throw new RuntimeException("Wrote " + this.numDocs + " docs, finish called with numDocs=" + numDocs);
    }
    indexWriter.finish(numDocs, vectorsStream.getFilePointer());// tvx
    vectorsStream.writeVLong(numChunks);
    vectorsStream.writeVLong(numDirtyChunks);
    CodecUtil.writeFooter(vectorsStream);
  }
  // numProx:该域该文档的词频
  @Override  // finish时，处理的是一个词的所有词频, numProx：相同词的词频   将一个词的tv读取出来。相同词的频次：numProx，positions:stream0, offsets:stream1
  public void addProx(int numProx, DataInput positions, DataInput offsets)
      throws IOException {
    assert (curField.hasPositions) == (positions != null);
    assert (curField.hasOffsets) == (offsets != null);

    if (curField.hasPositions) { // 有这个position
      final int posStart = curField.posStart + curField.totalPositions; // 前部词是从第几个开始(相同的词算2个)，为了统计该segment上顺序所有文档所有词的position
      if (posStart + numProx > positionsBuf.length) {  // position是否能否装得下全部的
        positionsBuf = ArrayUtil.grow(positionsBuf, posStart + numProx); // 每次扩容1/8
      }
      int position = 0;
      if (curField.hasPayloads) { // 为false
        final int payStart = curField.payStart + curField.totalPositions;
        if (payStart + numProx > payloadLengthsBuf.length) {
          payloadLengthsBuf = ArrayUtil.grow(payloadLengthsBuf, payStart + numProx);
        }
        for (int i = 0; i < numProx; ++i) { // 词频个数，每个词都过一遍
          final int code = positions.readVInt();
          if ((code & 1) != 0) {
            // This position has a payload
            final int payloadLength = positions.readVInt();
            payloadLengthsBuf[payStart + i] = payloadLength;
            payloadBytes.copyBytes(positions, payloadLength);
          } else {
            payloadLengthsBuf[payStart + i] = 0;
          }
          position += code >>> 1;
          positionsBuf[posStart + i] = position; // 每个词的position只是根据相同的上一个position来确认
        }
      } else {
        for (int i = 0; i < numProx; ++i) { // 词频
          position += (positions.readVInt() >>> 1); // 获取该词的位置，见TermVectorsConsumerPerField P218
          positionsBuf[posStart + i] = position;
        }
      }
    }

    if (curField.hasOffsets) { // 也有
      final int offStart = curField.offStart + curField.totalPositions;
      if (offStart + numProx > startOffsetsBuf.length) { // 放不下
        final int newLength = ArrayUtil.oversize(offStart + numProx, 4);
        startOffsetsBuf = ArrayUtil.growExact(startOffsetsBuf, newLength);
        lengthsBuf = ArrayUtil.growExact(lengthsBuf, newLength);
      }
      int lastOffset = 0, startOffset, endOffset;
      for (int i = 0; i < numProx; ++i) {
        startOffset = lastOffset + offsets.readVInt();// 获取该词offset，见TermVectorsConsumerPerField P198
        endOffset = startOffset + offsets.readVInt(); // 也是解压缩
        lastOffset = endOffset;
        startOffsetsBuf[offStart + i] = startOffset; // 词的offset请求
        lengthsBuf[offStart + i] = endOffset - startOffset;
      }
    }

    curField.totalPositions += numProx;
  }
  
  // bulk merge is scary: its caused corruption bugs in the past.
  // we try to be extra safe with this impl, but add an escape hatch to
  // have a workaround for undiscovered bugs.
  static final String BULK_MERGE_ENABLED_SYSPROP = CompressingTermVectorsWriter.class.getName() + ".enableBulkMerge";
  static final boolean BULK_MERGE_ENABLED;
  static {
    boolean v = true;
    try {
      v = Boolean.parseBoolean(System.getProperty(BULK_MERGE_ENABLED_SYSPROP, "true"));
    } catch (SecurityException ignored) {}
    BULK_MERGE_ENABLED = v;
  }

  @Override
  public int merge(MergeState mergeState) throws IOException {
    if (mergeState.needsIndexSort) {
      // TODO: can we gain back some optos even if index is sorted?  E.g. if sort results in large chunks of contiguous docs from one sub
      // being copied over...?
      return super.merge(mergeState);
    }
    int docCount = 0;
    int numReaders = mergeState.maxDocs.length;

    MatchingReaders matching = new MatchingReaders(mergeState);
    
    for (int readerIndex=0;readerIndex<numReaders;readerIndex++) {
      CompressingTermVectorsReader matchingVectorsReader = null;
      final TermVectorsReader vectorsReader = mergeState.termVectorsReaders[readerIndex];
      if (matching.matchingReaders[readerIndex]) {
        // we can only bulk-copy if the matching reader is also a CompressingTermVectorsReader
        if (vectorsReader != null && vectorsReader instanceof CompressingTermVectorsReader) {
          matchingVectorsReader = (CompressingTermVectorsReader) vectorsReader;
        }
      }

      final int maxDoc = mergeState.maxDocs[readerIndex];
      final Bits liveDocs = mergeState.liveDocs[readerIndex];
      
      if (matchingVectorsReader != null &&
          matchingVectorsReader.getCompressionMode() == compressionMode &&
          matchingVectorsReader.getChunkSize() == chunkSize &&
          matchingVectorsReader.getVersion() == VERSION_CURRENT && 
          matchingVectorsReader.getPackedIntsVersion() == PackedInts.VERSION_CURRENT &&
          BULK_MERGE_ENABLED &&
          liveDocs == null &&
          !tooDirty(matchingVectorsReader)) {
        // optimized merge, raw byte copy
        // its not worth fine-graining this if there are deletions.
        
        matchingVectorsReader.checkIntegrity();
        
        // flush any pending chunks
        if (!pendingDocs.isEmpty()) {
          flush();
          numDirtyChunks++; // incomplete: we had to force this flush
        }
        
        // iterate over each chunk. we use the vectors index to find chunk boundaries,
        // read the docstart + doccount from the chunk header (we write a new header, since doc numbers will change),
        // and just copy the bytes directly.
        IndexInput rawDocs = matchingVectorsReader.getVectorsStream();
        FieldsIndex index = matchingVectorsReader.getIndexReader();
        rawDocs.seek(index.getStartPointer(0));
        int docID = 0;
        while (docID < maxDoc) {
          // read header
          int base = rawDocs.readVInt();
          if (base != docID) {
            throw new CorruptIndexException("invalid state: base=" + base + ", docID=" + docID, rawDocs);
          }
          int bufferedDocs = rawDocs.readVInt();
          
          // write a new index entry and new header for this chunk.
          indexWriter.writeIndex(bufferedDocs, vectorsStream.getFilePointer());
          vectorsStream.writeVInt(docCount); // rebase
          vectorsStream.writeVInt(bufferedDocs);
          docID += bufferedDocs;
          docCount += bufferedDocs;
          numDocs += bufferedDocs;
          
          if (docID > maxDoc) {
            throw new CorruptIndexException("invalid state: base=" + base + ", count=" + bufferedDocs + ", maxDoc=" + maxDoc, rawDocs);
          }
          
          // copy bytes until the next chunk boundary (or end of chunk data).
          // using the stored fields index for this isn't the most efficient, but fast enough
          // and is a source of redundancy for detecting bad things.
          final long end;
          if (docID == maxDoc) {
            end = matchingVectorsReader.getMaxPointer();
          } else {
            end = index.getStartPointer(docID);
          }
          vectorsStream.copyBytes(rawDocs, end - rawDocs.getFilePointer());
        }
               
        if (rawDocs.getFilePointer() != matchingVectorsReader.getMaxPointer()) {
          throw new CorruptIndexException("invalid state: pos=" + rawDocs.getFilePointer() + ", max=" + matchingVectorsReader.getMaxPointer(), rawDocs);
        }
        
        // since we bulk merged all chunks, we inherit any dirty ones from this segment.
        numChunks += matchingVectorsReader.getNumChunks();
        numDirtyChunks += matchingVectorsReader.getNumDirtyChunks();
      } else {        
        // naive merge...
        if (vectorsReader != null) {
          vectorsReader.checkIntegrity();
        }
        for (int i = 0; i < maxDoc; i++) {
          if (liveDocs != null && liveDocs.get(i) == false) {
            continue;
          }
          Fields vectors;
          if (vectorsReader == null) {
            vectors = null;
          } else {
            vectors = vectorsReader.get(i);
          }
          addAllDocVectors(vectors, mergeState);
          ++docCount;
        }
      }
    }
    finish(mergeState.mergeFieldInfos, docCount);
    return docCount;
  }

  /** 
   * Returns true if we should recompress this reader, even though we could bulk merge compressed data 
   * <p>
   * The last chunk written for a segment is typically incomplete, so without recompressing,
   * in some worst-case situations (e.g. frequent reopen with tiny flushes), over time the 
   * compression ratio can degrade. This is a safety switch.
   */
  boolean tooDirty(CompressingTermVectorsReader candidate) {
    // more than 1% dirty, or more than hard limit of 1024 dirty chunks
    return candidate.getNumDirtyChunks() > 1024 || 
           candidate.getNumDirtyChunks() * 100 > candidate.getNumChunks();
  }
}
