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
package org.apache.lucene.codecs.lucene80;


import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.EmptyDocValuesProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.MathUtil;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.compress.LZ4;
import org.apache.lucene.util.compress.LZ4.FastCompressionHashTable;
import org.apache.lucene.util.packed.DirectMonotonicWriter;
import org.apache.lucene.util.packed.DirectWriter;

import static org.apache.lucene.codecs.lucene80.Lucene80DocValuesFormat.DIRECT_MONOTONIC_BLOCK_SHIFT;
import static org.apache.lucene.codecs.lucene80.Lucene80DocValuesFormat.NUMERIC_BLOCK_SHIFT;
import static org.apache.lucene.codecs.lucene80.Lucene80DocValuesFormat.NUMERIC_BLOCK_SIZE;
 // 写入时
/** writer for {@link Lucene80DocValuesFormat} */
final class Lucene80DocValuesConsumer extends DocValuesConsumer implements Closeable {

  IndexOutput data, meta; //  meta=/data1/_0_Lucene80_0.dvd, data=/data1/_0_Lucene80_0.dvm
  final int maxDoc; // 最大的文档id
  private final SegmentWriteState state;

  /** expert: Creates a new writer */
  public Lucene80DocValuesConsumer(SegmentWriteState state, String dataCodec, String dataExtension, String metaCodec, String metaExtension) throws IOException {
    boolean success = false;
    try {
      this.state = state;
      String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, dataExtension); // dvd
      data = state.directory.createOutput(dataName, state.context); // 产生的是ByteSizeCachingDirectory,在merge阶段就变成了RateLimitedIndexOutput
      CodecUtil.writeIndexHeader(data, dataCodec, Lucene80DocValuesFormat.VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
      String metaName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);
      meta = state.directory.createOutput(metaName, state.context);// dvm
      CodecUtil.writeIndexHeader(meta, metaCodec, Lucene80DocValuesFormat.VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
      maxDoc = state.segmentInfo.maxDoc();
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(this);
      }
    }
  }
 // 官博的时候刷新到dvm dvd中
  @Override
  public void close() throws IOException {
    boolean success = false;
    try {
      if (meta != null) {
        meta.writeInt(-1); // write EOF marker
        CodecUtil.writeFooter(meta); // write checksum
      }
      if (data != null) {
        CodecUtil.writeFooter(data); // write checksum
      }
      success = true;
    } finally {
      if (success) {
        IOUtils.close(data, meta);
      } else {
        IOUtils.closeWhileHandlingException(data, meta);
      }
      meta = data = null;
    }
  }

  @Override
  public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
    meta.writeInt(field.number);
    meta.writeByte(Lucene80DocValuesFormat.NUMERIC);

    writeValues(field, new EmptyDocValuesProducer() {
      @Override
      public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
        return DocValues.singleton(valuesProducer.getNumeric(field));
      }
    });
  }

  private static class MinMaxTracker {
    long min, max, numValues, spaceInBits;

    MinMaxTracker() {
      reset();
      spaceInBits = 0;
    }

    private void reset() {
      min = Long.MAX_VALUE;
      max = Long.MIN_VALUE;
      numValues = 0;
    }

    /** Accumulate a new value. */
    void update(long v) {
      min = Math.min(min, v);
      max = Math.max(max, v);
      ++numValues;
    }

    /** Update the required space. */
    void finish() {
      if (max > min) {
        spaceInBits += DirectWriter.unsignedBitsRequired(max - min) * numValues;
      }
    }

    /** Update space usage and get ready for accumulating values for the next block. */
    void nextBlock() {
      finish();
      reset();
    }
  }

  private long[] writeValues(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
    SortedNumericDocValues values = valuesProducer.getSortedNumeric(field);
    int numDocsWithValue = 0;
    MinMaxTracker minMax = new MinMaxTracker();
    MinMaxTracker blockMinMax = new MinMaxTracker();
    long gcd = 0;
    Set<Long> uniqueValues = new HashSet<>();
    for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
      for (int i = 0, count = values.docValueCount(); i < count; ++i) {
        long v = values.nextValue();

        if (gcd != 1) {
          if (v < Long.MIN_VALUE / 2 || v > Long.MAX_VALUE / 2) {
            // in that case v - minValue might overflow and make the GCD computation return
            // wrong results. Since these extreme values are unlikely, we just discard
            // GCD computation for them
            gcd = 1;
          } else if (minMax.numValues != 0) { // minValue needs to be set first
            gcd = MathUtil.gcd(gcd, v - minMax.min);
          }
        }

        minMax.update(v);
        blockMinMax.update(v);
        if (blockMinMax.numValues == NUMERIC_BLOCK_SIZE) {
          blockMinMax.nextBlock();
        }

        if (uniqueValues != null
            && uniqueValues.add(v)
            && uniqueValues.size() > 256) {
          uniqueValues = null;
        }
      }

      numDocsWithValue++;
    }

    minMax.finish();
    blockMinMax.finish();

    final long numValues = minMax.numValues;
    long min = minMax.min;
    final long max = minMax.max;
    assert blockMinMax.spaceInBits <= minMax.spaceInBits;

    if (numDocsWithValue == 0) {              // meta[-2, 0]: No documents with values
      meta.writeLong(-2); // docsWithFieldOffset
      meta.writeLong(0L); // docsWithFieldLength
      meta.writeShort((short) -1); // jumpTableEntryCount
      meta.writeByte((byte) -1);   // denseRankPower
    } else if (numDocsWithValue == maxDoc) {  // meta[-1, 0]: All documents has values
      meta.writeLong(-1); // docsWithFieldOffset
      meta.writeLong(0L); // docsWithFieldLength
      meta.writeShort((short) -1); // jumpTableEntryCount
      meta.writeByte((byte) -1);   // denseRankPower
    } else {                                  // meta[data.offset, data.length]: IndexedDISI structure for documents with values
      long offset = data.getFilePointer();
      meta.writeLong(offset);// docsWithFieldOffset
      values = valuesProducer.getSortedNumeric(field);
      final short jumpTableEntryCount = IndexedDISI.writeBitSet(values, data, IndexedDISI.DEFAULT_DENSE_RANK_POWER);
      meta.writeLong(data.getFilePointer() - offset); // docsWithFieldLength
      meta.writeShort(jumpTableEntryCount);//返回多少个block
      meta.writeByte(IndexedDISI.DEFAULT_DENSE_RANK_POWER);
    }

    meta.writeLong(numValues);
    final int numBitsPerValue;
    boolean doBlocks = false;
    Map<Long, Integer> encode = null;
    if (min >= max) {                         // meta[-1]: All values are 0
      numBitsPerValue = 0;
      meta.writeInt(-1); // tablesize
    } else {
      if (uniqueValues != null
          && uniqueValues.size() > 1
          && DirectWriter.unsignedBitsRequired(uniqueValues.size() - 1) < DirectWriter.unsignedBitsRequired((max - min) / gcd)) {
        numBitsPerValue = DirectWriter.unsignedBitsRequired(uniqueValues.size() - 1);
        final Long[] sortedUniqueValues = uniqueValues.toArray(new Long[0]);
        Arrays.sort(sortedUniqueValues);
        meta.writeInt(sortedUniqueValues.length); // tablesize
        for (Long v : sortedUniqueValues) {
          meta.writeLong(v); // table[] entry
        }
        encode = new HashMap<>();
        for (int i = 0; i < sortedUniqueValues.length; ++i) {
          encode.put(sortedUniqueValues[i], i);
        }
        min = 0;
        gcd = 1;
      } else {
        uniqueValues = null;
        // we do blocks if that appears to save 10+% storage
        doBlocks = minMax.spaceInBits > 0 && (double) blockMinMax.spaceInBits / minMax.spaceInBits <= 0.9;
        if (doBlocks) {
          numBitsPerValue = 0xFF;
          meta.writeInt(-2 - NUMERIC_BLOCK_SHIFT); // tablesize
        } else {
          numBitsPerValue = DirectWriter.unsignedBitsRequired((max - min) / gcd);
          if (gcd == 1 && min > 0
              && DirectWriter.unsignedBitsRequired(max) == DirectWriter.unsignedBitsRequired(max - min)) {
            min = 0;
          }
          meta.writeInt(-1); // tablesize
        }
      }
    }

    meta.writeByte((byte) numBitsPerValue);
    meta.writeLong(min);
    meta.writeLong(gcd);
    long startOffset = data.getFilePointer();
    meta.writeLong(startOffset); // valueOffset
    long jumpTableOffset = -1;
    if (doBlocks) {
      jumpTableOffset = writeValuesMultipleBlocks(valuesProducer.getSortedNumeric(field), gcd);
    } else if (numBitsPerValue != 0) {
      writeValuesSingleBlock(valuesProducer.getSortedNumeric(field), numValues, numBitsPerValue, min, gcd, encode);
    }
    meta.writeLong(data.getFilePointer() - startOffset); // valuesLength
    meta.writeLong(jumpTableOffset);
    return new long[] {numDocsWithValue, numValues};
  }

  private void writeValuesSingleBlock(SortedNumericDocValues values, long numValues, int numBitsPerValue,
      long min, long gcd, Map<Long, Integer> encode) throws IOException {
    DirectWriter writer = DirectWriter.getInstance(data, numValues, numBitsPerValue);
    for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
      for (int i = 0, count = values.docValueCount(); i < count; ++i) {
        long v = values.nextValue();
        if (encode == null) {
          writer.add((v - min) / gcd);
        } else {
          writer.add(encode.get(v));
        }
      }
    }
    writer.finish();
  }

  // Returns the offset to the jump-table for vBPV
  private long writeValuesMultipleBlocks(SortedNumericDocValues values, long gcd) throws IOException {
    long[] offsets = new long[ArrayUtil.oversize(1, Long.BYTES)];
    int offsetsIndex = 0;
    final long[] buffer = new long[NUMERIC_BLOCK_SIZE];
    final ByteBuffersDataOutput encodeBuffer = ByteBuffersDataOutput.newResettableInstance();
    int upTo = 0;
    for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
      for (int i = 0, count = values.docValueCount(); i < count; ++i) {
        buffer[upTo++] = values.nextValue();
        if (upTo == NUMERIC_BLOCK_SIZE) {
          offsets = ArrayUtil.grow(offsets, offsetsIndex+1);
          offsets[offsetsIndex++] = data.getFilePointer();
          writeBlock(buffer, NUMERIC_BLOCK_SIZE, gcd, encodeBuffer);
          upTo = 0;
        }
      }
    }
    if (upTo > 0) {
      offsets = ArrayUtil.grow(offsets, offsetsIndex+1);
      offsets[offsetsIndex++] = data.getFilePointer();
      writeBlock(buffer, upTo, gcd, encodeBuffer);
    }

    // All blocks has been written. Flush the offset jump-table
    final long offsetsOrigo = data.getFilePointer();
    for (int i = 0 ; i < offsetsIndex ; i++) {
      data.writeLong(offsets[i]);
    }
    data.writeLong(offsetsOrigo);
    return offsetsOrigo;
  }

  private void writeBlock(long[] values, int length, long gcd, ByteBuffersDataOutput buffer) throws IOException {
    assert length > 0;
    long min = values[0];
    long max = values[0];
    for (int i = 1; i < length; ++i) {
      final long v = values[i];
      assert Math.floorMod(values[i] - min, gcd) == 0;
      min = Math.min(min, v);
      max = Math.max(max, v);
    }
    if (min == max) {
      data.writeByte((byte) 0);
      data.writeLong(min);
    } else {
      final int bitsPerValue = DirectWriter.unsignedBitsRequired(max - min);
      buffer.reset();
      assert buffer.size() == 0;
      final DirectWriter w = DirectWriter.getInstance(buffer, length, bitsPerValue);
      for (int i = 0; i < length; ++i) {
        w.add((values[i] - min) / gcd);
      }
      w.finish();
      data.writeByte((byte) bitsPerValue);
      data.writeLong(min);
      data.writeInt(Math.toIntExact(buffer.size()));
      buffer.copyTo(data);
    }
  }

  class CompressedBinaryBlockWriter implements Closeable {
    final FastCompressionHashTable ht = new LZ4.FastCompressionHashTable();    
    int uncompressedBlockLength = 0;
    int maxUncompressedBlockLength = 0;
    int numDocsInCurrentBlock = 0;
    final int[] docLengths = new int[Lucene80DocValuesFormat.BINARY_DOCS_PER_COMPRESSED_BLOCK]; 
    byte[] block = BytesRef.EMPTY_BYTES;
    int totalChunks = 0;
    long maxPointer = 0;
    final long blockAddressesStart; 

    private final IndexOutput tempBinaryOffsets;
    
    
    public CompressedBinaryBlockWriter() throws IOException {
      tempBinaryOffsets = state.directory.createTempOutput(state.segmentInfo.name, "binary_pointers", state.context);
      boolean success = false;
      try {
        CodecUtil.writeHeader(tempBinaryOffsets, Lucene80DocValuesFormat.META_CODEC + "FilePointers", Lucene80DocValuesFormat.VERSION_CURRENT);
        blockAddressesStart = data.getFilePointer();
        success = true;
      } finally {
        if (success == false) {
          IOUtils.closeWhileHandlingException(this); //self-close because constructor caller can't 
        }
      }
    }

    void addDoc(int doc, BytesRef v) throws IOException {
      docLengths[numDocsInCurrentBlock] = v.length;
      block = ArrayUtil.grow(block, uncompressedBlockLength + v.length);
      System.arraycopy(v.bytes, v.offset, block, uncompressedBlockLength, v.length);
      uncompressedBlockLength += v.length;
      numDocsInCurrentBlock++;
      if (numDocsInCurrentBlock == Lucene80DocValuesFormat.BINARY_DOCS_PER_COMPRESSED_BLOCK) {
        flushData();
      }      
    }

    private void flushData() throws IOException {
      if (numDocsInCurrentBlock > 0) {
        // Write offset to this block to temporary offsets file
        totalChunks++;
        long thisBlockStartPointer = data.getFilePointer();
        
        // Optimisation - check if all lengths are same
        boolean allLengthsSame = true;
        for (int i = 1; i < Lucene80DocValuesFormat.BINARY_DOCS_PER_COMPRESSED_BLOCK; i++) {
          if (docLengths[i] != docLengths[i-1]) {
            allLengthsSame = false;
            break;
          }
        }
        if (allLengthsSame) {
            // Only write one value shifted. Steal a bit to indicate all other lengths are the same
            int onlyOneLength = (docLengths[0] <<1) | 1;
            data.writeVInt(onlyOneLength);
        } else {
          for (int i = 0; i < Lucene80DocValuesFormat.BINARY_DOCS_PER_COMPRESSED_BLOCK; i++) {
            if (i == 0) {
              // Write first value shifted and steal a bit to indicate other lengths are to follow
              int multipleLengths = (docLengths[0] <<1);
              data.writeVInt(multipleLengths);              
            } else {
              data.writeVInt(docLengths[i]);
            }
          }
        }
        maxUncompressedBlockLength = Math.max(maxUncompressedBlockLength, uncompressedBlockLength);
        LZ4.compress(block, 0, uncompressedBlockLength, data, ht);
        numDocsInCurrentBlock = 0;
        // Ensure initialized with zeroes because full array is always written
        Arrays.fill(docLengths, 0);
        uncompressedBlockLength = 0;
        maxPointer = data.getFilePointer();
        tempBinaryOffsets.writeVLong(maxPointer - thisBlockStartPointer);
      }
    }
    
    void writeMetaData() throws IOException {
      if (totalChunks == 0) {
        return;
      }
      
      long startDMW = data.getFilePointer();
      meta.writeLong(startDMW);
      
      meta.writeVInt(totalChunks);
      meta.writeVInt(Lucene80DocValuesFormat.BINARY_BLOCK_SHIFT);
      meta.writeVInt(maxUncompressedBlockLength);
      meta.writeVInt(DIRECT_MONOTONIC_BLOCK_SHIFT);
      
    
      CodecUtil.writeFooter(tempBinaryOffsets);
      IOUtils.close(tempBinaryOffsets);             
      //write the compressed block offsets info to the meta file by reading from temp file
      try (ChecksumIndexInput filePointersIn = state.directory.openChecksumInput(tempBinaryOffsets.getName(), IOContext.READONCE)) {
        CodecUtil.checkHeader(filePointersIn, Lucene80DocValuesFormat.META_CODEC + "FilePointers", Lucene80DocValuesFormat.VERSION_CURRENT,
          Lucene80DocValuesFormat.VERSION_CURRENT);
        Throwable priorE = null;
        try {
          final DirectMonotonicWriter filePointers = DirectMonotonicWriter.getInstance(meta, data, totalChunks, DIRECT_MONOTONIC_BLOCK_SHIFT);
          long fp = blockAddressesStart;
          for (int i = 0; i < totalChunks; ++i) {
            filePointers.add(fp);
            fp += filePointersIn.readVLong();
          }
          if (maxPointer < fp) {
            throw new CorruptIndexException("File pointers don't add up ("+fp+" vs expected "+maxPointer+")", filePointersIn);
          }
          filePointers.finish();
        } catch (Throwable e) {
          priorE = e;
        } finally {
          CodecUtil.checkFooter(filePointersIn, priorE);
        }
      }
      // Write the length of the DMW block in the data 
      meta.writeLong(data.getFilePointer() - startDMW);
    }

    @Override
    public void close() throws IOException {
      if (tempBinaryOffsets != null) {
        IOUtils.close(tempBinaryOffsets);             
        state.directory.deleteFile(tempBinaryOffsets.getName());
      }
    }
    
  }
  

  @Override
  public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
    meta.writeInt(field.number);
    meta.writeByte(Lucene80DocValuesFormat.BINARY);

    try (CompressedBinaryBlockWriter blockWriter = new CompressedBinaryBlockWriter()){
      BinaryDocValues values = valuesProducer.getBinary(field);
      long start = data.getFilePointer();
      meta.writeLong(start); // dataOffset
      int numDocsWithField = 0;
      int minLength = Integer.MAX_VALUE;
      int maxLength = 0;
      for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
        numDocsWithField++;
        BytesRef v = values.binaryValue();      
        blockWriter.addDoc(doc, v);      
        int length = v.length;      
        minLength = Math.min(length, minLength);
        maxLength = Math.max(length, maxLength);
      }
      blockWriter.flushData();

      assert numDocsWithField <= maxDoc;
      meta.writeLong(data.getFilePointer() - start); // dataLength

      if (numDocsWithField == 0) {
        meta.writeLong(-2); // docsWithFieldOffset
        meta.writeLong(0L); // docsWithFieldLength
        meta.writeShort((short) -1); // jumpTableEntryCount
        meta.writeByte((byte) -1);   // denseRankPower
      } else if (numDocsWithField == maxDoc) {
        meta.writeLong(-1); // docsWithFieldOffset
        meta.writeLong(0L); // docsWithFieldLength
        meta.writeShort((short) -1); // jumpTableEntryCount
        meta.writeByte((byte) -1);   // denseRankPower
      } else {
        long offset = data.getFilePointer();
        meta.writeLong(offset); // docsWithFieldOffset
        values = valuesProducer.getBinary(field);
        final short jumpTableEntryCount = IndexedDISI.writeBitSet(values, data, IndexedDISI.DEFAULT_DENSE_RANK_POWER);
        meta.writeLong(data.getFilePointer() - offset); // docsWithFieldLength
        meta.writeShort(jumpTableEntryCount);
        meta.writeByte(IndexedDISI.DEFAULT_DENSE_RANK_POWER);
      }

      meta.writeInt(numDocsWithField);
      meta.writeInt(minLength);
      meta.writeInt(maxLength);    
      
      blockWriter.writeMetaData();
      
    }

  }

  @Override
  public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
    meta.writeInt(field.number);
    meta.writeByte(Lucene80DocValuesFormat.SORTED);
    doAddSortedField(field, valuesProducer);
  }
  /// 存储termId，参考 Lucene80DocValeusConsumer.readSorted()
  private void doAddSortedField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
    SortedDocValues values = valuesProducer.getSorted(field); //SortedSetSelector$MinValue
    int numDocsWithField = 0;
    for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
      numDocsWithField++; // 只是统计该域所有文档相同域名的域个数（一个文档相同域名的域可能有两个）
    }

    if (numDocsWithField == 0) {
      meta.writeLong(-2); // docsWithFieldOffset
      meta.writeLong(0L); // docsWithFieldLength
      meta.writeShort((short) -1); // jumpTableEntryCount
      meta.writeByte((byte) -1);   // denseRankPower
    } else if (numDocsWithField == maxDoc) {// 一般会跑到这里，重新建立dvd文件，
      meta.writeLong(-1); // docsWithFieldOffset
      meta.writeLong(0L); // docsWithFieldLength
      meta.writeShort((short) -1); // jumpTableEntryCount
      meta.writeByte((byte) -1);   // denseRankPower
    } else {
      long offset = data.getFilePointer(); // 获取文件写入的地方，即将写入docId
      meta.writeLong(offset); // docsWithFieldOffset， 存放到meta中
      values = valuesProducer.getSorted(field); //SortedSetSelector$MinValue
      final short jumpTableentryCount = IndexedDISI.writeBitSet(values, data, IndexedDISI.DEFAULT_DENSE_RANK_POWER);
      meta.writeLong(data.getFilePointer() - offset); // docsWithFieldLength
      meta.writeShort(jumpTableentryCount); // 记录多少个block
      meta.writeByte(IndexedDISI.DEFAULT_DENSE_RANK_POWER);
    }

    meta.writeInt(numDocsWithField); // 文档个数
    if (values.getValueCount() <= 1) { // 所有域个数
      meta.writeByte((byte) 0); // bitsPerValue
      meta.writeLong(0L); // ordsOffset
      meta.writeLong(0L); // ordsLength
    } else { // 一般跑这里，存储存储排序后的的每个termId编号
      int numberOfBitsPerOrd = DirectWriter.unsignedBitsRequired(values.getValueCount() - 1); // 获取的是segment内唯一的域个数
      meta.writeByte((byte) numberOfBitsPerOrd); // bitsPerValue
      long start = data.getFilePointer();    // 获取data文档目前写入的位置(写TermId之前)
      meta.writeLong(start); // ordsOffset    排序好的termId在dvd中存放的起始位置
      DirectWriter writer = DirectWriter.getInstance(data, numDocsWithField, numberOfBitsPerOrd); // DirectWriter，就是简单地长度压缩
      values = valuesProducer.getSorted(field); //SortedSetSelector$MinValue
      for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) { // 遍历这numDocsWithField
        writer.add(values.ordValue()); // 按照文档写入顺序，放入每个term对应的大小排序顺序
      }//下标是文档Id, value是这个文档的对应词的大小排序
      writer.finish();
      meta.writeLong(data.getFilePointer() - start); // ordsLength
    }

    addTermsDict(DocValues.singleton(valuesProducer.getSorted(field)));
  }
  // 16个词一个索引。写
  private void addTermsDict(SortedSetDocValues values) throws IOException {
    final long size = values.getValueCount(); // 多少个词cardinatory的个数
    meta.writeVLong(size);
    meta.writeInt(Lucene80DocValuesFormat.TERMS_DICT_BLOCK_SHIFT);

    ByteBuffersDataOutput addressBuffer = new ByteBuffersDataOutput();
    ByteBuffersIndexOutput addressOutput = new ByteBuffersIndexOutput(addressBuffer, "temp", "temp");
    meta.writeInt(DIRECT_MONOTONIC_BLOCK_SHIFT);// 16
    long numBlocks = (size + Lucene80DocValuesFormat.TERMS_DICT_BLOCK_MASK) >>> Lucene80DocValuesFormat.TERMS_DICT_BLOCK_SHIFT; // 一个block为16，可以存放多少个block
    DirectMonotonicWriter writer = DirectMonotonicWriter.getInstance(meta, addressOutput, numBlocks, DIRECT_MONOTONIC_BLOCK_SHIFT);

    BytesRefBuilder previous = new BytesRefBuilder();
    long ord = 0;
    long start = data.getFilePointer();
    int maxLength = 0;
    TermsEnum iterator = values.termsEnum(); // SortedDocValuesTermsEnum
    for (BytesRef term = iterator.next(); term != null; term = iterator.next()) { // 从第0、1、2、3...小的词逐个读取
      if ((ord & Lucene80DocValuesFormat.TERMS_DICT_BLOCK_MASK) == 0) {
        writer.add(data.getFilePointer() - start); // 向writer中写入一次data使用位置
        data.writeVInt(term.length); // 写入真实的term长度
        data.writeBytes(term.bytes, term.offset, term.length);//写入真实的term的二进制数据，压缩从头开始
      } else {// 存放比较神奇，只要从0-15个词开始往后读，前缀不变，每次读取时，只要改变下后缀就行了
        final int prefixLength = StringHelper.bytesDifference(previous.get(), term);
        final int suffixLength = term.length - prefixLength;
        assert suffixLength > 0; // terms are unique
        // 相同前缀用低4位，不同的后缀用高4位
        data.writeByte((byte) (Math.min(prefixLength, 15) | (Math.min(15, suffixLength - 1) << 4))); // 高4位和低4位
        if (prefixLength >= 15) { // 若前缀大于15，再接着另存
          data.writeVInt(prefixLength - 15);
        }
        if (suffixLength >= 16) { // 后缀大于16，再接着另存
          data.writeVInt(suffixLength - 16);
        } // dvd保存term内容，存储的是不同后缀的长度
        data.writeBytes(term.bytes, term.offset + prefixLength, term.length - prefixLength);
      }
      maxLength = Math.max(maxLength, term.length);
      previous.copyBytes(term);
      ++ord;
    }
    writer.finish(); // 将每隔15个词在dvm和dvd中的位置给记录下来（一级索引，dvm放偏移的元数据，dvd才是放的偏移的真实数据）
    meta.writeInt(maxLength);
    meta.writeLong(start);
    meta.writeLong(data.getFilePointer() - start);
    start = data.getFilePointer();
    addressBuffer.copyTo(data); // 将第16*x个域的值在dvm中的位置给记录下来给存储到dvd中
    meta.writeLong(start); // dvd中起始位置
    meta.writeLong(data.getFilePointer() - start);
    // 第三层，记录 term 字典的索引，values 是按照值 hash 排过序的，这里每 1024 条抽取一个作为索引，加速查询
    // Now write the reverse terms index
    writeTermsIndex(values);
  }// 二级索引是相同长度前缀
  // TermsIndex是TermsDict索引, TermsDict是16个term一个索引，而 TermsIndex是1024一个索引结构
  private void writeTermsIndex(SortedSetDocValues values) throws IOException {
    final long size = values.getValueCount(); // segment范围内所有文档相同域distinct(term)的个数
    meta.writeInt(Lucene80DocValuesFormat.TERMS_DICT_REVERSE_INDEX_SHIFT); // 字典间隔1024
    long start = data.getFilePointer();

    long numBlocks = 1L + ((size + Lucene80DocValuesFormat.TERMS_DICT_REVERSE_INDEX_MASK) >>> Lucene80DocValuesFormat.TERMS_DICT_REVERSE_INDEX_SHIFT);
    ByteBuffersDataOutput addressBuffer = new ByteBuffersDataOutput();
    DirectMonotonicWriter writer;
    try (ByteBuffersIndexOutput addressOutput = new ByteBuffersIndexOutput(addressBuffer, "temp", "temp")) {
      writer = DirectMonotonicWriter.getInstance(meta, addressOutput, numBlocks, DIRECT_MONOTONIC_BLOCK_SHIFT); // 也是使用这玩意写入数据
      TermsEnum iterator = values.termsEnum();// SortedDocValuesTermsEnum
      BytesRefBuilder previous = new BytesRefBuilder();
      long offset = 0;  // 相同前缀的累加值
      long ord = 0;
      for (BytesRef term = iterator.next(); term != null; term = iterator.next()) { // 就segment内唯一某个域的distinct(词)的存储，依次遍历
        if ((ord & Lucene80DocValuesFormat.TERMS_DICT_REVERSE_INDEX_MASK) == 0) { // 每隔1024个词存一次  1024*   my_bug
          writer.add(offset); // 存储的是第二级别相同长度
          final int sortKeyLength;
          if (ord == 0) {
            // no previous term: no bytes to write
            sortKeyLength = 0;
          } else {
            sortKeyLength = StringHelper.sortKeyLength(previous.get(), term); // 相同的前缀
          }
          offset += sortKeyLength;// 累加的目的主要是为了便于存储到DirectMonotonicWriter中（满足单调递增的）
          data.writeBytes(term.bytes, term.offset, sortKeyLength); // dvd  和前一个词相比，存储相同的前缀内容
        } else if ((ord & Lucene80DocValuesFormat.TERMS_DICT_REVERSE_INDEX_MASK) == Lucene80DocValuesFormat.TERMS_DICT_REVERSE_INDEX_MASK) { // 1024*x + 1023
          previous.copyBytes(term); // 每次找到第1024*x + 1023个词，主要是为了获取该词，为第1024*(x+1)个词找相同的前缀
        }
        ++ord;
      } // 二级索引由两部分构成，一部分是第1024*x个词的相同前缀内容，第二部分是第1024*x个词和第1024*x-1个词的相同前缀累加值（数组会放在dvm和dvd中）
      writer.add(offset);
      writer.finish();
      meta.writeLong(start);
      meta.writeLong(data.getFilePointer() - start); // 存放二级索引每第1024个词相同前缀内容
      start = data.getFilePointer();
      addressBuffer.copyTo(data);
      meta.writeLong(start); // 往meta中写入
      meta.writeLong(data.getFilePointer() - start);
    }
  }

  @Override
  public void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
    meta.writeInt(field.number);
    meta.writeByte(Lucene80DocValuesFormat.SORTED_NUMERIC);

    long[] stats = writeValues(field, valuesProducer);
    int numDocsWithField = Math.toIntExact(stats[0]);
    long numValues = stats[1];
    assert numValues >= numDocsWithField;

    meta.writeInt(numDocsWithField);
    if (numValues > numDocsWithField) {
      long start = data.getFilePointer();
      meta.writeLong(start);
      meta.writeVInt(DIRECT_MONOTONIC_BLOCK_SHIFT);

      final DirectMonotonicWriter addressesWriter = DirectMonotonicWriter.getInstance(meta, data, numDocsWithField + 1L, DIRECT_MONOTONIC_BLOCK_SHIFT);
      long addr = 0;
      addressesWriter.add(addr);
      SortedNumericDocValues values = valuesProducer.getSortedNumeric(field);
      for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
        addr += values.docValueCount();
        addressesWriter.add(addr);
      }
      addressesWriter.finish();
      meta.writeLong(data.getFilePointer() - start);
    }
  }
  /// 这是某一个域
  @Override // 映射 Lucene80DocValuesConsumer.readFields()
  public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
    meta.writeInt(field.number); // 域number
    meta.writeByte(Lucene80DocValuesFormat.SORTED_SET); // 该域存储类型

    SortedSetDocValues values = valuesProducer.getSortedSet(field); // BufferedSortedSetDocValues
    int numDocsWithField = 0;
    long numOrds = 0;
    for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
      numDocsWithField++;// 总文档数
      for (long ord = values.nextOrd(); ord != SortedSetDocValues.NO_MORE_ORDS; ord = values.nextOrd()) {
        numOrds++;// 总次数
      }
    } // 已经把数据解压缩出来了，存放在values中了

    if (numDocsWithField == numOrds) { // 每个文档只有一个词，不是多个词。一般都会跑到这里
      meta.writeByte((byte) 0); // multiValued (0 = singleValued)
      doAddSortedField(field, new EmptyDocValuesProducer() { //单值类型
        @Override
        public SortedDocValues getSorted(FieldInfo field) throws IOException {
          return SortedSetSelector.wrap(valuesProducer.getSortedSet(field), SortedSetSelector.Type.MIN);// 读取最小的那个值
        }
      });
      return;
    } // 不是每个文档都有该值
    meta.writeByte((byte) 1);  // multiValued (1 = multiValued)

    assert numDocsWithField != 0;
    if (numDocsWithField == maxDoc) {
      meta.writeLong(-1); // docsWithFieldOffset
      meta.writeLong(0L); // docsWithFieldLength
      meta.writeShort((short) -1); // jumpTableEntryCount
      meta.writeByte((byte) -1); // denseRankPower
    } else {
      long offset = data.getFilePointer();
      meta.writeLong(offset);  // docsWithFieldOffset
      values = valuesProducer.getSortedSet(field);
      final short jumpTableEntryCount = IndexedDISI.writeBitSet(values, data, IndexedDISI.DEFAULT_DENSE_RANK_POWER);// 会用bitmap映射
      meta.writeLong(data.getFilePointer() - offset); // docsWithFieldLength
      meta.writeShort(jumpTableEntryCount); // 多少个block，一个block 65536个文档
      meta.writeByte(IndexedDISI.DEFAULT_DENSE_RANK_POWER);
    }

    int numberOfBitsPerOrd = DirectWriter.unsignedBitsRequired(values.getValueCount() - 1);
    meta.writeByte((byte) numberOfBitsPerOrd); // bitsPerValue
    long start = data.getFilePointer();
    meta.writeLong(start); // ordsOffset
    DirectWriter writer = DirectWriter.getInstance(data, numOrds, numberOfBitsPerOrd);
    values = valuesProducer.getSortedSet(field);
    for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) { // 遍历所有的文档
      for (long ord = values.nextOrd(); ord != SortedSetDocValues.NO_MORE_ORDS; ord = values.nextOrd()) {//
        writer.add(ord);
      }
    }
    writer.finish();
    meta.writeLong(data.getFilePointer() - start); // ordsLength

    meta.writeInt(numDocsWithField);// 多少个文档有这个词
    start = data.getFilePointer();
    meta.writeLong(start); // addressesOffset
    meta.writeVInt(DIRECT_MONOTONIC_BLOCK_SHIFT);

    final DirectMonotonicWriter addressesWriter = DirectMonotonicWriter.getInstance(meta, data, numDocsWithField + 1, DIRECT_MONOTONIC_BLOCK_SHIFT);
    long addr = 0;
    addressesWriter.add(addr);
    values = valuesProducer.getSortedSet(field);
    for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
      values.nextOrd();
      addr++;
      while (values.nextOrd() != SortedSetDocValues.NO_MORE_ORDS) {
        addr++;
      }
      addressesWriter.add(addr); // 多少个是唯一的
    }
    addressesWriter.finish();
    meta.writeLong(data.getFilePointer() - start); // addressesLength

    addTermsDict(values);
  }
}
