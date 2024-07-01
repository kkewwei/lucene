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
package org.apache.lucene.codecs.lucene90;

import static org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat.DIRECT_MONOTONIC_BLOCK_SHIFT;
import static org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat.NUMERIC_BLOCK_SHIFT;
import static org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat.NUMERIC_BLOCK_SIZE;
import static org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat.SKIP_INDEX_LEVEL_SHIFT;
import static org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat.SKIP_INDEX_MAX_LEVEL;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.EmptyDocValuesProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.internal.hppc.LongHashSet;
import org.apache.lucene.internal.hppc.LongIntHashMap;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LongsRef;
import org.apache.lucene.util.MathUtil;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.compress.LZ4;
import org.apache.lucene.util.packed.DirectMonotonicWriter;
import org.apache.lucene.util.packed.DirectWriter;
// 写入时
/** writer for {@link Lucene90DocValuesFormat} */
final class Lucene90DocValuesConsumer extends DocValuesConsumer {

  IndexOutput data, meta, skipIndex;//  meta=/data1/_0_Lucene90_0.dvd, data=/data1/_0_Lucene90_0.dvm
  final int maxDoc;// 最大的文档id
  private byte[] termsDictBuffer;
  private final int skipIndexIntervalSize; // 默认4096

  /** expert: Creates a new writer */
  public Lucene90DocValuesConsumer(
      SegmentWriteState state,
      int skipIndexIntervalSize,
      String dataCodec,
      String dataExtension,
      String metaCodec,
      String metaExtension,
      String skipIndexCodec,
      String skipIndexExtension)
      throws IOException {
    this.termsDictBuffer = new byte[1 << 14];
    boolean success = false;
    try {
      String dataName = // dvd
          IndexFileNames.segmentFileName(
              state.segmentInfo.name, state.segmentSuffix, dataExtension);
      data = state.directory.createOutput(dataName, state.context);// 产生的是ByteSizeCachingDirectory,在merge阶段就变成了RateLimitedIndexOutput
      CodecUtil.writeIndexHeader(
          data,
          dataCodec,
          Lucene90DocValuesFormat.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      String metaName =// dvm
          IndexFileNames.segmentFileName(
              state.segmentInfo.name, state.segmentSuffix, metaExtension);
      meta = state.directory.createOutput(metaName, state.context);
      CodecUtil.writeIndexHeader(
          meta,
          metaCodec,
          Lucene90DocValuesFormat.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      String skipIndexName =
          IndexFileNames.segmentFileName(
              state.segmentInfo.name, state.segmentSuffix, skipIndexExtension);
      skipIndex = state.directory.createOutput(skipIndexName, state.context);
      CodecUtil.writeIndexHeader(
          skipIndex,
          skipIndexCodec,
          Lucene90DocValuesFormat.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      maxDoc = state.segmentInfo.maxDoc();
      this.skipIndexIntervalSize = skipIndexIntervalSize;
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(this);
      }
    }
  }
  // 关闭的时候刷新到dvm dvd中
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
      if (skipIndex != null) {
        CodecUtil.writeFooter(skipIndex);
      }
      success = true;
    } finally {
      if (success) {
        IOUtils.close(data, meta, skipIndex);
      } else {
        IOUtils.closeWhileHandlingException(data, meta, skipIndex);
      }
      meta = data = skipIndex = null;
    }
  }

  @Override
  public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer)
      throws IOException {
    meta.writeInt(field.number);
    meta.writeByte(Lucene90DocValuesFormat.NUMERIC);// 若是NUMERIC类型
    DocValuesProducer producer =
        new EmptyDocValuesProducer() {
          @Override
          public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
            return DocValues.singleton(valuesProducer.getNumeric(field)); //提供全新的段，保证一定是singleton
          }
        };
    if (field.docValuesSkipIndexType() != DocValuesSkipIndexType.NONE ) {
      writeSkipIndex(field, producer);
    }
    writeValues(field, producer, false);
  }

  private static class MinMaxTracker {
    long min, max, numValues, spaceInBits;// spaceInBits：记录需要多少个bit来装这些doc，

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

    /** Accumulate state from another tracker. */
    void update(MinMaxTracker other) {
      min = Math.min(min, other.min);
      max = Math.max(max, other.max);
      numValues += other.numValues;
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

  private static class SkipAccumulator {
    int minDocID;
    int maxDocID;
    int docCount;
    long minValue;
    long maxValue;

    SkipAccumulator(int docID) {
      minDocID = docID;
      minValue = Long.MAX_VALUE;
      maxValue = Long.MIN_VALUE;
      docCount = 0;
    }

    boolean isDone(int skipIndexIntervalSize, int valueCount, long nextValue, int nextDoc) {
      if (docCount < skipIndexIntervalSize) {//文档数超过4096个才行
        return false;
      }// 一般4096个文档后，都是要构建一个索引的
      // Once we reach the interval size, we will keep accepting documents if
      // - next doc value is not a multi-value
      // - current accumulator only contains a single value and next value is the same value
      // - the accumulator is dense and the next doc keeps the density (no gaps)
      return valueCount > 1 // 要么valueCount> 1
          || minValue != maxValue//最大最小不相等
          || minValue != nextValue// 也不是最小值
          || docCount != nextDoc - minDocID; // 解析来的值不连续
    }//只要满足一个，才能构建结构

    void accumulate(long value) {
      minValue = Math.min(minValue, value);
      maxValue = Math.max(maxValue, value);
    }

    void accumulate(SkipAccumulator other) {
      assert minDocID <= other.minDocID && maxDocID < other.maxDocID;
      maxDocID = other.maxDocID;
      minValue = Math.min(minValue, other.minValue);
      maxValue = Math.max(maxValue, other.maxValue);
      docCount += other.docCount;
    }

    void nextDoc(int docID) {
      maxDocID = docID;
      ++docCount;
    }

    public static SkipAccumulator merge(List<SkipAccumulator> list, int index, int length) {
      SkipAccumulator acc = new SkipAccumulator(list.get(index).minDocID);
      for (int i = 0; i < length; i++) {
        acc.accumulate(list.get(index + i));
      }
      return acc;
    }
  }
  // 刷新的时候会跑到这里； Number，Sorted和sortset单value，sort_number, sort_set多value也会进来；
  private void writeSkipIndex(FieldInfo field, DocValuesProducer valuesProducer)
      throws IOException {
    assert field.docValuesSkipIndexType() != DocValuesSkipIndexType.NONE;
    final long start = skipIndex.getFilePointer();//dvd文件的起始位置
    final SortedNumericDocValues values = valuesProducer.getSortedNumeric(field);
    long globalMaxValue = Long.MIN_VALUE;
    long globalMinValue = Long.MAX_VALUE;
    int globalDocCount = 0;// 总文档个数
    int globalMaxValueCount = 0;
    int maxDocId = -1;
    final List<SkipAccumulator> accumulators = new ArrayList<>();
    SkipAccumulator accumulator = null;
    final int maxAccumulators = 1 << (SKIP_INDEX_LEVEL_SHIFT * (SKIP_INDEX_MAX_LEVEL - 1));// 每512个SkipAccumulator（一个包含4096个文档）
    for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
      final int valueCount = values.docValueCount();
      final long firstValue = values.nextValue();// 这里先读取第一条数据，后面还是会统计该文档全量的value
      globalMaxValueCount = Math.max(globalMaxValueCount, valueCount);
      if (accumulator != null
          && accumulator.isDone(skipIndexIntervalSize, valueCount, firstValue, doc)) {// 文档个数超过了4096个
        globalMaxValue = Math.max(globalMaxValue, accumulator.maxValue);
        globalMinValue = Math.min(globalMinValue, accumulator.minValue);
        globalDocCount += accumulator.docCount;
        maxDocId = accumulator.maxDocID;
        accumulator = null;// 置位null
        if (accumulators.size() == maxAccumulators) {
          writeLevels(accumulators);//跑到这里的话，需要512*4096个文档才能进来
          accumulators.clear();
        }
      }
      if (accumulator == null) {// 第一次处理，仍然为null
        accumulator = new SkipAccumulator(doc);// 统计的是这个阶段的最大最小值
        accumulators.add(accumulator);// 已经放进去了
      }
      accumulator.nextDoc(doc);//统计文档数
      accumulator.accumulate(firstValue);//统计最大最小值
      for (int i = 1; i < valueCount; ++i) {
        accumulator.accumulate(values.nextValue());
      }
    }

    if (accumulators.isEmpty() == false) {
      globalMaxValue = Math.max(globalMaxValue, accumulator.maxValue);// 再计算下最后一次的最大最小值
      globalMinValue = Math.min(globalMinValue, accumulator.minValue);
      globalDocCount += accumulator.docCount;
      maxDocId = accumulator.maxDocID;
      writeLevels(accumulators);
    }
    meta.writeLong(start); // record the start in meta
    meta.writeLong(skipIndex.getFilePointer() - start); // record the length
    assert globalDocCount == 0 || globalMaxValue >= globalMinValue;
    meta.writeLong(globalMaxValue);
    meta.writeLong(globalMinValue);
    assert globalDocCount <= maxDocId + 1;
    meta.writeInt(globalDocCount);
    meta.writeInt(maxDocId);
    meta.writeInt(globalMaxValueCount);
  }
   // 就是写跳表结构
  private void writeLevels(List<SkipAccumulator> accumulators) throws IOException {
    final List<List<SkipAccumulator>> accumulatorsLevels = new ArrayList<>(SKIP_INDEX_MAX_LEVEL);// 构建每个级别跳表
    accumulatorsLevels.add(accumulators);// 第一级
    for (int i = 0; i < SKIP_INDEX_MAX_LEVEL - 1; i++) {
      accumulatorsLevels.add(buildLevel(accumulatorsLevels.get(i)));//构建下一级别的
    }
    int totalAccumulators = accumulators.size();
    for (int index = 0; index < totalAccumulators; index++) {
      // compute how many levels we need to write for the current accumulator
      final int levels = getLevels(index, totalAccumulators);// 看我们这个index需要放对应lever的SkipAccumulator
      // write the number of levels
      skipIndex.writeByte((byte) levels);// 当前支持的层级
      // write intervals in reverse order. This is done so we don't
      // need to read all of them in case of slipping
      for (int level = levels - 1; level >= 0; level--) {
        final SkipAccumulator accumulator =
            accumulatorsLevels.get(level).get(index >> (SKIP_INDEX_LEVEL_SHIFT * level));
        skipIndex.writeInt(accumulator.maxDocID);
        skipIndex.writeInt(accumulator.minDocID);
        skipIndex.writeLong(accumulator.maxValue);
        skipIndex.writeLong(accumulator.minValue);
        skipIndex.writeInt(accumulator.docCount);
      }
    }
  }

  private static List<SkipAccumulator> buildLevel(List<SkipAccumulator> accumulators) {// 根据上一级的个数，来总结下一级
    final int levelSize = 1 << SKIP_INDEX_LEVEL_SHIFT;// 为8，每8个SkipAccumulator来创建一个跳表
    final List<SkipAccumulator> collector = new ArrayList<>();
    for (int i = 0; i < accumulators.size() - levelSize + 1; i += levelSize) {// 至少8个才能创建跳表
      collector.add(SkipAccumulator.merge(accumulators, i, levelSize));// 上个级别的8个SkipAccumulator继续构建一个新的
    }
    return collector;
  }

  private static int getLevels(int index, int size) {
    if (Integer.numberOfTrailingZeros(index) >= SKIP_INDEX_LEVEL_SHIFT) {// 只有8的倍数，才会计算跳表
      // TODO: can we do it in constant time rather than linearly with SKIP_INDEX_MAX_LEVEL?
      final int left = size - index;
      for (int level = SKIP_INDEX_MAX_LEVEL - 1; level > 0; level--) {// 从最高级别往下看，看符合哪个级别
        final int numberIntervals = 1 << (SKIP_INDEX_LEVEL_SHIFT * level);// 多少级别
        if (left >= numberIntervals && index % numberIntervals == 0) {// 必须是8的偶数倍才支持跳表结果
          return level + 1;
        }
      }
    }
    return 1;
  }
   // number，sort，sort_set，sort_number也会进来会进来。ords表明这个通过values.nextValue()读取的value都是order，比如sort_set
  private long[] writeValues(FieldInfo field, DocValuesProducer valuesProducer, boolean ords)
      throws IOException {
    SortedNumericDocValues values = valuesProducer.getSortedNumeric(field);//SortedSetSelector$MinValue
    final long firstValue;
    if (values.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
      firstValue = values.nextValue();// 首先读取第一个词。若是sortset的话，返回的是词的排序order
    } else {
      firstValue = 0L;
    }// 以下统计有4个目的，最大最小值统计，最大公约数，uniqueValues个数，总共多少个文档
    values = valuesProducer.getSortedNumeric(field);// 获取已经存在的这个软删除dv文档
    int numDocsWithValue = 0;// 总共多少个文档
    MinMaxTracker minMax = new MinMaxTracker();// 最大最小记录器
    MinMaxTracker blockMinMax = new MinMaxTracker();
    long gcd = 0;//最大公约数，一般记作称GCD
    LongHashSet uniqueValues = ords ? null : new LongHashSet();// 独立统计value个数。若是order就没必要统计
    for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
      for (int i = 0, count = values.docValueCount(); i < count; ++i) {// 读取该字段字的value个数。若single的话，看起来没有必要进行循环 kewei
        long v = values.nextValue();

        if (gcd != 1) {// 说明还有最大公约数
          if (v < Long.MIN_VALUE / 2 || v > Long.MAX_VALUE / 2) {
            // in that case v - minValue might overflow and make the GCD computation return
            // wrong results. Since these extreme values are unlikely, we just discard
            // GCD computation for them
            gcd = 1;
          } else {
            gcd = MathUtil.gcd(gcd, v - firstValue);
          }
        }

        blockMinMax.update(v);// 每隔16384个value,最大最下值,以及value个数。没啥用
        if (blockMinMax.numValues == NUMERIC_BLOCK_SIZE) {//看记录的个数超过16384，numValues包含重复的value个数
          minMax.update(blockMinMax);// 更新全局的min/max,以及value个数
          blockMinMax.nextBlock();// 清掉局部数据，
        }

        if (uniqueValues != null && uniqueValues.add(v) && uniqueValues.size() > 256) {
          uniqueValues = null;//超过256个value，就不再单独统计value了
        }
      }

      numDocsWithValue++;
    }

    minMax.update(blockMinMax);
    minMax.finish();
    blockMinMax.finish();

    if (ords && minMax.numValues > 0) {// 有多个value(重复的也算多个)
      if (minMax.min != 0) {
        throw new IllegalStateException(
            "The min value for ordinals should always be 0, got " + minMax.min);
      }
      if (minMax.max != 0 && gcd != 1) {
        throw new IllegalStateException(
            "GCD compression should never be used on ordinals, found gcd=" + gcd);
      }
    }

    final long numValues = minMax.numValues;
    long min = minMax.min;
    final long max = minMax.max;
    assert blockMinMax.spaceInBits <= minMax.spaceInBits;

    if (numDocsWithValue == 0) { // meta[-2, 0]: No documents with values
      meta.writeLong(-2); // docsWithFieldOffset
      meta.writeLong(0L); // docsWithFieldLength
      meta.writeShort((short) -1); // jumpTableEntryCount
      meta.writeByte((byte) -1); // denseRankPower
    } else if (numDocsWithValue == maxDoc) { //每个文档都有value meta[-1, 0]: All documents has values
      meta.writeLong(-1); // docsWithFieldOffset
      meta.writeLong(0L); // docsWithFieldLength
      meta.writeShort((short) -1); // jumpTableEntryCount
      meta.writeByte((byte) -1); // denseRankPower
    } else { // meta[data.offset, data.length]: IndexedDISI structure for documents with values
      long offset = data.getFilePointer();// 部分字段有该值
      meta.writeLong(offset); // docsWithFieldOffset
      values = valuesProducer.getSortedNumeric(field);
      final short jumpTableEntryCount =
          IndexedDISI.writeBitSet(values, data, IndexedDISI.DEFAULT_DENSE_RANK_POWER);
      meta.writeLong(data.getFilePointer() - offset); // docsWithFieldLength
      meta.writeShort(jumpTableEntryCount);//返回多少个block
      meta.writeByte(IndexedDISI.DEFAULT_DENSE_RANK_POWER);
    }

    meta.writeLong(numValues);// 总共多少个value
    final int numBitsPerValue;
    boolean doBlocks = false;
    LongIntHashMap encode = null;
    if (min >= max) { // meta[-1]: All values are 0
      numBitsPerValue = 0;
      meta.writeInt(-1); // tablesize。 独立词个数
    } else {
      if (uniqueValues != null // 说明独立个数不超过256个，那是好事。太少了
          && uniqueValues.size() > 1
          && DirectWriter.unsignedBitsRequired(uniqueValues.size() - 1)
              < DirectWriter.unsignedBitsRequired((max - min) / gcd)) { // 这个是干啥的
        numBitsPerValue = DirectWriter.unsignedBitsRequired(uniqueValues.size() - 1);
        final long[] sortedUniqueValues = uniqueValues.toArray();
        Arrays.sort(sortedUniqueValues);
        meta.writeInt(sortedUniqueValues.length); // tablesize
        for (long v : sortedUniqueValues) {
          meta.writeLong(v); // table[] entry
        }
        encode = new LongIntHashMap();
        for (int i = 0; i < sortedUniqueValues.length; ++i) {
          encode.put(sortedUniqueValues[i], i);
        }
        min = 0;
        gcd = 1;
      } else {// 一般都进来的
        uniqueValues = null;
        // we do blocks if that appears to save 10+% storage
        doBlocks = //  使用bitset来装的需要多少，
            minMax.spaceInBits > 0 && (double) blockMinMax.spaceInBits / minMax.spaceInBits <= 0.9;
        if (doBlocks) {// 是否压缩存储，压缩的啥？
          numBitsPerValue = 0xFF;
          meta.writeInt(-2 - NUMERIC_BLOCK_SHIFT); // tablesize
        } else {//一般跑这里
          numBitsPerValue = DirectWriter.unsignedBitsRequired((max - min) / gcd);
          if (gcd == 1
              && min > 0
              && DirectWriter.unsignedBitsRequired(max)
                  == DirectWriter.unsignedBitsRequired(max - min)) {
            min = 0;
          }
          meta.writeInt(-1); // tablesize 。 独立词个数
        }
      }
    }

    meta.writeByte((byte) numBitsPerValue);// 待使用的空间
    meta.writeLong(min);
    meta.writeLong(gcd);
    long startOffset = data.getFilePointer();
    meta.writeLong(startOffset); // valueOffset
    long jumpTableOffset = -1;
    if (doBlocks) {// 值得压缩
      jumpTableOffset = writeValuesMultipleBlocks(valuesProducer.getSortedNumeric(field), gcd);
    } else if (numBitsPerValue != 0) {
      writeValuesSingleBlock(//不值得压缩
          valuesProducer.getSortedNumeric(field), numValues, numBitsPerValue, min, gcd, encode);
    }
    meta.writeLong(data.getFilePointer() - startOffset); // valuesLength
    meta.writeLong(jumpTableOffset);
    return new long[] {numDocsWithValue, numValues};
  }
  // number，sort，sort_set，sort_number也会进来会进来
  private void writeValuesSingleBlock(
      SortedNumericDocValues values,
      long numValues,// 总共词的个数
      int numBitsPerValue,
      long min,
      long gcd,
      LongIntHashMap encode)
      throws IOException {
    DirectWriter writer = DirectWriter.getInstance(data, numValues, numBitsPerValue);// 就是看单个value占据的宽度numBitsPerValue
    for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {// 不该循环
      for (int i = 0, count = values.docValueCount(); i < count; ++i) {
        long v = values.nextValue();// 若是sortedset的话， 返回的是这个term的排序order。若是sortednumber，则是value原始值
        if (encode == null) {// 为啥空的时候需要这样存储
          writer.add((v - min) / gcd);
        } else {
          writer.add(encode.get(v));
        }
      }
    }
    writer.finish();
  }

  // Returns the offset to the jump-table for vBPV
  private long writeValuesMultipleBlocks(SortedNumericDocValues values, long gcd)
      throws IOException {
    long[] offsets = new long[ArrayUtil.oversize(1, Long.BYTES)];
    int offsetsIndex = 0;
    final long[] buffer = new long[NUMERIC_BLOCK_SIZE];
    final ByteBuffersDataOutput encodeBuffer = ByteBuffersDataOutput.newResettableInstance();
    int upTo = 0;
    for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
      for (int i = 0, count = values.docValueCount(); i < count; ++i) {
        buffer[upTo++] = values.nextValue();
        if (upTo == NUMERIC_BLOCK_SIZE) {
          offsets = ArrayUtil.grow(offsets, offsetsIndex + 1);
          offsets[offsetsIndex++] = data.getFilePointer();
          writeBlock(buffer, NUMERIC_BLOCK_SIZE, gcd, encodeBuffer);
          upTo = 0;
        }
      }
    }
    if (upTo > 0) {
      offsets = ArrayUtil.grow(offsets, offsetsIndex + 1);
      offsets[offsetsIndex++] = data.getFilePointer();
      writeBlock(buffer, upTo, gcd, encodeBuffer);
    }

    // All blocks has been written. Flush the offset jump-table
    final long offsetsOrigo = data.getFilePointer();
    for (int i = 0; i < offsetsIndex; i++) {
      data.writeLong(offsets[i]);
    }
    data.writeLong(offsetsOrigo);
    return offsetsOrigo;
  }

  private void writeBlock(long[] values, int length, long gcd, ByteBuffersDataOutput buffer)
      throws IOException {
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
      final int bitsPerValue = DirectWriter.unsignedBitsRequired((max - min) / gcd);
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
  // 比如向量字段
  @Override
  public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
    meta.writeInt(field.number);
    meta.writeByte(Lucene90DocValuesFormat.BINARY);

    BinaryDocValues values = valuesProducer.getBinary(field);
    long start = data.getFilePointer();
    meta.writeLong(start); // dataOffset
    int numDocsWithField = 0;
    int minLength = Integer.MAX_VALUE;
    int maxLength = 0;
    for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
      numDocsWithField++;
      BytesRef v = values.binaryValue();// 读取二进制
      int length = v.length;
      data.writeBytes(v.bytes, v.offset, v.length);
      minLength = Math.min(length, minLength);
      maxLength = Math.max(length, maxLength);
    }
    assert numDocsWithField <= maxDoc;
    meta.writeLong(data.getFilePointer() - start); // dataLength

    if (numDocsWithField == 0) {
      meta.writeLong(-2); // docsWithFieldOffset
      meta.writeLong(0L); // docsWithFieldLength
      meta.writeShort((short) -1); // jumpTableEntryCount
      meta.writeByte((byte) -1); // denseRankPower
    } else if (numDocsWithField == maxDoc) {//每个文档都有该值
      meta.writeLong(-1); // docsWithFieldOffset
      meta.writeLong(0L); // docsWithFieldLength
      meta.writeShort((short) -1); // jumpTableEntryCount
      meta.writeByte((byte) -1); // denseRankPower
    } else {
      long offset = data.getFilePointer(); // 获取文件写入的地方，即将写入docId
      meta.writeLong(offset); // docsWithFieldOffset     存放到meta中
      values = valuesProducer.getBinary(field); //SortedSetSelector$MinValue
      final short jumpTableEntryCount =
          IndexedDISI.writeBitSet(values, data, IndexedDISI.DEFAULT_DENSE_RANK_POWER);
      meta.writeLong(data.getFilePointer() - offset); // docsWithFieldLength
      meta.writeShort(jumpTableEntryCount); // 记录多少个block
      meta.writeByte(IndexedDISI.DEFAULT_DENSE_RANK_POWER);
    }

    meta.writeInt(numDocsWithField); // 文档个数
    meta.writeInt(minLength);
    meta.writeInt(maxLength);
    if (maxLength > minLength) {
      start = data.getFilePointer();
      meta.writeLong(start);
      meta.writeVInt(DIRECT_MONOTONIC_BLOCK_SHIFT);

      final DirectMonotonicWriter writer =// 一起写二进制的长度，value在前面存储了
          DirectMonotonicWriter.getInstance(
              meta, data, numDocsWithField + 1, DIRECT_MONOTONIC_BLOCK_SHIFT);
      long addr = 0;
      writer.add(addr);
      values = valuesProducer.getBinary(field);
      for (int doc = values.nextDoc();
          doc != DocIdSetIterator.NO_MORE_DOCS;
          doc = values.nextDoc()) {
        addr += values.binaryValue().length;
        writer.add(addr);
      }
      writer.finish();
      meta.writeLong(data.getFilePointer() - start);
    }
  }

  @Override
  public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
    meta.writeInt(field.number);
    meta.writeByte(Lucene90DocValuesFormat.SORTED);
    doAddSortedField(field, valuesProducer, false);
  }
  /// 存储termId，参考 Lucene80DocValeusProducer.readSorted() // seqenceId
  private void doAddSortedField(//addNumericField, doAddSortedField,  addSortedNumericField, addSortedSetField都会进来。addTypeByte就是说value为单值多值
      FieldInfo field, DocValuesProducer valuesProducer, boolean addTypeByte) throws IOException {
    DocValuesProducer producer =
        new EmptyDocValuesProducer() {
          @Override
          public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
            SortedDocValues sorted = valuesProducer.getSorted(field);// 会转变成一个读取SortedNumericDocValues
            NumericDocValues sortedOrds =
                new NumericDocValues() {
                  @Override
                  public long longValue() throws IOException {
                    return sorted.ordValue();// 返回的是这个term的排序order
                  }

                  @Override
                  public boolean advanceExact(int target) throws IOException {
                    return sorted.advanceExact(target);
                  }

                  @Override
                  public int docID() {
                    return sorted.docID();
                  }

                  @Override
                  public int nextDoc() throws IOException {
                    return sorted.nextDoc();
                  }

                  @Override
                  public int advance(int target) throws IOException {
                    return sorted.advance(target);
                  }

                  @Override
                  public long cost() {
                    return sorted.cost();
                  }

                  @Override
                  public void intoBitSet(int upTo, FixedBitSet bitSet, int offset)
                      throws IOException {
                    sorted.intoBitSet(upTo, bitSet, offset);
                  }

                  @Override
                  public int docIDRunEnd() throws IOException {
                    return sorted.docIDRunEnd();
                  }
                };
            return DocValues.singleton(sortedOrds);
          }
        };
    if (field.docValuesSkipIndexType() != DocValuesSkipIndexType.NONE) {
      writeSkipIndex(field, producer);
    }// 这个参数两用了，写入时，只有只有sortset的单值写入的addTypeByte=true，为0，读取时代表单值。
    if (addTypeByte) {//sort是不会加这个属性的。
      meta.writeByte((byte) 0); // multiValued (0 = singleValued)  //
    }
    writeValues(field, producer, true);
    addTermsDict(DocValues.singleton(valuesProducer.getSorted(field)));
  }
  // 16个词一个索引。写索引结构
  private void addTermsDict(SortedSetDocValues values) throws IOException {
    final long size = values.getValueCount(); // 多少个词cardinatory的个数
    meta.writeVLong(size);

    int blockMask = Lucene90DocValuesFormat.TERMS_DICT_BLOCK_LZ4_MASK;// 一个block 64个
    int shift = Lucene90DocValuesFormat.TERMS_DICT_BLOCK_LZ4_SHIFT;

    meta.writeInt(DIRECT_MONOTONIC_BLOCK_SHIFT);// 16
    ByteBuffersDataOutput addressBuffer = new ByteBuffersDataOutput();
    ByteBuffersIndexOutput addressOutput =
        new ByteBuffersIndexOutput(addressBuffer, "temp", "temp");
    long numBlocks = (size + blockMask) >>> shift; // 一个block为16，可以存放多少个block
    DirectMonotonicWriter writer =// 单调递增写入
        DirectMonotonicWriter.getInstance(
            meta, addressOutput, numBlocks, DIRECT_MONOTONIC_BLOCK_SHIFT);

    BytesRefBuilder previous = new BytesRefBuilder();
    long ord = 0;
    long start = data.getFilePointer();
    int maxLength = 0, maxBlockLength = 0;
    TermsEnum iterator = values.termsEnum();// SortedDocValuesTermsEnum

    LZ4.FastCompressionHashTable ht = new LZ4.FastCompressionHashTable();// lz4压缩词典
    ByteArrayDataOutput bufferedOutput = new ByteArrayDataOutput(termsDictBuffer);
    int dictLength = 0;

    for (BytesRef term = iterator.next(); term != null; term = iterator.next()) { // 从第0、1、2、3...小的大小排序词逐个读取
      if ((ord & blockMask) == 0) {// 每过64个词典value
        if (ord != 0) {// 若是64整数倍
          // flush the previous block
          final int uncompressedLength =
              compressAndGetTermsDictBlockLength(bufferedOutput, dictLength, ht);// 直接向文件写入了
          maxBlockLength = Math.max(maxBlockLength, uncompressedLength);// 最大
          bufferedOutput.reset(termsDictBuffer);// 并且直接充值
        }

        writer.add(data.getFilePointer() - start); // 向writer中写入一次data使用位置
        // Write the first term both to the index output, and to the buffer where we'll use it as a
        // dictionary for compression
        data.writeVInt(term.length);// 写入真实的term长度
        data.writeBytes(term.bytes, term.offset, term.length);//写入真实的term的二进制数据，压缩从头开始
        bufferedOutput = maybeGrowBuffer(bufferedOutput, term.length);
        bufferedOutput.writeBytes(term.bytes, term.offset, term.length);
        dictLength = term.length;
      } else {// 存放比较神奇，只要从0-15个词开始往后读，前缀不变，每次读取时，只要改变下后缀就行了
        final int prefixLength = StringHelper.bytesDifference(previous.get(), term);
        final int suffixLength = term.length - prefixLength;
        assert suffixLength > 0; // terms are unique
        // Will write (suffixLength + 1 byte + 2 vint) bytes. Grow the buffer in need.
        bufferedOutput = maybeGrowBuffer(bufferedOutput, suffixLength + 11);
        bufferedOutput.writeByte(// 相同前缀用低4位，不同的后缀用高4位
            (byte) (Math.min(prefixLength, 15) | (Math.min(15, suffixLength - 1) << 4)));// 高4位和低4位
        if (prefixLength >= 15) {// 若前缀大于15，再接着另存
          bufferedOutput.writeVInt(prefixLength - 15);
        }
        if (suffixLength >= 16) {// 后缀大于16，再接着另存
          bufferedOutput.writeVInt(suffixLength - 16);
        } // dvd保存term内容，存储的是不同后缀的长度
        bufferedOutput.writeBytes(term.bytes, term.offset + prefixLength, suffixLength);// 仅仅存后缀
      }
      maxLength = Math.max(maxLength, term.length);// 统计最大term大小
      previous.copyBytes(term);
      ++ord;
    }
    // Compress and write out the last block
    if (bufferedOutput.getPosition() > dictLength) {
      final int uncompressedLength =
          compressAndGetTermsDictBlockLength(bufferedOutput, dictLength, ht);
      maxBlockLength = Math.max(maxBlockLength, uncompressedLength);
    }

    writer.finish();// 将每隔15个词在dvm和dvd中的位置给记录下来（一级索引，dvm放偏移的元数据，dvd才是放的偏移的真实数据）
    meta.writeInt(maxLength);
    // Write one more int for storing max block length.
    meta.writeInt(maxBlockLength);
    meta.writeLong(start);
    meta.writeLong(data.getFilePointer() - start);
    start = data.getFilePointer();
    addressBuffer.copyTo(data); // 将第16*x个域的值在dvm中的位置给记录下来给存储到dvd中
    meta.writeLong(start);// dvd中起始位置
    meta.writeLong(data.getFilePointer() - start);
// 第三层，记录 term 字典的索引，values 是按照值 hash 排过序的，这里每 1024 条抽取一个作为索引，加速查询
    // Now write the reverse terms index
    writeTermsIndex(values);
  }// 二级索引是相同长度前缀

  private int compressAndGetTermsDictBlockLength(
      ByteArrayDataOutput bufferedOutput, int dictLength, LZ4.FastCompressionHashTable ht)
      throws IOException {
    int uncompressedLength = bufferedOutput.getPosition() - dictLength;
    data.writeVInt(uncompressedLength);
    LZ4.compressWithDictionary(termsDictBuffer, 0, dictLength, uncompressedLength, data, ht);
    return uncompressedLength;
  }

  private ByteArrayDataOutput maybeGrowBuffer(ByteArrayDataOutput bufferedOutput, int termLength) {
    int pos = bufferedOutput.getPosition(), originalLength = termsDictBuffer.length;
    if (pos + termLength >= originalLength - 1) {
      termsDictBuffer = ArrayUtil.grow(termsDictBuffer, originalLength + termLength);
      bufferedOutput = new ByteArrayDataOutput(termsDictBuffer, pos, termsDictBuffer.length - pos);
    }
    return bufferedOutput;
  }// 二级索引是相同长度前缀
  // TermsIndex是TermsDict索引, TermsDict是16个term一个索引，而 TermsIndex是1024一个索引结构
  private void writeTermsIndex(SortedSetDocValues values) throws IOException {
    final long size = values.getValueCount(); // segment范围内所有文档相同域distinct(term)的个数
    meta.writeInt(Lucene90DocValuesFormat.TERMS_DICT_REVERSE_INDEX_SHIFT); // 字典间隔1024
    long start = data.getFilePointer();

    long numBlocks =
        1L
            + ((size + Lucene90DocValuesFormat.TERMS_DICT_REVERSE_INDEX_MASK)
                >>> Lucene90DocValuesFormat.TERMS_DICT_REVERSE_INDEX_SHIFT);
    ByteBuffersDataOutput addressBuffer = new ByteBuffersDataOutput();
    DirectMonotonicWriter writer;
    try (ByteBuffersIndexOutput addressOutput =
        new ByteBuffersIndexOutput(addressBuffer, "temp", "temp")) {
      writer = // 也是使用这玩意写入数据
          DirectMonotonicWriter.getInstance(
              meta, addressOutput, numBlocks, DIRECT_MONOTONIC_BLOCK_SHIFT);
      TermsEnum iterator = values.termsEnum();
      BytesRefBuilder previous = new BytesRefBuilder();
      long offset = 0; // 相同前缀的累加值
      long ord = 0;
      for (BytesRef term = iterator.next(); term != null; term = iterator.next()) {// 就segment内唯一某个域的distinct(词)的存储，依次遍历
        if ((ord & Lucene90DocValuesFormat.TERMS_DICT_REVERSE_INDEX_MASK) == 0) { // // 每隔1024个词存一次也就是1024*-1个词进来一次
          writer.add(offset);// 存储的是第二级别相同长度
          final int sortKeyLength;
          if (ord == 0) {
            // no previous term: no bytes to write
            sortKeyLength = 0;
          } else { // 相同的前缀
            sortKeyLength = StringHelper.sortKeyLength(previous.get(), term);
          }
          offset += sortKeyLength;// 累加的目的主要是为了便于存储到DirectMonotonicWriter中（满足单调递增的）
          data.writeBytes(term.bytes, term.offset, sortKeyLength);// dvd  和前一个词相比，存储相同的前缀内容
        } else if ((ord & Lucene90DocValuesFormat.TERMS_DICT_REVERSE_INDEX_MASK) // 1024*x + 1023
            == Lucene90DocValuesFormat.TERMS_DICT_REVERSE_INDEX_MASK) {
          previous.copyBytes(term); // 每次找到第1024*x + 1023个词，主要是为了获取该词，为第1024*(x+1)个词找相同的前缀
        }// 这个省了点空间：只用存储相同的前缀，而不是全量前缀，也可以大致判断待查的词是在后面，还是前面。
        ++ord;
      }// 二级索引由两部分构成，一部分是第1024*x个词的相同前缀内容，第二部分是第1024*x个词和第1024*x-1个词的相同前缀累加值（数组会放在dvm和dvd中）
      writer.add(offset);
      writer.finish();
      meta.writeLong(start);
      meta.writeLong(data.getFilePointer() - start);// 存放二级索引每第1024个词相同前缀内容
      start = data.getFilePointer();
      addressBuffer.copyTo(data);
      meta.writeLong(start); // 往meta中写入
      meta.writeLong(data.getFilePointer() - start);
    }
  }
  // int刷新会进来
  @Override
  public void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer)
      throws IOException {
    meta.writeInt(field.number);// 见Lucene80DocValuesProducer.readFields()首先读取段号+类型
    meta.writeByte(Lucene90DocValuesFormat.SORTED_NUMERIC); // 标配
    doAddSortedNumericField(field, valuesProducer, false);
  }
  // sort_number单值, sort_set多value会都会进来
  private void doAddSortedNumericField(
      FieldInfo field, DocValuesProducer valuesProducer, boolean ords) throws IOException {
    if (field.docValuesSkipIndexType() != DocValuesSkipIndexType.NONE) {
      writeSkipIndex(field, valuesProducer);
    }// 写入：既表示sortset的多值写入
    if (ords) {//只有sortedset时才会设置为true， value是否是order（数字型读取时直接跳过读取）,若是order的时候，也代表是multivalues
      meta.writeByte((byte) 1); // multiValued (1 = multiValued)
    }
    long[] stats = writeValues(field, valuesProducer, ords);// 包括写了跳表。(返回numDocsWithValue, numValues)
    int numDocsWithField = Math.toIntExact(stats[0]);
    long numValues = stats[1];
    assert numValues >= numDocsWithField;

    meta.writeInt(numDocsWithField);// 多少个doc包含了这个field
    if (numValues > numDocsWithField) {//说明有的doc的Value有多个词
      long start = data.getFilePointer();
      meta.writeLong(start);
      meta.writeVInt(DIRECT_MONOTONIC_BLOCK_SHIFT);

      final DirectMonotonicWriter addressesWriter =
          DirectMonotonicWriter.getInstance(
              meta, data, numDocsWithField + 1L, DIRECT_MONOTONIC_BLOCK_SHIFT);
      long addr = 0;
      addressesWriter.add(addr);
      SortedNumericDocValues values = valuesProducer.getSortedNumeric(field);
      for (int doc = values.nextDoc();
          doc != DocIdSetIterator.NO_MORE_DOCS;
          doc = values.nextDoc()) {
        addr += values.docValueCount();// 每个文档有几个value
        addressesWriter.add(addr);
      }
      addressesWriter.finish();
      meta.writeLong(data.getFilePointer() - start);// 统计“每个文档有几个value”存储的总长度
    }
  }

  private static boolean isSingleValued(SortedSetDocValues values) throws IOException {
    if (DocValues.unwrapSingleton(values) != null) {
      return true;
    }

    assert values.docID() == -1;
    for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) { //遍历每个文档，
      int docValueCount = values.docValueCount();// 总文档数
      assert docValueCount > 0;
      if (docValueCount > 1) {
        return false;
      }
    }
    return true;
  }
  /// keyword字段类型会进来
  @Override// 映射 Lucene80DocValuesConsumer.readFields()
  public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer)
      throws IOException {
    meta.writeInt(field.number);// 域number
    meta.writeByte(Lucene90DocValuesFormat.SORTED_SET); // 该域存储类型

    if (isSingleValued(valuesProducer.getSortedSet(field))) {// 只要包含这个字段的每个文档，都有一个值

      doAddSortedField(
          field,
          new EmptyDocValuesProducer() {
            @Override
            public SortedDocValues getSorted(FieldInfo field) throws IOException {
              return SortedSetSelector.wrap(
                  valuesProducer.getSortedSet(field), SortedSetSelector.Type.MIN);// 读取最小的那个值
            }
          },
          true);
      return;
    }

    doAddSortedNumericField(
        field,
        new EmptyDocValuesProducer() {
          @Override
          public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
            SortedSetDocValues values = valuesProducer.getSortedSet(field);
            return new SortedNumericDocValues() {

              long[] ords = LongsRef.EMPTY_LONGS;
              int i, docValueCount;

              @Override
              public long nextValue() throws IOException {/// getValue，给的是orderId
                return ords[i++];
              }

              @Override
              public int docValueCount() {
                return docValueCount;
              }

              @Override
              public boolean advanceExact(int target) throws IOException {
                throw new UnsupportedOperationException();
              }

              @Override
              public int docID() {
                return values.docID();
              }

              @Override
              public int nextDoc() throws IOException {
                int doc = values.nextDoc();
                if (doc != NO_MORE_DOCS) {
                  docValueCount = values.docValueCount();// 这个doc有几个term
                  ords = ArrayUtil.grow(ords, docValueCount);// 返回的是order顺序
                  for (int j = 0; j < docValueCount; j++) {
                    ords[j] = values.nextOrd();
                  }
                  i = 0;
                }
                return doc;
              }

              @Override
              public int advance(int target) throws IOException {
                throw new UnsupportedOperationException();
              }

              @Override
              public long cost() {
                return values.cost();
              }

              @Override
              public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
                values.intoBitSet(upTo, bitSet, offset);
              }

              @Override
              public int docIDRunEnd() throws IOException {
                return values.docIDRunEnd();
              }
            };
          }
        },
        true);
    // 相比普通的integer，这里是多了词典部分
    addTermsDict(valuesProducer.getSortedSet(field));
  }
}
