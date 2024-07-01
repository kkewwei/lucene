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

import static org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat.SKIP_INDEX_JUMP_LENGTH_PER_LEVEL;
import static org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat.SKIP_INDEX_MAX_LEVEL;
import static org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat.TERMS_DICT_BLOCK_LZ4_SHIFT;

import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BaseTermsEnum;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.TermsEnum.SeekStatus;
import org.apache.lucene.internal.hppc.IntObjectHashMap;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.FileTypeHint;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.compress.LZ4;
import org.apache.lucene.util.packed.DirectMonotonicReader;
import org.apache.lucene.util.packed.DirectReader;

/** reader for {@link Lucene90DocValuesFormat} */ // 查询    里面包含全量的该segment的所有的DocValues字段，每个节点重启的时候就会加载一个segment的元数据
final class Lucene90DocValuesProducer extends DocValuesProducer {
  private final IntObjectHashMap<NumericEntry> numerics;// 存放数值类型
  private final IntObjectHashMap<BinaryEntry> binaries;// 存放二进制, 节点启动的时候就会加载
  private final IntObjectHashMap<SortedEntry> sorted;// 存放sorted类型
  private final IntObjectHashMap<SortedSetEntry> sortedSets;//  存放sortedSet类型
  private final IntObjectHashMap<SortedNumericEntry> sortedNumerics;//  存放sortedNumer类型
  private final IntObjectHashMap<DocValuesSkipperEntry> skippers;// long类型进行，大小对比的文件
  private final IndexInput data;// 仅仅映射该segment dvd全量数据
  private final IndexInput skipIndexData;
  private final int maxDoc;
  private int version = -1;
  private final boolean merging;
// segment加载的时候就会进来，将该segment所有字段的DocValues都加载进来
  /** expert: instantiates a new reader */
  Lucene90DocValuesProducer(
      SegmentReadState state,
      String dataCodec,
      String dataExtension,
      String metaCodec,
      String metaExtension,
      String skipIndexCodec,
      String skipIndexExtension)
      throws IOException {
    String metaName =// dvm
        IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);
    this.maxDoc = state.segmentInfo.maxDoc();
    numerics = new IntObjectHashMap<>();
    binaries = new IntObjectHashMap<>();
    sorted = new IntObjectHashMap<>();
    sortedSets = new IntObjectHashMap<>();
    sortedNumerics = new IntObjectHashMap<>();
    skippers = new IntObjectHashMap<>();
    merging = false;

    // read in the entries from the metadata file.
    try (ChecksumIndexInput in = state.directory.openChecksumInput(metaName)) {
      Throwable priorE = null;

      try {
        version =
            CodecUtil.checkIndexHeader(
                in,
                metaCodec,
                Lucene90DocValuesFormat.VERSION_START,
                Lucene90DocValuesFormat.VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix);

        readFields(in, state.fieldInfos);

        if (version < Lucene90DocValuesFormat.VERSION_SKIPPER_MAX_VALUE_COUNT) {
          inferMaxValueCounts(state.fieldInfos);
        }

      } catch (Throwable exception) {
        priorE = exception;
      } finally {
        CodecUtil.checkFooter(in, priorE);
      }
    }

    String dataName =
        IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, dataExtension);
    // Doc-values have a forward-only access pattern
    this.data = state.directory.openInput(dataName, state.context.withHints(FileTypeHint.DATA));
    boolean success = false;
    try {
      final int version2 =
          CodecUtil.checkIndexHeader(
              data,
              dataCodec,
              Lucene90DocValuesFormat.VERSION_START,
              Lucene90DocValuesFormat.VERSION_CURRENT,
              state.segmentInfo.getId(),
              state.segmentSuffix);
      if (version != version2) {
        throw new CorruptIndexException(
            "Format versions mismatch: meta=" + version + ", data=" + version2, data);
      }

      // NOTE: data file is too costly to verify checksum against all the bytes on open,
      // but for now we at least verify proper structure of the checksum footer: which looks
      // for FOOTER_MAGIC + algorithmID. This is cheap and can detect some forms of corruption
      // such as file truncation.
      CodecUtil.retrieveChecksum(data);

      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(this.data);
      }
    }

    if (version >= Lucene90DocValuesFormat.VERSION_SKIPPER_SEPARATE_FILE) {
      IndexInput skipIn = null;
      try {
        String skipIndexName =
            IndexFileNames.segmentFileName(
                state.segmentInfo.name, state.segmentSuffix, skipIndexExtension);
        skipIn =
            state.directory.openInput(skipIndexName, state.context.withHints(FileTypeHint.INDEX));
        final int skipVersion =
            CodecUtil.checkIndexHeader(
                skipIn,
                skipIndexCodec,
                Lucene90DocValuesFormat.VERSION_SKIPPER_SEPARATE_FILE,
                Lucene90DocValuesFormat.VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix);
        if (version != skipVersion) {
          throw new CorruptIndexException(
              "Format versions mismatch: meta=" + version + ", skipIndex=" + skipVersion, skipIn);
        }
        CodecUtil.retrieveChecksum(skipIn);
      } catch (Throwable t) {
        IOUtils.closeWhileSuppressingExceptions(t, data, skipIn);
        throw t;
      }
      this.skipIndexData = skipIn;
    } else {
      this.skipIndexData = null;
    }
  }

  // Used for cloning
  private Lucene90DocValuesProducer(
      IntObjectHashMap<NumericEntry> numerics,
      IntObjectHashMap<BinaryEntry> binaries,
      IntObjectHashMap<SortedEntry> sorted,
      IntObjectHashMap<SortedSetEntry> sortedSets,
      IntObjectHashMap<SortedNumericEntry> sortedNumerics,
      IntObjectHashMap<DocValuesSkipperEntry> skippers,
      IndexInput data,
      IndexInput skipIndexData,
      int maxDoc,
      int version,
      boolean merging) {
    this.numerics = numerics;
    this.binaries = binaries;
    this.sorted = sorted;
    this.sortedSets = sortedSets;
    this.sortedNumerics = sortedNumerics;
    this.skippers = skippers;
    this.data = data.clone();
    this.skipIndexData = skipIndexData != null ? skipIndexData.clone() : null;
    this.maxDoc = maxDoc;
    this.version = version;
    this.merging = merging;
  }

  @Override
  public DocValuesProducer getMergeInstance() {
    return new Lucene90DocValuesProducer(
        numerics,
        binaries,
        sorted,
        sortedSets,
        sortedNumerics,
        skippers,
        data,
        skipIndexData,
        maxDoc,
        version,
        true);
  }

  private void inferMaxValueCounts(FieldInfos fieldInfos) {
    for (var cursor : skippers) {
      DocValuesSkipperEntry entry = cursor.value;
      if (entry.maxValueCount == -1 && entry.docCount != 0) {
        int fieldNumber = cursor.key;
        FieldInfo info = fieldInfos.fieldInfo(fieldNumber);
        int inferredMaxValueCount = -1;
        if (info != null) {
          switch (info.getDocValuesType()) {
            case NUMERIC, SORTED -> inferredMaxValueCount = 1;
            case SORTED_NUMERIC -> {
              SortedNumericEntry sne = sortedNumerics.get(fieldNumber);
              if (sne != null && sne.numValues == sne.numDocsWithField) {
                inferredMaxValueCount = 1;
              }
            }
            case SORTED_SET -> {
              SortedSetEntry sse = sortedSets.get(fieldNumber);
              if (sse != null) {
                if (sse.singleValueEntry != null) {
                  inferredMaxValueCount = 1;
                } else if (sse.ordsEntry != null
                    && sse.ordsEntry.numValues == sse.ordsEntry.numDocsWithField) {
                  inferredMaxValueCount = 1;
                }
              }
            }
            // $CASES-OMITTED$
            default -> {
              // leave as -1
            }
          }
        }
        if (inferredMaxValueCount != -1) {
          skippers.put(
              fieldNumber,
              new DocValuesSkipperEntry(
                  entry.offset,
                  entry.length,
                  entry.minValue,
                  entry.maxValue,
                  entry.docCount,
                  entry.maxDocId,
                  inferredMaxValueCount));
        }
      }
    }
  }
  // 在数据节点启动的时候，就会加载元数据dvm，映射 Lucene80DocValuesConsumer.addSortedSetField()
  private void readFields(IndexInput meta, FieldInfos infos) throws IOException {
    for (int fieldNumber = meta.readInt(); fieldNumber != -1; fieldNumber = meta.readInt()) {// 依次读取每个字段
      FieldInfo info = infos.fieldInfo(fieldNumber);
      if (info == null) {
        throw new CorruptIndexException("Invalid field number: " + fieldNumber, meta);
      }// 任何field，都会读取元数据
      byte type = meta.readByte();// 可看Lucene80DocValuesConsumer.addSortedSetField
      if (info.docValuesSkipIndexType() != DocValuesSkipIndexType.NONE) {
        skippers.put(info.number, readDocValueSkipperMeta(meta));// 仅仅读取元数据
      }
      if (type == Lucene90DocValuesFormat.NUMERIC) {
        numerics.put(info.number, readNumeric(meta));
      } else if (type == Lucene90DocValuesFormat.BINARY) {
        binaries.put(info.number, readBinary(meta));
      } else if (type == Lucene90DocValuesFormat.SORTED) {
        sorted.put(info.number, readSorted(meta));
      } else if (type == Lucene90DocValuesFormat.SORTED_SET) {
        sortedSets.put(info.number, readSortedSet(meta));
      } else if (type == Lucene90DocValuesFormat.SORTED_NUMERIC) {
        sortedNumerics.put(info.number, readSortedNumeric(meta));
      } else {
        throw new CorruptIndexException("invalid type: " + type, meta);
      }
    }
  }
  // 这个是读取NUMERIC类型，启动时读取dvm文件
  private NumericEntry readNumeric(IndexInput meta) throws IOException {
    NumericEntry entry = new NumericEntry();
    readNumeric(meta, entry);
    return entry;
  }

  private DocValuesSkipperEntry readDocValueSkipperMeta(IndexInput meta) throws IOException {
    long offset = meta.readLong();// 记录dvd起始位置
    long length = meta.readLong();// 记录长度
    long maxValue = meta.readLong();
    long minValue = meta.readLong();
    int docCount = meta.readInt();
    int maxDocID = meta.readInt();
    final int maxValueCount;
    if (version >= Lucene90DocValuesFormat.VERSION_SKIPPER_MAX_VALUE_COUNT) {
      maxValueCount = meta.readInt();
    } else {
      maxValueCount = docCount == 0 ? 0 : -1;
    }

    return new DocValuesSkipperEntry(
        offset, length, minValue, maxValue, docCount, maxDocID, maxValueCount);
  }
  // 可以看下Lucene80DocValuesConsumer.doAddSortedField()。 // number，SORTED，SORTED_SET，SORTED_NUMERIC类型会进来
  private void readNumeric(IndexInput meta, NumericEntry entry) throws IOException {
    entry.docsWithFieldOffset = meta.readLong();// 若每个文档id都有该字段，那么不用存储docId编号，该值为-1。
    entry.docsWithFieldLength = meta.readLong(); // 若每个文档id都有该字段，那么不用存储docId编号，该值为0。
    entry.jumpTableEntryCount = meta.readShort();// -1
    entry.denseRankPower = meta.readByte(); // -1 写死了的
    entry.numValues = meta.readLong(); // values个数
    int tableSize = meta.readInt();
    if (tableSize > 256) {
      throw new CorruptIndexException("invalid table size: " + tableSize, meta);
    }
    if (tableSize >= 0) {
      entry.table = new long[tableSize];
      for (int i = 0; i < tableSize; ++i) {
        entry.table[i] = meta.readLong();// 独立的词，词典（数字才有的）
      }
    }
    if (tableSize < -1) {
      entry.blockShift = -2 - tableSize;
    } else {
      entry.blockShift = -1;
    }
    entry.bitsPerValue = meta.readByte();
    entry.minValue = meta.readLong();
    entry.gcd = meta.readLong();
    entry.valuesOffset = meta.readLong();//存储的具体的每个termId
    entry.valuesLength = meta.readLong();
    entry.valueJumpTableOffset = meta.readLong();// 一般-1 未压缩
  }
  // 这个是读取BINARY类型，启动时读取dvm文件
  private BinaryEntry readBinary(IndexInput meta) throws IOException {
    final BinaryEntry entry = new BinaryEntry();
    entry.dataOffset = meta.readLong();
    entry.dataLength = meta.readLong();
    entry.docsWithFieldOffset = meta.readLong();//即将写入docId
    entry.docsWithFieldLength = meta.readLong();
    entry.jumpTableEntryCount = meta.readShort();
    entry.denseRankPower = meta.readByte();
    entry.numDocsWithField = meta.readInt();
    entry.minLength = meta.readInt();
    entry.maxLength = meta.readInt();
    if (entry.minLength < entry.maxLength) {
      entry.addressesOffset = meta.readLong();// 指向的每个doc存放的多少value的列

      // Old count of uncompressed addresses
      long numAddresses = entry.numDocsWithField + 1L;

      final int blockShift = meta.readVInt();//// 返回的是16: DIRECT_MONOTONIC_BLOCK_SHIFT
      entry.addressesMeta = DirectMonotonicReader.loadMeta(meta, numAddresses, blockShift);// 可以读取“每个doc存放的多少value的列” 的具体valuecount
      entry.addressesLength = meta.readLong(); //指向“每个doc存放的多少value的列"的总长度
    }
    return entry;
  }
  // 磁盘启动时就加载了,,,// 可看 Lucene80DocValuesConsumer.doAddsortedField()，从dvm中读取，在节点启动的时候就读取，读取 SORTED_SET / SORTED 类型
  private SortedEntry readSorted(IndexInput meta) throws IOException {
    SortedEntry entry = new SortedEntry();
    entry.ordsEntry = new NumericEntry();
    readNumeric(meta, entry.ordsEntry);
    entry.termsDictEntry = new TermsDictEntry();
    readTermDict(meta, entry.termsDictEntry);
    return entry;
  }
  // 参考 Lucene80DocValuesConsumer.addSortedSetField 中间的代码，节点启动时，读取dvm的整个文件
  private SortedSetEntry readSortedSet(IndexInput meta) throws IOException {
    SortedSetEntry entry = new SortedSetEntry();
    byte multiValued = meta.readByte();// dvm  应该是order
    switch (multiValued) {// 每个文档只有一个词，一般都会跑到这里
      case 0: // singlevalued    每个文档都有个该值
        entry.singleValueEntry = readSorted(meta);// 一般要进来，每个文档的该域只有只有一个value
        return entry;
      case 1: // multivalued
        break;
      default:
        throw new CorruptIndexException("Invalid multiValued flag: " + multiValued, meta);
    }
    entry.ordsEntry = new SortedNumericEntry();
    readSortedNumeric(meta, entry.ordsEntry);//
    entry.termsDictEntry = new TermsDictEntry();
    readTermDict(meta, entry.termsDictEntry);
    return entry;
  }

  private static void readTermDict(IndexInput meta, TermsDictEntry entry) throws IOException {
    entry.termsDictSize = meta.readVLong();// 多少个独立的词（相同词算一个）
    final int blockShift = meta.readInt();//16， 二级索引
    final long addressesSize =//16 ，存储的时候是个二维byte[]，一维存储大小为32kb=1<<16
        (entry.termsDictSize + (1L << TERMS_DICT_BLOCK_LZ4_SHIFT) - 1)// 一级索引多少个节点
            >>> TERMS_DICT_BLOCK_LZ4_SHIFT;
    entry.termsAddressesMeta = DirectMonotonicReader.loadMeta(meta, addressesSize, blockShift);// 从dvm加载一级索引（每16个词在dvd中存放起始位置）的元数据
    entry.maxTermLength = meta.readInt();// 最长的那个词长度
    entry.maxBlockLength = meta.readInt();
    entry.termsDataOffset = meta.readLong();// 向dvd中开始写terms的原始值（每个词的相同前缀长度及后缀）的起始位置
    entry.termsDataLength = meta.readLong(); // dvd中所有value的长度
    entry.termsAddressesOffset = meta.readLong();// 开始向dvd写一级索引（每16个词的在dvd中存放）的起始位置
    entry.termsAddressesLength = meta.readLong();// dvd中一级索引的长度
    entry.termsDictIndexShift = meta.readInt();// 10， 二级索引区间1<<10=1024    （开始二级索引信息）// 二级索引由两部分构成，一部分是第1024*x个词的相同前缀内容，第二部分是第1024*x个词和第1024*x-1个词的相同前缀累加值（数组会放在dvm和dvd中）
    final long indexSize =
        (entry.termsDictSize + (1L << entry.termsDictIndexShift) - 1) >>> entry.termsDictIndexShift;//二级索引多少个节点
    entry.termsIndexAddressesMeta = DirectMonotonicReader.loadMeta(meta, 1 + indexSize, blockShift);// 从dvm中加载二级索引第二部分（第1024*x个词和第1024*x-1个词的相同前缀累加值）的元数据部分
    entry.termsIndexOffset = meta.readLong();// 在dvd中存放的第二级索引第一部分（第1024*x个词的相同前缀内容）存放的起始位置
    entry.termsIndexLength = meta.readLong();// 在dvd中存放的第二级索引第一部分（第1024*x个词的相同前缀内容）存放的长度
    entry.termsIndexAddressesOffset = meta.readLong();// 从dvd中加载二级索引第二部分（第1024*x个词和第1024*x-1个词的相同前缀累加值）的数据部分
    entry.termsIndexAddressesLength = meta.readLong();// 从dvd中加载二级索引第二部分（第1024*x个词和第1024*x-1个词的相同前缀累加值）的数据部分长度
  }// profix1 value, profix2 value,, profix2 value,
  // 这个是读取SORTED_NUMBER类型， 见Lucene80DocValuesConsumer.addSortedNumericField
  private SortedNumericEntry readSortedNumeric(IndexInput meta) throws IOException {
    SortedNumericEntry entry = new SortedNumericEntry();
    readSortedNumeric(meta, entry);
    return entry;
  }

  private SortedNumericEntry readSortedNumeric(IndexInput meta, SortedNumericEntry entry)
      throws IOException {
    readNumeric(meta, entry);
    entry.numDocsWithField = meta.readInt();// 多少个文档有这个词
    if (entry.numDocsWithField != entry.numValues) {// 有的文档不知一个value
      entry.addressesOffset = meta.readLong();// 指向的每个doc存放的多少value的列
      final int blockShift = meta.readVInt();// 返回的是16: DIRECT_MONOTONIC_BLOCK_SHIFT
      entry.addressesMeta =// 可以读取“每个doc存放的多少value的列” 的具体valuecount
          DirectMonotonicReader.loadMeta(meta, entry.numDocsWithField + 1, blockShift);
      entry.addressesLength = meta.readLong();
    }
    return entry;
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(data, skipIndexData);
  }

  private record DocValuesSkipperEntry(
      long offset,
      long length,
      long minValue,
      long maxValue,
      int docCount,
      int maxDocId,
      int maxValueCount) {}

  // Cached DocValuesRangeSupport instance to avoid repeated stack walks in ensureCaller()
  private static final org.apache.lucene.internal.vectorization.DocValuesRangeSupport
      DOC_VALUES_RANGE_SUPPORT =
          org.apache.lucene.internal.vectorization.VectorizationProvider.getInstance()
              .getDocValuesRangeSupport();
  private static final org.apache.lucene.internal.vectorization.DocValuesBulkDecodeSupport
      DOC_VALUES_BULK_DECODE_SUPPORT =
          org.apache.lucene.internal.vectorization.VectorizationProvider.getInstance()
              .getDocValuesBulkDecodeSupport();

  static void rangeIntoBitSet(
      org.apache.lucene.util.LongValues values,
      int fromDoc,
      int toDoc,
      long minValue,
      long maxValue,
      org.apache.lucene.util.FixedBitSet bitSet,
      int offset) {
    DOC_VALUES_RANGE_SUPPORT.rangeIntoBitSet(
        values, fromDoc, toDoc, minValue, maxValue, bitSet, offset);
  }

  private static int fixedCardinality(
      SortedNumericEntry entry, DocValuesSkipperEntry skipperEntry) {
    if (skipperEntry == null
        || skipperEntry.maxValueCount <= 1
        || entry.numDocsWithField == 0
        || entry.numValues % entry.numDocsWithField != 0) {
      return -1;
    }
    long cardinality = entry.numValues / entry.numDocsWithField;
    if (cardinality > Integer.MAX_VALUE || cardinality != skipperEntry.maxValueCount) {
      return -1;
    }
    return (int) cardinality;
  }

  private static void sortedNumericScalarRangeIntoBitSet(
      LongValues values,
      int fromDoc,
      int toDoc,
      int cardinality,
      long minValue,
      long maxValue,
      FixedBitSet bitSet,
      int offset) {
    for (int doc = fromDoc; doc < toDoc; doc++) {
      long valueOffset = (long) doc * cardinality;
      for (int i = 0; i < cardinality; i++) {
        long value = values.get(valueOffset + i);
        if (value >= minValue) {
          if (value <= maxValue) {
            bitSet.set(doc - offset);
          }
          break;
        }
      }
    }
  }

  private static boolean sortedNumericMatchesRange(
      LongValues values, long start, long end, long minValue, long maxValue) {
    for (long valueOffset = start; valueOffset < end; valueOffset++) {
      long value = values.get(valueOffset);
      if (value >= minValue) {
        return value <= maxValue;
      }
    }
    return false;
  }

  private static boolean canBulkDecodeByteAligned(NumericEntry entry) {
    return entry.blockShift < 0 && entry.bitsPerValue > 0 && (entry.bitsPerValue & 0x07) == 0;
  }

  private static boolean isContiguous(int size, int[] docs, int docsOffset) {
    return size == 0 || docs[docsOffset + size - 1] - docs[docsOffset] == size - 1;
  }

  private static int paddingBytesNeededForBulkDecode(int bitsPerValue) {
    if (bitsPerValue == 24) {
      return 1;
    } else if (bitsPerValue == 40 || bitsPerValue == 48 || bitsPerValue == 56) {
      return Long.BYTES - bitsPerValue / Byte.SIZE;
    }
    return 0;
  }

  private static byte[] bulkDecodeByteAlignedValues(
      RandomAccessInput slice,
      NumericEntry entry,
      int size,
      int[] docs,
      int docsOffset,
      long[] values,
      int valuesOffset,
      byte[] bytes)
      throws IOException {
    if (canBulkDecodeByteAligned(entry) == false || isContiguous(size, docs, docsOffset) == false) {
      return null;
    }

    final int bytesPerValue = entry.bitsPerValue / Byte.SIZE;
    final long byteCountLong = (long) size * bytesPerValue;
    if (byteCountLong > Integer.MAX_VALUE) {
      return null;
    }
    final int byteCount = (int) byteCountLong;
    final int readByteCount =
        byteCount == 0 ? 0 : byteCount + paddingBytesNeededForBulkDecode(entry.bitsPerValue);
    final long offset = byteCount == 0 ? 0 : (long) docs[docsOffset] * bytesPerValue;
    if (offset + readByteCount > slice.length()) {
      return null;
    }
    if (bytes.length < readByteCount) {
      bytes = new byte[readByteCount];
    }
    if (byteCount != 0) {
      slice.readBytes(offset, bytes, 0, readByteCount);
      DOC_VALUES_BULK_DECODE_SUPPORT.decodeByteAligned(
          bytes, 0, entry.bitsPerValue, values, valuesOffset, size);
    }
    return bytes;
  }

  private static void applyTable(long[] values, int valuesOffset, long[] table, int size) {
    for (int i = valuesOffset, end = valuesOffset + size; i < end; i++) {
      values[i] = table[(int) values[i]];
    }
  }

  private static void applyGcdDelta(
      long[] values, int valuesOffset, long mul, long delta, int size) {
    for (int i = valuesOffset, end = valuesOffset + size; i < end; i++) {
      values[i] = mul * values[i] + delta;
    }
  }

  private static class NumericEntry {
    long[] table;// 数字才有的，独立的数字gesso
    int blockShift; // 
    byte bitsPerValue;// 若=0，表示每个文档该字段的值都一样。是会进来的
    long docsWithFieldOffset;// 若每个文档id都有该字段，那么不用存储docId编号，该值为-1。
    long docsWithFieldLength;
    short jumpTableEntryCount;
    byte denseRankPower;
    long numValues;// // 有多个value(重复的也算多个)
    long minValue;
    long gcd;
    long valuesOffset; //存储的具体的每个termId
    long valuesLength;
    long valueJumpTableOffset; // -1 if no jump-table
  }

  private static class BinaryEntry {
    long dataOffset;
    long dataLength;
    long docsWithFieldOffset;
    long docsWithFieldLength;
    short jumpTableEntryCount;
    byte denseRankPower;
    int numDocsWithField;
    int minLength;
    int maxLength;
    long addressesOffset; // 指向的每个doc存放的多少value的列
    long addressesLength;//指向“每个doc存放的多少value的列"的总长度
    DirectMonotonicReader.Meta addressesMeta; // 可以读取“每个doc存放的多少value的列”
  }

  private static class TermsDictEntry {
    long termsDictSize;// 多少个独立的词（相同词算一个）
    DirectMonotonicReader.Meta termsAddressesMeta;
    int maxTermLength;
    long termsDataOffset; // 词典部分
    long termsDataLength;
    long termsAddressesOffset; // 和termsDataOffset区别
    long termsAddressesLength;
    int termsDictIndexShift;
    DirectMonotonicReader.Meta termsIndexAddressesMeta;
    long termsIndexOffset;// 加载二级索引第一部分的
    long termsIndexLength;
    long termsIndexAddressesOffset; // 加载二级索引第二部分
    long termsIndexAddressesLength;

    int maxBlockLength;
  }

  private static class SortedEntry {
    NumericEntry ordsEntry;
    TermsDictEntry termsDictEntry;
  }
  // 最牛逼的数据结构，直接拥有SortedNumericEntry
  private static class SortedSetEntry {
    SortedEntry singleValueEntry;
    SortedNumericEntry ordsEntry;// ordsEntry和singleValueEntry只能是二选一。一个词的时候就只能是singleValueEntry
    TermsDictEntry termsDictEntry;  //
  }

  private static class SortedNumericEntry extends NumericEntry {
    int numDocsWithField;
    DirectMonotonicReader.Meta addressesMeta; //// 可以读取“每个doc存放的多少value的列” 的具体valuecount
    long addressesOffset;/// 指向的每个doc存放的多少value的列
    long addressesLength; // 记录doc包含的term个数的总长度
  }

  @Override
  public NumericDocValues getNumeric(FieldInfo field) throws IOException {
    NumericEntry entry = numerics.get(field.number);
    return getNumeric(entry);
  }

  private abstract static class DenseNumericDocValues extends NumericDocValues {//表示每个doc都只有一个value

    final int maxDoc;
    int doc = -1;

    DenseNumericDocValues(int maxDoc) {
      this.maxDoc = maxDoc;
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() throws IOException {
      return advance(doc + 1);
    }

    @Override
    public int advance(int target) throws IOException {
      if (target >= maxDoc) {
        return doc = NO_MORE_DOCS;
      }
      return doc = target;
    }

    @Override
    public boolean advanceExact(int target) {
      doc = target;
      return true;
    }

    @Override
    public long cost() {
      return maxDoc;
    }

    @Override
    public int docIDRunEnd() throws IOException {
      return maxDoc;
    }

    @Override
    public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
      assert offset <= doc;
      upTo = Math.min(upTo, maxDoc);
      if (upTo > doc) {
        bitSet.set(doc - offset, upTo - offset);
        advance(upTo);
      }
    }
  }

  private abstract static class SparseNumericDocValues extends NumericDocValues {

    final IndexedDISI disi;

    SparseNumericDocValues(IndexedDISI disi) {
      this.disi = disi;
    }

    @Override
    public int advance(int target) throws IOException {
      return disi.advance(target);
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
      return disi.advanceExact(target);
    }

    @Override
    public int nextDoc() throws IOException {
      return disi.nextDoc();
    }

    @Override
    public int docID() {
      return disi.docID();
    }

    @Override
    public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
      disi.intoBitSet(upTo, bitSet, offset);
    }

    @Override
    public long cost() {
      return disi.cost();
    }

    @Override
    public int docIDRunEnd() throws IOException {
      return disi.docIDRunEnd();
    }
  }

  private LongValues getDirectReaderInstance(
      RandomAccessInput slice, int bitsPerValue, long offset, long numValues) {
    if (merging) {
      return DirectReader.getMergeInstance(slice, bitsPerValue, offset, numValues);
    } else {
      return DirectReader.getInstance(slice, bitsPerValue, offset);
    }
  }
  //
  private NumericDocValues getNumeric(NumericEntry entry) throws IOException {// 这个是数值型的
    if (entry.docsWithFieldOffset == -2) {
      // empty
      return DocValues.emptyNumeric();
    } else if (entry.docsWithFieldOffset == -1) {// 每个doc都只有一个value
      // dense
      if (entry.bitsPerValue == 0) {// value是统一的值
        return new DenseNumericDocValues(maxDoc) {
          @Override
          public long longValue() throws IOException {
            return entry.minValue;
          }

          @Override
          public void longValues(
              int size,
              int[] docs,
              int docsOffset,
              long[] values,
              int valuesOffset,
              long defaultValue)
              throws IOException {
            Arrays.fill(values, valuesOffset, valuesOffset + size, entry.minValue);
            if (size != 0) {
              doc = docs[docsOffset + size - 1];
            }
          }
        };
      } else {
        final RandomAccessInput slice = //存储的具体的每个termId
            data.randomAccessSlice(entry.valuesOffset, entry.valuesLength);
        // Prefetch the first page of data. Following pages are expected to get prefetched through
        // read-ahead.
        if (slice.length() > 0) {
          slice.prefetch(0, 1);// 尝试读取16k
        }
        if (entry.blockShift >= 0) {
          // dense but split into blocks of different bits per value
          return new DenseNumericDocValues(maxDoc) {
            final VaryingBPVReader vBPVReader = new VaryingBPVReader(entry, slice);

            @Override
            public long longValue() throws IOException {
              return vBPVReader.getLongValue(doc);
            }

            @Override
            public void longValues(
                int size,
                int[] docs,
                int docsOffset,
                long[] values,
                int valuesOffset,
                long defaultValue)
                throws IOException {
              // Delegate to help performance: when the super call inlines, calls to
              // #advanceExact/#longValue become monomorphic.
              super.longValues(size, docs, docsOffset, values, valuesOffset, defaultValue);
            }
          };
        } else {
          final LongValues values =
              getDirectReaderInstance(slice, entry.bitsPerValue, 0L, entry.numValues);
          if (entry.table != null) {
            final long[] table = entry.table;
            return new DenseNumericDocValues(maxDoc) {
              private byte[] bulkBytes = new byte[0];

              @Override
              public long longValue() throws IOException {
                return table[(int) values.get(doc)];
              }

              @Override
              public void longValues(
                  int size,
                  int[] docs,
                  int docsOffset,
                  long[] values,
                  int valuesOffset,
                  long defaultValue)
                  throws IOException {
                byte[] bytes =
                    bulkDecodeByteAlignedValues(
                        slice, entry, size, docs, docsOffset, values, valuesOffset, bulkBytes);
                if (bytes == null) {
                  super.longValues(size, docs, docsOffset, values, valuesOffset, defaultValue);
                } else {
                  applyTable(values, valuesOffset, table, size);
                  bulkBytes = bytes;
                  if (size != 0) {
                    doc = docs[docsOffset + size - 1];
                  }
                }
              }
            };
          } else if (entry.gcd == 1 && entry.minValue == 0) {
            // Common case for ordinals, which are encoded as numerics
            return new DenseNumericDocValues(maxDoc) {
              private byte[] bulkBytes = new byte[0];

              @Override
              public long longValue() throws IOException {
                return values.get(doc);// 这里会去直接获取，可以直接按照8 bit读取
              }

              @Override
              public void longValues(
                  int size,
                  int[] docs,
                  int docsOffset,
                  long[] values,
                  int valuesOffset,
                  long defaultValue)
                  throws IOException {
                byte[] bytes =
                    bulkDecodeByteAlignedValues(
                        slice, entry, size, docs, docsOffset, values, valuesOffset, bulkBytes);
                if (bytes == null) {
                  super.longValues(size, docs, docsOffset, values, valuesOffset, defaultValue);
                } else {
                  bulkBytes = bytes;
                  if (size != 0) {
                    doc = docs[docsOffset + size - 1];
                  }
                }
              }

              @Override
              public void rangeIntoBitSet(
                  int fromDoc,
                  int toDoc,
                  long minValue,
                  long maxValue,
                  org.apache.lucene.util.FixedBitSet bitSet,
                  int offset) {
                // Bulk range evaluation via DocValuesRangeSupport
                Lucene90DocValuesProducer.rangeIntoBitSet(
                    values, fromDoc, toDoc, minValue, maxValue, bitSet, offset);
              }
            };
          } else {
            final long mul = entry.gcd;
            final long delta = entry.minValue;
            return new DenseNumericDocValues(maxDoc) {// 数值型的
              private byte[] bulkBytes = new byte[0];

              @Override
              public long longValue() throws IOException {
                return mul * values.get(doc) + delta;
              }

              @Override
              public void longValues(
                  int size,
                  int[] docs,
                  int docsOffset,
                  long[] values,
                  int valuesOffset,
                  long defaultValue)
                  throws IOException {
                byte[] bytes =
                    bulkDecodeByteAlignedValues(
                        slice, entry, size, docs, docsOffset, values, valuesOffset, bulkBytes);
                if (bytes == null) {
                  super.longValues(size, docs, docsOffset, values, valuesOffset, defaultValue);
                } else {
                  applyGcdDelta(values, valuesOffset, mul, delta, size);
                  bulkBytes = bytes;
                  if (size != 0) {
                    doc = docs[docsOffset + size - 1];
                  }
                }
              }

              @Override
              public void rangeIntoBitSet(
                  int fromDoc,
                  int toDoc,
                  long minValue,
                  long maxValue,
                  org.apache.lucene.util.FixedBitSet bitSet,
                  int offset) {
                // Per-doc evaluation for gcd/delta encoded fields
                for (int d = fromDoc; d < toDoc; d++) {
                  long v = mul * values.get(d) + delta;
                  if (v >= minValue && v <= maxValue) {
                    bitSet.set(d - offset);
                  }
                }
              }
            };
          }
        }
      }
    } else {// 有的文档的value不止一个
      // sparse
      final IndexedDISI disi =
          new IndexedDISI(
              data,
              entry.docsWithFieldOffset,
              entry.docsWithFieldLength,
              entry.jumpTableEntryCount,
              entry.denseRankPower,
              entry.numValues);
      if (entry.bitsPerValue == 0) {// 表示每个文档该字段的值都一样
        return new SparseNumericDocValues(disi) {
          @Override
          public long longValue() throws IOException {
            return entry.minValue;
          }
        };
      } else {
        final RandomAccessInput slice = //存储的具体的每个termId
            data.randomAccessSlice(entry.valuesOffset, entry.valuesLength);
        // Prefetch the first page of data. Following pages are expected to get prefetched through
        // read-ahead.
        if (slice.length() > 0) {
          slice.prefetch(0, 1);
        }
        if (entry.blockShift >= 0) {
          // sparse and split into blocks of different bits per value
          return new SparseNumericDocValues(disi) {
            final VaryingBPVReader vBPVReader = new VaryingBPVReader(entry, slice);

            @Override
            public long longValue() throws IOException {
              final int index = disi.index();
              return vBPVReader.getLongValue(index);
            }
          };
        } else {
          final LongValues values =
              getDirectReaderInstance(slice, entry.bitsPerValue, 0L, entry.numValues);
          if (entry.table != null) {
            final long[] table = entry.table;
            return new SparseNumericDocValues(disi) {
              @Override
              public long longValue() throws IOException {
                return table[(int) values.get(disi.index())];
              }
            };
          } else if (entry.gcd == 1 && entry.minValue == 0) {
            return new SparseNumericDocValues(disi) {
              @Override
              public long longValue() throws IOException {
                return values.get(disi.index());
              }
            };
          } else {
            final long mul = entry.gcd;
            final long delta = entry.minValue;
            return new SparseNumericDocValues(disi) {
              @Override
              public long longValue() throws IOException {
                return mul * values.get(disi.index()) + delta;
              }
            };
          }
        }
      }
    }
  }

  private LongValues getNumericValues(NumericEntry entry) throws IOException {
    if (entry.bitsPerValue == 0) {
      return new LongValues() {
        @Override
        public long get(long index) {
          return entry.minValue;
        }
      };
    } else {
      final RandomAccessInput slice = //存储的具体的每个termId
          data.randomAccessSlice(entry.valuesOffset, entry.valuesLength);
      // Prefetch the first page of data. Following pages are expected to get prefetched through
      // read-ahead.
      if (slice.length() > 0) {
        slice.prefetch(0, 1);
      }
      if (entry.blockShift >= 0) {
        return new LongValues() {
          final VaryingBPVReader vBPVReader = new VaryingBPVReader(entry, slice);

          @Override
          public long get(long index) {
            try {
              return vBPVReader.getLongValue(index);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        };
      } else {
        final LongValues values =
            getDirectReaderInstance(slice, entry.bitsPerValue, 0L, entry.numValues);
        if (entry.table != null) {
          final long[] table = entry.table;
          return new LongValues() {
            @Override
            public long get(long index) {
              return table[(int) values.get(index)];
            }
          };
        } else if (entry.gcd != 1) {
          final long gcd = entry.gcd;
          final long minValue = entry.minValue;
          return new LongValues() {
            @Override
            public long get(long index) {
              return values.get(index) * gcd + minValue;
            }
          };
        } else if (entry.minValue != 0) {
          final long minValue = entry.minValue;
          return new LongValues() {
            @Override
            public long get(long index) {
              return values.get(index) + minValue;
            }
          };
        } else {
          return values;
        }
      }
    }
  }

  private abstract static class DenseBinaryDocValues extends BinaryDocValues {

    final int maxDoc;
    int doc = -1;

    DenseBinaryDocValues(int maxDoc) {
      this.maxDoc = maxDoc;
    }

    @Override
    public int nextDoc() throws IOException {
      return advance(doc + 1);
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public long cost() {
      return maxDoc;
    }

    @Override
    public int advance(int target) throws IOException {
      if (target >= maxDoc) {
        return doc = NO_MORE_DOCS;
      }
      return doc = target;
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
      doc = target;
      return true;
    }

    @Override
    public int docIDRunEnd() throws IOException {
      return maxDoc;
    }

    @Override
    public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
      assert offset <= doc;
      upTo = Math.min(upTo, maxDoc);
      if (upTo > doc) {
        bitSet.set(doc - offset, upTo - offset);
        advance(upTo);
      }
    }
  }

  private abstract static class SparseBinaryDocValues extends BinaryDocValues {

    final IndexedDISI disi;

    SparseBinaryDocValues(IndexedDISI disi) {
      this.disi = disi;
    }

    @Override
    public int nextDoc() throws IOException {
      return disi.nextDoc();
    }

    @Override
    public int docID() {
      return disi.docID();
    }

    @Override
    public long cost() {
      return disi.cost();
    }

    @Override
    public int advance(int target) throws IOException {
      return disi.advance(target);
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
      return disi.advanceExact(target);
    }

    @Override
    public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
      disi.intoBitSet(upTo, bitSet, offset);
    }

    @Override
    public int docIDRunEnd() throws IOException {
      return disi.docIDRunEnd();
    }
  }

  @Override
  public BinaryDocValues getBinary(FieldInfo field) throws IOException {
    BinaryEntry entry = binaries.get(field.number);

    if (entry.docsWithFieldOffset == -2) {
      return DocValues.emptyBinary();
    }

    final RandomAccessInput bytesSlice = data.randomAccessSlice(entry.dataOffset, entry.dataLength);
    // Prefetch the first page of data. Following pages are expected to get prefetched through
    // read-ahead.
    if (bytesSlice.length() > 0) {
      bytesSlice.prefetch(0, 1);
    }

    if (entry.docsWithFieldOffset == -1) {
      // dense
      if (entry.minLength == entry.maxLength) {
        // fixed length
        final int length = entry.maxLength;
        return new DenseBinaryDocValues(maxDoc) {
          final BytesRef bytes = new BytesRef(new byte[length], 0, length);

          @Override
          public BytesRef binaryValue() throws IOException {
            bytesSlice.readBytes((long) doc * length, bytes.bytes, 0, length);
            return bytes;
          }
        };
      } else {
        // variable length
        final RandomAccessInput addressesData =// 可以读取“每个doc存放的多少value的列” 的具体valuecount
            this.data.randomAccessSlice(entry.addressesOffset, entry.addressesLength);
        // Prefetch the first page of data. Following pages are expected to get prefetched through
        // read-ahead.
        if (addressesData.length() > 0) {
          addressesData.prefetch(0, 1);
        }
        final LongValues addresses =
            DirectMonotonicReader.getInstance(entry.addressesMeta, addressesData, merging);
        return new DenseBinaryDocValues(maxDoc) {
          final BytesRef bytes = new BytesRef(new byte[entry.maxLength], 0, entry.maxLength);

          @Override
          public BytesRef binaryValue() throws IOException {
            long startOffset = addresses.get(doc);
            bytes.length = (int) (addresses.get(doc + 1L) - startOffset);
            bytesSlice.readBytes(startOffset, bytes.bytes, 0, bytes.length);
            return bytes;
          }
        };
      }
    } else {
      // sparse
      final IndexedDISI disi =
          new IndexedDISI(
              data,
              entry.docsWithFieldOffset,
              entry.docsWithFieldLength,
              entry.jumpTableEntryCount,
              entry.denseRankPower,
              entry.numDocsWithField);
      if (entry.minLength == entry.maxLength) {
        // fixed length
        final int length = entry.maxLength;
        return new SparseBinaryDocValues(disi) {
          final BytesRef bytes = new BytesRef(new byte[length], 0, length);

          @Override
          public BytesRef binaryValue() throws IOException {
            bytesSlice.readBytes((long) disi.index() * length, bytes.bytes, 0, length);
            return bytes;
          }
        };
      } else {
        // variable length
        final RandomAccessInput addressesData =// 可以读取“每个doc存放的多少value的列” 的具体valuecount
            this.data.randomAccessSlice(entry.addressesOffset, entry.addressesLength);
        // Prefetch the first page of data. Following pages are expected to get prefetched through
        // read-ahead.
        if (addressesData.length() > 0) {
          addressesData.prefetch(0, 1);
        }
        final LongValues addresses =
            DirectMonotonicReader.getInstance(entry.addressesMeta, addressesData);
        return new SparseBinaryDocValues(disi) {
          final BytesRef bytes = new BytesRef(new byte[entry.maxLength], 0, entry.maxLength);

          @Override
          public BytesRef binaryValue() throws IOException {
            final int index = disi.index();
            long startOffset = addresses.get(index);
            bytes.length = (int) (addresses.get(index + 1L) - startOffset);
            bytesSlice.readBytes(startOffset, bytes.bytes, 0, bytes.length);
            return bytes;
          }
        };
      }
    }
  }

  @Override
  public SortedDocValues getSorted(FieldInfo field) throws IOException {
    SortedEntry entry = sorted.get(field.number);
    return getSorted(entry);
  }

  private SortedDocValues getSorted(SortedEntry entry) throws IOException {
    // Specialize the common case for ordinals: single block of packed integers.
    final NumericEntry ordsEntry = entry.ordsEntry;
    if (ordsEntry.blockShift < 0 // single block
        && ordsEntry.bitsPerValue > 0) { // more than 1 value

      if (ordsEntry.gcd != 1 || ordsEntry.minValue != 0 || ordsEntry.table != null) {
        throw new IllegalStateException("Ordinals shouldn't use GCD, offset or table compression");
      }

      final RandomAccessInput slice = //存储的具体的每个termId
          data.randomAccessSlice(ordsEntry.valuesOffset, ordsEntry.valuesLength);
      // Prefetch the first page of data. Following pages are expected to get prefetched through
      // read-ahead.
      if (slice.length() > 0) {
        slice.prefetch(0, 1);
      }
      final LongValues values =
          getDirectReaderInstance(slice, ordsEntry.bitsPerValue, 0L, ordsEntry.numValues);

      if (ordsEntry.docsWithFieldOffset == -1) { // dense， // 每个文档id都有该字段，那么不用存储docId编号，该值为-1。
        return new BaseSortedDocValues(entry) {

          private final int maxDoc = Lucene90DocValuesProducer.this.maxDoc;
          private int doc = -1;

          @Override
          public int ordValue() throws IOException {
            return (int) values.get(doc);// 存储起来很神奇，就是直接按照 bitsPerValue存储的
          }

          @Override
          public boolean advanceExact(int target) throws IOException {
            doc = target;// 可以直接进来
            return true;
          }

          @Override
          public int docID() {
            return doc;
          }

          @Override
          public int nextDoc() throws IOException {
            return advance(doc + 1);
          }

          @Override
          public int advance(int target) throws IOException {
            if (target >= maxDoc) {
              return doc = NO_MORE_DOCS;
            }
            return doc = target;
          }

          @Override
          public long cost() {
            return maxDoc;
          }

          @Override
          public int docIDRunEnd() throws IOException {
            return maxDoc;
          }

          @Override
          public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
            assert offset <= doc;
            upTo = Math.min(upTo, maxDoc);
            if (upTo > doc) {
              bitSet.set(doc - offset, upTo - offset);
              advance(upTo);
            }
          }
        };
      } else if (ordsEntry.docsWithFieldOffset >= 0) { // sparse but non-empty
        final IndexedDISI disi =
            new IndexedDISI(
                data,
                ordsEntry.docsWithFieldOffset,
                ordsEntry.docsWithFieldLength,
                ordsEntry.jumpTableEntryCount,
                ordsEntry.denseRankPower,
                ordsEntry.numValues);

        return new BaseSortedDocValues(entry) {

          @Override
          public int ordValue() throws IOException {
            return (int) values.get(disi.index());
          }

          @Override
          public boolean advanceExact(int target) throws IOException {
            return disi.advanceExact(target);// 并不是每个doc都包含这个文档，需要根据doc稠密矩阵判断doc是否存在
          }

          @Override
          public int docID() {
            return disi.docID();
          }

          @Override
          public int nextDoc() throws IOException {
            return disi.nextDoc();
          }

          @Override
          public int advance(int target) throws IOException {
            return disi.advance(target);
          }

          @Override
          public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
            disi.intoBitSet(upTo, bitSet, offset);
          }

          @Override
          public long cost() {
            return disi.cost();
          }

          @Override
          public int docIDRunEnd() throws IOException {
            return disi.docIDRunEnd();
          }
        };
      }
    }

    final NumericDocValues ords = getNumeric(entry.ordsEntry);
    return new BaseSortedDocValues(entry) {

      @Override
      public int ordValue() throws IOException {
        return (int) ords.longValue();
      }

      @Override
      public boolean advanceExact(int target) throws IOException {
        return ords.advanceExact(target);
      }

      @Override
      public int docID() {
        return ords.docID();
      }

      @Override
      public int nextDoc() throws IOException {
        return ords.nextDoc();
      }

      @Override
      public int advance(int target) throws IOException {
        return ords.advance(target);
      }

      @Override
      public long cost() {
        return ords.cost();
      }

      @Override
      public int docIDRunEnd() throws IOException {
        return ords.docIDRunEnd();
      }

      @Override
      public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
        ords.intoBitSet(upTo, bitSet, offset);
      }
    };
  }

  private abstract class BaseSortedDocValues extends SortedDocValues {

    final SortedEntry entry;
    final TermsEnum termsEnum;// 就是 TermDict

    BaseSortedDocValues(SortedEntry entry) throws IOException {
      this.entry = entry;
      this.termsEnum = termsEnum();
    }

    @Override
    public int getValueCount() {
      return Math.toIntExact(entry.termsDictEntry.termsDictSize);
    }

    @Override
    public BytesRef lookupOrd(int ord) throws IOException {
      termsEnum.seekExact(ord);// 根据order的话，直接是一级索引查找
      return termsEnum.term();
    }

    @Override
    public int lookupTerm(BytesRef key) throws IOException {
      SeekStatus status = termsEnum.seekCeil(key);
      switch (status) {
        case FOUND:
          return Math.toIntExact(termsEnum.ord());
        case NOT_FOUND:
        case END:
        default:
          return Math.toIntExact(-1L - termsEnum.ord());
      }
    }

    @Override
    public TermsEnum termsEnum() throws IOException {
      return new TermsDict(entry.termsDictEntry, data);// 词典部分
    }
  }

  private abstract class BaseSortedSetDocValues extends SortedSetDocValues {

    final SortedSetEntry entry;
    final IndexInput data;
    final TermsEnum termsEnum;

    BaseSortedSetDocValues(SortedSetEntry entry, IndexInput data) throws IOException {
      this.entry = entry;
      this.data = data;
      this.termsEnum = termsEnum();
    }

    @Override
    public long getValueCount() {
      return entry.termsDictEntry.termsDictSize;
    }

    @Override
    public BytesRef lookupOrd(long ord) throws IOException {
      termsEnum.seekExact(ord);
      return termsEnum.term();
    }

    @Override
    public long lookupTerm(BytesRef key) throws IOException {
      SeekStatus status = termsEnum.seekCeil(key);
      switch (status) {
        case FOUND:
          return termsEnum.ord();
        case NOT_FOUND:
        case END:
        default:
          return -1L - termsEnum.ord();
      }
    }

    @Override
    public TermsEnum termsEnum() throws IOException {
      return new TermsDict(entry.termsDictEntry, data);
    }
  }

  private class TermsDict extends BaseTermsEnum {
    static final int LZ4_DECOMPRESSOR_PADDING = 7;

    final TermsDictEntry entry;
    final LongValues blockAddresses;
    final IndexInput bytes;// 完全是词典第一部分正词部分的存储
    final long blockMask;// 64个词为一个block，会压缩存储
    final LongValues indexAddresses;
    final RandomAccessInput indexBytes;
    final BytesRef term;
    final BytesRef blockBuffer;
    final ByteArrayDataInput blockInput;
    long ord = -1;
    long currentCompressedBlockStart = -1;
    long currentCompressedBlockEnd = -1;

    TermsDict(TermsDictEntry entry, IndexInput data) throws IOException {
      this.entry = entry;
      RandomAccessInput addressesSlice =
          data.randomAccessSlice(entry.termsAddressesOffset, entry.termsAddressesLength);
      blockAddresses =
          DirectMonotonicReader.getInstance(entry.termsAddressesMeta, addressesSlice, merging);
      bytes = data.slice("terms", entry.termsDataOffset, entry.termsDataLength);
      blockMask = (1L << TERMS_DICT_BLOCK_LZ4_SHIFT) - 1;//63
      RandomAccessInput indexAddressesSlice =
          data.randomAccessSlice(entry.termsIndexAddressesOffset, entry.termsIndexAddressesLength);
      indexAddresses =
          DirectMonotonicReader.getInstance(
              entry.termsIndexAddressesMeta, indexAddressesSlice, merging);
      indexBytes = data.randomAccessSlice(entry.termsIndexOffset, entry.termsIndexLength);
      term = new BytesRef(entry.maxTermLength);

      // add the max term length for the dictionary
      // add 7 padding bytes can help decompression run faster.
      int bufferSize = entry.maxBlockLength + entry.maxTermLength + LZ4_DECOMPRESSOR_PADDING;
      blockBuffer = new BytesRef(new byte[bufferSize], 0, bufferSize);
      blockInput = new ByteArrayDataInput();
    }

    @Override
    public BytesRef next() throws IOException {
      if (++ord >= entry.termsDictSize) {// 词典总词的个数
        return null;
      }

      if ((ord & blockMask) == 0L) {// 开始解压这个block
        decompressBlock();
      } else {// 按顺序读取的
        DataInput input = blockInput;
        final int token = Byte.toUnsignedInt(input.readByte());
        int prefixLength = token & 0x0F;// 先读取前缀长度
        int suffixLength = 1 + (token >>> 4);
        if (prefixLength == 15) {
          prefixLength += input.readVInt();// 前缀长度
        }
        if (suffixLength == 16) {
          suffixLength += input.readVInt();
        }
        term.length = prefixLength + suffixLength;
        input.readBytes(term.bytes, prefixLength, suffixLength);// 读取后缀长度
      }
      return term;
    }

    @Override
    public void seekExact(long ord) throws IOException {// 会在词典一级索引/二级索引找
      if (ord < 0 || ord >= entry.termsDictSize) {// 看超过词典总数没有
        throw new IndexOutOfBoundsException();
      }
      // Signed shift since ord is -1 when the terms enum is not positioned
      final long currentBlockIndex = this.ord >> TERMS_DICT_BLOCK_LZ4_SHIFT;
      final long blockIndex = ord >> TERMS_DICT_BLOCK_LZ4_SHIFT;//每64个value压缩一起？
      if (ord < this.ord || blockIndex != currentBlockIndex) {// 不在当前block
        // The looked up ord is before the current ord or belongs to a different block, seek again
        final long blockAddress = blockAddresses.get(blockIndex);// 二级索引乍到这个block的地址
        bytes.seek(blockAddress);
        this.ord = (blockIndex << TERMS_DICT_BLOCK_LZ4_SHIFT) - 1;
      }
      // Scan to the looked up ord
      while (this.ord < ord) {
        next();
      }
    }

    private BytesRef getTermFromIndex(long index) throws IOException {
      assert index >= 0 && index <= (entry.termsDictSize - 1) >>> entry.termsDictIndexShift;
      final long start = indexAddresses.get(index);
      term.length = (int) (indexAddresses.get(index + 1) - start);
      indexBytes.readBytes(start, term.bytes, 0, term.length);
      return term;
    }

    private long seekTermsIndex(BytesRef text) throws IOException {
      long lo = 0L;
      long hi = (entry.termsDictSize - 1) >> entry.termsDictIndexShift;
      while (lo <= hi) {
        final long mid = (lo + hi) >>> 1;// 读取中间这个词的term
        getTermFromIndex(mid);
        final int cmp = term.compareTo(text);
        if (cmp <= 0) {
          lo = mid + 1;
        } else {
          hi = mid - 1;
        }
      }

      assert hi < 0 || getTermFromIndex(hi).compareTo(text) <= 0;
      assert hi == ((entry.termsDictSize - 1) >> entry.termsDictIndexShift)
          || getTermFromIndex(hi + 1).compareTo(text) > 0;
      assert hi < 0 ^ entry.termsDictSize > 0; // return -1 iff empty term dict

      return hi;
    }

    private BytesRef getFirstTermFromBlock(long block) throws IOException {
      assert block >= 0 && block <= (entry.termsDictSize - 1) >>> TERMS_DICT_BLOCK_LZ4_SHIFT;
      final long blockAddress = blockAddresses.get(block);
      bytes.seek(blockAddress);
      term.length = bytes.readVInt();
      bytes.readBytes(term.bytes, 0, term.length);
      return term;
    }

    private long seekBlock(BytesRef text) throws IOException {// 根据 text是首先根据二级索引找
      long index = seekTermsIndex(text);// 在词典二级索引上查找
      if (index == -1L) { // 没找到
        // empty terms dict
        this.ord = 0;
        return -2L;
      }

      long ordLo = index << entry.termsDictIndexShift;// 词具体的起始termOrder
      long ordHi = Math.min(entry.termsDictSize, ordLo + (1L << entry.termsDictIndexShift)) - 1L;

      long blockLo = ordLo >>> TERMS_DICT_BLOCK_LZ4_SHIFT;
      long blockHi = ordHi >>> TERMS_DICT_BLOCK_LZ4_SHIFT;// 在二级16个词中找，

      while (blockLo <= blockHi) {
        final long blockMid = (blockLo + blockHi) >>> 1;
        getFirstTermFromBlock(blockMid);
        final int cmp = term.compareTo(text);
        if (cmp <= 0) {
          blockLo = blockMid + 1;
        } else {
          blockHi = blockMid - 1;
        }
      }

      assert blockHi < 0 || getFirstTermFromBlock(blockHi).compareTo(text) <= 0;
      assert blockHi == ((entry.termsDictSize - 1) >>> TERMS_DICT_BLOCK_LZ4_SHIFT)
          || getFirstTermFromBlock(blockHi + 1).compareTo(text) > 0;

      // read the block only if term dict is not empty
      assert entry.termsDictSize > 0;
      // reset ord and bytes to the ceiling block even if
      // text is before the first term (blockHi == -1)
      final long block = Math.max(blockHi, 0);
      final long blockAddress = blockAddresses.get(block);
      this.ord = block << TERMS_DICT_BLOCK_LZ4_SHIFT;
      bytes.seek(blockAddress);
      decompressBlock();

      return blockHi;
    }

    @Override
    public SeekStatus seekCeil(BytesRef text) throws IOException {// 看某个词是否被找到了
      final long block = seekBlock(text);// block：是在指在哪个具体的64个词的索引中
      if (block == -2) {  // 没找到
        // empty terms dict
        assert entry.termsDictSize == 0;
        return SeekStatus.END;
      } else if (block == -1) {//
        // before the first term
        return SeekStatus.NOT_FOUND;
      }

      while (true) {
        int cmp = term.compareTo(text);
        if (cmp == 0) {
          return SeekStatus.FOUND;
        } else if (cmp > 0) {
          return SeekStatus.NOT_FOUND;
        }
        if (next() == null) {
          return SeekStatus.END;
        }
      }
    }

    private void decompressBlock() throws IOException {// 开始读取，并解压这个block(64个term)
      // The first term is kept uncompressed, so no need to decompress block if only
      // look up the first term when doing seek block.
      term.length = bytes.readVInt();// 第一个词是未压缩的
      bytes.readBytes(term.bytes, 0, term.length);
      long offset = bytes.getFilePointer();
      if (offset < entry.termsDataLength - 1) {// 还没结束
        // Avoid decompress again if we are reading a same block.
        if (currentCompressedBlockStart != offset) {
          blockBuffer.offset = term.length;
          blockBuffer.length = bytes.readVInt();
          // Decompress the remaining of current block, using the first term as a dictionary
          System.arraycopy(term.bytes, 0, blockBuffer.bytes, 0, blockBuffer.offset);//把第一个词作为词典
          LZ4.decompress(bytes, blockBuffer.length, blockBuffer.bytes, blockBuffer.offset);// 把64个词全部读取出来
          currentCompressedBlockStart = offset;
          currentCompressedBlockEnd = bytes.getFilePointer();
        } else {
          // Skip decompression but need to re-seek to block end.
          bytes.seek(currentCompressedBlockEnd);
        }

        // Reset the buffer.
        blockInput.reset(blockBuffer.bytes, blockBuffer.offset, blockBuffer.length);// 将解压的64个词，放入blockInput中
      }
    }

    @Override
    public BytesRef term() throws IOException {
      return term;
    }

    @Override
    public long ord() throws IOException {
      return ord;
    }

    @Override
    public long totalTermFreq() throws IOException {
      return -1L;
    }

    @Override
    public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public ImpactsEnum impacts(int flags) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int docFreq() throws IOException {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
    SortedNumericEntry entry = sortedNumerics.get(field.number);
    return getSortedNumeric(entry, skippers.get(field.number));
  }

  private SortedNumericDocValues getSortedNumeric(
      SortedNumericEntry entry, DocValuesSkipperEntry skipperEntry) throws IOException {
    if (entry.numValues == entry.numDocsWithField) {
      return DocValues.singleton(getNumeric(entry));// 每个文档该字段只有一个值
    }
    // 有的doc 的term个数>1个，这里进来了
    final RandomAccessInput addressesInput =// 获取每个doc包含的term的个数。 // 可以读取“每个doc存放的多少value的列” 的具体valuecount
        data.randomAccessSlice(entry.addressesOffset, entry.addressesLength);
    // Prefetch the first page of data. Following pages are expected to get prefetched through
    // read-ahead.
    if (addressesInput.length() > 0) {
      addressesInput.prefetch(0, 1);
    }
    final LongValues addresses =
        DirectMonotonicReader.getInstance(entry.addressesMeta, addressesInput, merging);

    final LongValues values = getNumericValues(entry);
    final int denseFixedCardinality = fixedCardinality(entry, skipperEntry);

    if (entry.docsWithFieldOffset == -1) {
      // dense
      return new SortedNumericDocValues() {

        int doc = -1;
        long start, end;
        int count;

        @Override
        public int nextDoc() throws IOException {
          return advance(doc + 1);
        }

        @Override
        public int docID() {
          return doc;
        }

        @Override
        public long cost() {
          return maxDoc;
        }

        @Override
        public int advance(int target) throws IOException {
          if (target >= maxDoc) {
            return doc = NO_MORE_DOCS;
          }
          start = addresses.get(target);
          end = addresses.get(target + 1L);
          count = (int) (end - start);
          return doc = target;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
          start = addresses.get(target);
          end = addresses.get(target + 1L);
          count = (int) (end - start);
          doc = target;
          return true;
        }

        @Override
        public long nextValue() throws IOException {
          return values.get(start++);
        }

        @Override
        public int docValueCount() {
          return count;
        }

        @Override
        public void rangeIntoBitSet(
            int fromDoc, int toDoc, long minValue, long maxValue, FixedBitSet bitSet, int offset) {
          int endDoc = Math.min(toDoc, maxDoc);
          if (fromDoc >= endDoc) {
            return;
          }
          if (entry.bitsPerValue == 0) {
            if (entry.minValue >= minValue && entry.minValue <= maxValue) {
              bitSet.set(fromDoc - offset, endDoc - offset);
            }
            return;
          }
          int cardinality = denseFixedCardinality;
          if (cardinality > 1) {
            sortedNumericScalarRangeIntoBitSet(
                values, fromDoc, endDoc, cardinality, minValue, maxValue, bitSet, offset);
            return;
          }
          for (int currentDoc = fromDoc; currentDoc < endDoc; currentDoc++) {
            long startOffset = addresses.get(currentDoc);
            long endOffset = addresses.get(currentDoc + 1L);
            if (sortedNumericMatchesRange(values, startOffset, endOffset, minValue, maxValue)) {
              bitSet.set(currentDoc - offset);
            }
          }
        }

        @Override
        public int docIDRunEnd() throws IOException {
          return maxDoc;
        }

        @Override
        public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
          assert offset <= doc;
          upTo = Math.min(upTo, maxDoc);
          if (upTo > doc) {
            bitSet.set(doc - offset, upTo - offset);
            advance(upTo);
          }
        }
      };
    } else {
      // sparse
      final IndexedDISI disi =
          new IndexedDISI(
              data,
              entry.docsWithFieldOffset,
              entry.docsWithFieldLength,
              entry.jumpTableEntryCount,
              entry.denseRankPower,
              entry.numDocsWithField);
      return new SortedNumericDocValues() {

        boolean set;
        long start, end;
        int count;

        @Override
        public int nextDoc() throws IOException {
          set = false;
          return disi.nextDoc();
        }

        @Override
        public int docID() {
          return disi.docID();
        }

        @Override
        public long cost() {
          return disi.cost();
        }

        @Override
        public int advance(int target) throws IOException {
          set = false;
          return disi.advance(target);
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
          set = false;
          return disi.advanceExact(target);
        }

        @Override
        public long nextValue() throws IOException {
          set();
          return values.get(start++);
        }

        @Override
        public int docValueCount() {
          set();
          return count;
        }

        @Override
        public void rangeIntoBitSet(
            int fromDoc, int toDoc, long minValue, long maxValue, FixedBitSet bitSet, int offset)
            throws IOException {
          set = false;
          int endDoc = Math.min(toDoc, maxDoc);
          if (fromDoc >= endDoc) {
            return;
          }
          int currentDoc = disi.docID();
          if (currentDoc < fromDoc) {
            currentDoc = disi.advance(fromDoc);
          }
          if (currentDoc >= endDoc) {
            return;
          }
          if (entry.bitsPerValue == 0) {
            if (entry.minValue >= minValue && entry.minValue <= maxValue) {
              disi.intoBitSet(endDoc, bitSet, offset);
            }
            set = false;
            return;
          }
          for (; currentDoc < endDoc; currentDoc = disi.nextDoc()) {
            int index = disi.index();
            long startOffset = addresses.get(index);
            long endOffset = addresses.get(index + 1L);
            if (sortedNumericMatchesRange(values, startOffset, endOffset, minValue, maxValue)) {
              bitSet.set(currentDoc - offset);
            }
          }
          set = false;
        }

        @Override
        public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
          set = false;
          disi.intoBitSet(upTo, bitSet, offset);
        }

        private void set() {
          if (set == false) {
            final int index = disi.index();
            start = addresses.get(index);
            end = addresses.get(index + 1L);
            count = (int) (end - start);
            set = true;
          }
        }

        @Override
        public int docIDRunEnd() throws IOException {
          return disi.docIDRunEnd();
        }
      };
    }
  }

  @Override
  public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
    SortedSetEntry entry = sortedSets.get(field.number);
    if (entry.singleValueEntry != null) {// 就是每个文档只有一个词。SingletonSortedNumericDoubleValues里面的docValueCount()始终=1
      return DocValues.singleton(getSorted(entry.singleValueEntry));
    }

    // Specialize the common case for ordinals: single block of packed integers.
    SortedNumericEntry ordsEntry = entry.ordsEntry;
    if (ordsEntry.blockShift < 0 && ordsEntry.bitsPerValue > 0) {
      if (ordsEntry.gcd != 1 || ordsEntry.minValue != 0 || ordsEntry.table != null) {
        throw new IllegalStateException("Ordinals shouldn't use GCD, offset or table compression");
      }
      //
      final RandomAccessInput addressesInput =// 可以读取“每个doc存放的多少value的列” 的具体valuecount
          data.randomAccessSlice(ordsEntry.addressesOffset, ordsEntry.addressesLength);
      // Prefetch the first page of data. Following pages are expected to get prefetched through
      // read-ahead.
      if (addressesInput.length() > 0) {
        addressesInput.prefetch(0, 1);
      }
      final LongValues addresses =//  排序好的termId在dvd中存放的起始位置
          DirectMonotonicReader.getInstance(ordsEntry.addressesMeta, addressesInput);
      // 遍历每个文档包含的term个数
      final RandomAccessInput slice = //存储的具体的每个termId
          data.randomAccessSlice(ordsEntry.valuesOffset, ordsEntry.valuesLength);
      // Prefetch the first page of data. Following pages are expected to get prefetched through
      // read-ahead.
      if (slice.length() > 0) {
        slice.prefetch(0, 1);
      }
      final LongValues values = DirectReader.getInstance(slice, ordsEntry.bitsPerValue);

      if (ordsEntry.docsWithFieldOffset == -1) { // dense
        return new BaseSortedSetDocValues(entry, data) {

          private final int maxDoc = Lucene90DocValuesProducer.this.maxDoc;
          private int doc = -1;
          private long curr;
          private int count;

          @Override
          public long nextOrd() throws IOException {
            return values.get(curr++);
          }

          @Override
          public boolean advanceExact(int target) throws IOException {
            curr = addresses.get(target);
            long end = addresses.get(target + 1L);
            count = (int) (end - curr);
            doc = target;
            return true;
          }

          @Override
          public int docValueCount() {
            return count;
          }

          @Override
          public int docID() {
            return doc;
          }

          @Override
          public int nextDoc() throws IOException {
            return advance(doc + 1);
          }
          // target是文档Id
          @Override
          public int advance(int target) throws IOException {
            if (target >= maxDoc) {
              return doc = NO_MORE_DOCS;
            }
            curr = addresses.get(target);
            long end = addresses.get(target + 1L);
            count = (int) (end - curr);
            return doc = target;
          }

          @Override
          public long cost() {
            return maxDoc;
          }

          @Override
          public int docIDRunEnd() throws IOException {
            return maxDoc;
          }

          @Override
          public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
            assert offset <= doc;
            upTo = Math.min(upTo, maxDoc);
            if (upTo > doc) {
              bitSet.set(doc - offset, upTo - offset);
              advance(upTo);
            }
          }
        };
      } else if (ordsEntry.docsWithFieldOffset >= 0) { // sparse but non-empty
        final IndexedDISI disi =
            new IndexedDISI(
                data,
                ordsEntry.docsWithFieldOffset,
                ordsEntry.docsWithFieldLength,
                ordsEntry.jumpTableEntryCount,
                ordsEntry.denseRankPower,
                ordsEntry.numValues);

        return new BaseSortedSetDocValues(entry, data) {

          boolean set;
          long curr;
          int count;

          @Override
          public long nextOrd() throws IOException {
            set();
            return values.get(curr++);
          }

          @Override
          public boolean advanceExact(int target) throws IOException {
            set = false;
            return disi.advanceExact(target);
          }

          @Override
          public int docValueCount() {
            set();
            return count;
          }

          @Override
          public int docID() {
            return disi.docID();
          }

          @Override
          public int nextDoc() throws IOException {
            set = false;
            return disi.nextDoc();
          }

          @Override
          public int advance(int target) throws IOException {
            set = false;
            return disi.advance(target);
          }

          @Override
          public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
            set = false;
            disi.intoBitSet(upTo, bitSet, offset);
          }

          @Override
          public long cost() {
            return disi.cost();
          }

          private void set() {
            if (set == false) {
              final int index = disi.index();
              curr = addresses.get(index);
              long end = addresses.get(index + 1L);
              count = (int) (end - curr);
              set = true;
            }
          }

          @Override
          public int docIDRunEnd() throws IOException {
            return disi.docIDRunEnd();
          }
        };
      }
    }

    final SortedNumericDocValues ords = getSortedNumeric(ordsEntry, null);
    return new BaseSortedSetDocValues(entry, data) {

      @Override
      public long nextOrd() throws IOException {
        return ords.nextValue();
      }

      @Override
      public int docValueCount() {
        return ords.docValueCount();
      }

      @Override
      public boolean advanceExact(int target) throws IOException {
        return ords.advanceExact(target);
      }

      @Override
      public int docID() {
        return ords.docID();
      }

      @Override
      public int nextDoc() throws IOException {
        return ords.nextDoc();
      }

      @Override
      public int advance(int target) throws IOException {
        return ords.advance(target);
      }

      @Override
      public long cost() {
        return ords.cost();
      }

      @Override
      public int docIDRunEnd() throws IOException {
        return ords.docIDRunEnd();
      }

      @Override
      public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
        ords.intoBitSet(upTo, bitSet, offset);
      }
    };
  }

  @Override
  public void checkIntegrity() throws IOException {
    CodecUtil.checksumEntireFile(data);
    if (skipIndexData != null) {
      CodecUtil.checksumEntireFile(skipIndexData);
    }
  }

  /**
   * Reader for longs split into blocks of different bits per values. The longs are requested by
   * index and must be accessed in monotonically increasing order.
   */
  // Note: The order requirement could be removed as the jump-tables allow for backwards iteration
  // Note 2: The rankSlice is only used if an advance of > 1 block is called. Its construction could
  // be lazy
  private class VaryingBPVReader {
    final RandomAccessInput slice; // 2 slices to avoid cache thrashing when using rank
    final RandomAccessInput rankSlice;
    final NumericEntry entry;
    final int shift;
    final long mul;
    final int mask;

    long block = -1;
    long delta;
    long offset;
    long blockEndOffset;
    LongValues values;

    VaryingBPVReader(NumericEntry entry, RandomAccessInput slice) throws IOException {
      this.entry = entry;
      this.slice = slice;
      this.rankSlice =
          entry.valueJumpTableOffset == -1
              ? null
              : data.randomAccessSlice(
                  entry.valueJumpTableOffset, data.length() - entry.valueJumpTableOffset);
      if (rankSlice != null && rankSlice.length() > 0) {
        // Prefetch the first page of data. Following pages are expected to get prefetched through
        // read-ahead.
        rankSlice.prefetch(0, 1);
      }
      shift = entry.blockShift;
      mul = entry.gcd;
      mask = (1 << shift) - 1;
    }

    long getLongValue(long index) throws IOException {
      final long block = index >>> shift;
      if (this.block != block) {
        int bitsPerValue;
        do {
          // If the needed block is the one directly following the current block, it is cheaper to
          // avoid the cache
          if (rankSlice != null && block != this.block + 1) {
            blockEndOffset = rankSlice.readLong(block * Long.BYTES) - entry.valuesOffset;
            this.block = block - 1;
          }
          offset = blockEndOffset;
          bitsPerValue = slice.readByte(offset++);
          delta = slice.readLong(offset);
          offset += Long.BYTES;
          if (bitsPerValue == 0) {
            blockEndOffset = offset;
          } else {
            final int length = slice.readInt(offset);
            offset += Integer.BYTES;
            blockEndOffset = offset + length;
          }
          this.block++;
        } while (this.block != block);
        final int numValues =
            Math.toIntExact(Math.min(1 << shift, entry.numValues - (block << shift)));
        values =
            bitsPerValue == 0
                ? LongValues.ZEROES
                : getDirectReaderInstance(slice, bitsPerValue, offset, numValues);
      }
      return mul * values.get(index & mask) + delta;
    }
  }
  // Weight.scorerSupplier时候会跑到这里
  @Override
  public DocValuesSkipper getSkipper(FieldInfo field) throws IOException {
    final DocValuesSkipperEntry entry = skippers.get(field.number);

    final IndexInput skipperSource = skipIndexData != null ? skipIndexData : data;
    final IndexInput input = skipperSource.slice("doc value skipper", entry.offset, entry.length);// 记录的这个位置
    // TODO: should we write to disk the actual max level for this segment?
    return new DocValuesSkipper() {
      final int[] minDocID = new int[SKIP_INDEX_MAX_LEVEL];
      final int[] maxDocID = new int[SKIP_INDEX_MAX_LEVEL];

      {
        for (int i = 0; i < SKIP_INDEX_MAX_LEVEL; i++) {
          minDocID[i] = maxDocID[i] = -1;
        }
      }

      final long[] minValue = new long[SKIP_INDEX_MAX_LEVEL];
      final long[] maxValue = new long[SKIP_INDEX_MAX_LEVEL];
      final int[] docCount = new int[SKIP_INDEX_MAX_LEVEL];
      int levels = 1;// 控制当前有效的level级别，比如一级level可以cover，那么就看二级levelel是否可以控制局面

      @Override
      public void advance(int target) throws IOException {
        if (target > entry.maxDocId) {// 超过了doc_value层面记录的最大文档，是不可能的。那么就直接废弃这个level。
          // skipper is exhausted
          for (int i = 0; i < SKIP_INDEX_MAX_LEVEL; i++) {
            minDocID[i] = maxDocID[i] = DocIdSetIterator.NO_MORE_DOCS;
          }
        } else {
          // find next interval
          assert target > maxDocID[0]
              : "target " + target + " must be bigger that current interval " + maxDocID[0];
          while (true) {
            levels = input.readByte();//首先check level。不紧不慢的读取，不怕慢点，就怕错过。一次前进一位也不少了，4096个文档
            assert levels <= SKIP_INDEX_MAX_LEVEL && levels > 0
                : "level out of range [" + levels + "]";
            boolean valid = true;
            // check if current interval is competitive or we can jump to the next position
            for (int level = levels - 1; level >= 0; level--) {//倒着存储的level，每次把每一层的SkipAccumulator全部读取到手
              if ((maxDocID[level] = input.readInt()) < target) {// 说明这个级别的level层级不够
                input.skipBytes(SKIP_INDEX_JUMP_LENGTH_PER_LEVEL[level]); // the jump for the level。才跳这么点
                valid = false;// 接着读取下一个Accu
                break;
              }// 级别够了，会一直读取到底
              minDocID[level] = input.readInt();
              maxValue[level] = input.readLong();
              minValue[level] = input.readLong();
              docCount[level] = input.readInt();
            }
            if (valid) {// 还是有效的
              // adjust levels
              while (levels < SKIP_INDEX_MAX_LEVEL && maxDocID[levels] >= target) {// 这个level还是有效的。比如一级level可以cover，那么就看二级levelel是否可以控制局面
                levels++;// 需要再升级一级别查找
              }
              break;
            }
          }
        }
      }

      @Override
      public int numLevels() {
        return levels;
      }

      @Override
      public int minDocID(int level) {
        return minDocID[level];
      }

      @Override
      public int maxDocID(int level) {
        return maxDocID[level];
      }

      @Override
      public long minValue(int level) {
        return minValue[level];
      }

      @Override
      public long maxValue(int level) {
        return maxValue[level];
      }

      @Override
      public int docCount(int level) {
        return docCount[level];
      }

      @Override
      public long minValue() {
        return entry.minValue;
      }

      @Override
      public long maxValue() {
        return entry.maxValue;
      }

      @Override
      public int docCount() {
        return entry.docCount;
      }

      @Override
      public int maxValueCount() {
        return entry.maxValueCount;
      }
    };
  }
}
