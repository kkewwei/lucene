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
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BaseTermsEnum;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocValues;
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
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.compress.LZ4;
import org.apache.lucene.util.packed.DirectMonotonicReader;
import org.apache.lucene.util.packed.DirectReader;
 // 查询    里面包含全量的该segment的DocValues字段，每个节点重启的时候就会加载这些元数据
/** reader for {@link Lucene80DocValuesFormat} */
final class Lucene80DocValuesProducer extends DocValuesProducer implements Closeable {
  private final Map<String,NumericEntry> numerics = new HashMap<>(); // 存放数值类型
  private final Map<String,BinaryEntry> binaries = new HashMap<>(); // 存放二进制
  private final Map<String,SortedEntry> sorted = new HashMap<>(); // 存放sorted类型
  private final Map<String,SortedSetEntry> sortedSets = new HashMap<>(); //  存放sortedSet类型
  private final Map<String,SortedNumericEntry> sortedNumerics = new HashMap<>();//  存放sortedNumer类型
  private long ramBytesUsed; //仅仅是类名
  private final IndexInput data;// 仅仅映射该segment dvd全量数据
  private final int maxDoc;
  private int version = -1;
  // segment加载的时候就会进来
  /** expert: instantiates a new reader */
  Lucene80DocValuesProducer(SegmentReadState state, String dataCodec, String dataExtension, String metaCodec, String metaExtension) throws IOException {
    String metaName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);// dvm
    this.maxDoc = state.segmentInfo.maxDoc();
    ramBytesUsed = RamUsageEstimator.shallowSizeOfInstance(getClass());

    // read in the entries from the metadata file.
    try (ChecksumIndexInput in = state.directory.openChecksumInput(metaName, state.context)) {
      Throwable priorE = null;
      
      try {
        version = CodecUtil.checkIndexHeader(in, metaCodec,
                                        Lucene80DocValuesFormat.VERSION_START,
                                        Lucene80DocValuesFormat.VERSION_CURRENT,
                                        state.segmentInfo.getId(),
                                        state.segmentSuffix);
        readFields(in, state.fieldInfos);
      } catch (Throwable exception) {
        priorE = exception;
      } finally {
        CodecUtil.checkFooter(in, priorE);
      }
    }

    String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, dataExtension);
    this.data = state.directory.openInput(dataName, state.context);
    boolean success = false;
    try {
      final int version2 = CodecUtil.checkIndexHeader(data, dataCodec,
                                                 Lucene80DocValuesFormat.VERSION_START,
                                                 Lucene80DocValuesFormat.VERSION_CURRENT,
                                                 state.segmentInfo.getId(),
                                                 state.segmentSuffix);
      if (version != version2) {
        throw new CorruptIndexException("Format versions mismatch: meta=" + version + ", data=" + version2, data);
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
  }
  // 在数据节点启动的时候，就会加载元数据dvm，映射 Lucene80DocValuesConsumer.addSortedSetField()
  private void readFields(ChecksumIndexInput meta, FieldInfos infos) throws IOException {
    for (int fieldNumber = meta.readInt(); fieldNumber != -1; fieldNumber = meta.readInt()) { // 依次读取每个字段
      FieldInfo info = infos.fieldInfo(fieldNumber);
      if (info == null) {
        throw new CorruptIndexException("Invalid field number: " + fieldNumber, meta);
      }
      byte type = meta.readByte();// 可看Lucene80DocValuesConsumer.addSortedSetField
      if (type == Lucene80DocValuesFormat.NUMERIC) {
        numerics.put(info.name, readNumeric(meta));
      } else if (type == Lucene80DocValuesFormat.BINARY) {
        binaries.put(info.name, readBinary(meta));
      } else if (type == Lucene80DocValuesFormat.SORTED) {
        sorted.put(info.name, readSorted(meta));
      } else if (type == Lucene80DocValuesFormat.SORTED_SET) {
        sortedSets.put(info.name, readSortedSet(meta));
      } else if (type == Lucene80DocValuesFormat.SORTED_NUMERIC) {
        sortedNumerics.put(info.name, readSortedNumeric(meta));
      } else {
        throw new CorruptIndexException("invalid type: " + type, meta);
      }
    }
  }
  // 这个是读取NUMERIC类型，启动时读取dvm文件
  private NumericEntry readNumeric(ChecksumIndexInput meta) throws IOException {
    NumericEntry entry = new NumericEntry();
    readNumeric(meta, entry);
    return entry;
  }
  // 可以看下Lucene80DocValuesConsumer.doAddSortedField()，从dvm中读取
  private void readNumeric(ChecksumIndexInput meta, NumericEntry entry) throws IOException {
    entry.docsWithFieldOffset = meta.readLong(); // 若每个文档id都有该字段，那么不用存储docId编号，该值为-1。
    entry.docsWithFieldLength = meta.readLong(); // // 若每个文档id都有该字段，那么不用存储docId编号，该值为0。
    entry.jumpTableEntryCount = meta.readShort(); // -1
    entry.denseRankPower = meta.readByte(); // -1 写死了的
    entry.numValues = meta.readLong(); // 文档个数
    int tableSize = meta.readInt();
    if (tableSize > 256) {
      throw new CorruptIndexException("invalid table size: " + tableSize, meta);
    }
    if (tableSize >= 0) {
      entry.table = new long[tableSize];
      ramBytesUsed += RamUsageEstimator.sizeOf(entry.table);
      for (int i = 0; i < tableSize; ++i) {
        entry.table[i] = meta.readLong();
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
    entry.valuesOffset = meta.readLong();
    entry.valuesLength = meta.readLong();
    entry.valueJumpTableOffset = meta.readLong();
  }
  // 这个是读取BINARY类型，启动时读取dvm文件
  private BinaryEntry readBinary(ChecksumIndexInput meta) throws IOException {
    BinaryEntry entry = new BinaryEntry();
    entry.dataOffset = meta.readLong();
    entry.dataLength = meta.readLong();
    entry.docsWithFieldOffset = meta.readLong(); //即将写入docId
    entry.docsWithFieldLength = meta.readLong();
    entry.jumpTableEntryCount = meta.readShort();
    entry.denseRankPower = meta.readByte();
    entry.numDocsWithField = meta.readInt();
    entry.minLength = meta.readInt();
    entry.maxLength = meta.readInt();
    if ((version >= Lucene80DocValuesFormat.VERSION_BIN_COMPRESSED && entry.numDocsWithField > 0) ||  entry.minLength < entry.maxLength) {
      entry.addressesOffset = meta.readLong();

      // Old count of uncompressed addresses 
      long numAddresses = entry.numDocsWithField + 1L;
      // New count of compressed addresses - the number of compresseed blocks
      if (version >= Lucene80DocValuesFormat.VERSION_BIN_COMPRESSED) {
        entry.numCompressedChunks = meta.readVInt();
        entry.docsPerChunkShift = meta.readVInt();
        entry.maxUncompressedChunkSize = meta.readVInt();
        numAddresses = entry.numCompressedChunks;
      }      
      
      final int blockShift = meta.readVInt();
      entry.addressesMeta = DirectMonotonicReader.loadMeta(meta, numAddresses, blockShift);
      ramBytesUsed += entry.addressesMeta.ramBytesUsed();
      entry.addressesLength = meta.readLong();
    }
    return entry;
  } // 磁盘启动时就加载了
  // 可看 Lucene80DocValuesConsumer.doAddsortedField()，从dvm中读取，在节点启动的时候就读取，读取SORTED_SET类型
  private SortedEntry readSorted(ChecksumIndexInput meta) throws IOException {
    SortedEntry entry = new SortedEntry();
    entry.docsWithFieldOffset = meta.readLong(); // docId存放起始位置
    entry.docsWithFieldLength = meta.readLong(); // docId存放长度
    entry.jumpTableEntryCount = meta.readShort(); // 记录多少个block
    entry.denseRankPower = meta.readByte();  //密度，默认9（1<<9=512个文档一个rank)
    entry.numDocsWithField = meta.readInt(); // 文档个数
    entry.bitsPerValue = meta.readByte(); // 每个value的存储占用的 长度
    entry.ordsOffset = meta.readLong();//  排序好的termId在dvd中存放的起始位置
    entry.ordsLength = meta.readLong();//termId在dvd中写结束位置
    readTermDict(meta, entry);
    return entry;
  }
   // 参考 Lucene80DocValuesConsumer.addSortedSetField 中间的代码，节点启动时，读取dvm的整个文件
  private SortedSetEntry readSortedSet(ChecksumIndexInput meta) throws IOException {
    SortedSetEntry entry = new SortedSetEntry();
    byte multiValued = meta.readByte();// dvm
    switch (multiValued) { // 每个文档只有一个词，一般都会跑到这里
      case 0: // singlevalued  每个文档都有个该值
        entry.singleValueEntry = readSorted(meta);// 一般要进来，每个文档的该域只有只有一个value
        return entry;
      case 1: // multivalued
        break;
      default:
        throw new CorruptIndexException("Invalid multiValued flag: " + multiValued, meta);
    }// 一般来说，不会进来的
    entry.docsWithFieldOffset = meta.readLong(); // docid在dvd中存放的起始位置
    entry.docsWithFieldLength = meta.readLong(); // docId存放的长度
    entry.jumpTableEntryCount = meta.readShort();
    entry.denseRankPower = meta.readByte(); // 密度为9（9个long，512个文档）统计一次存在多少个文档
    entry.bitsPerValue = meta.readByte();
    entry.ordsOffset = meta.readLong();  // 存放词的起始位置
    entry.ordsLength = meta.readLong(); // 词的长度
    entry.numDocsWithField = meta.readInt();// 多少个文档有这个词
    entry.addressesOffset = meta.readLong(); // 第一级别词典存放位置
    final int blockShift = meta.readVInt();
    entry.addressesMeta = DirectMonotonicReader.loadMeta(meta, entry.numDocsWithField + 1, blockShift);
    ramBytesUsed += entry.addressesMeta.ramBytesUsed();
    entry.addressesLength = meta.readLong();
    readTermDict(meta, entry);
    return entry;
  }
  // 可查看 Lucene80DocValuesConsumer.addTermsDict
  private static void readTermDict(ChecksumIndexInput meta, TermsDictEntry entry) throws IOException {
    entry.termsDictSize = meta.readVLong(); // 多少个独立的词（相同词算一个）
    entry.termsDictBlockShift = meta.readInt();//4 一级索引，每1<<4个次会产生一个索引
    final int blockShift = meta.readInt();//16 ，存储的时候是个二维byte[]，一维存储大小为32kb=1<<16
    final long addressesSize = (entry.termsDictSize + (1L << entry.termsDictBlockShift) - 1) >>> entry.termsDictBlockShift;// 一级索引多少个节点
    entry.termsAddressesMeta = DirectMonotonicReader.loadMeta(meta, addressesSize, blockShift);// 从dvm加载一级索引（每16个词在dvd中存放起始位置）的元数据
    entry.maxTermLength = meta.readInt();// 最长的那个词长度
    entry.termsDataOffset = meta.readLong();// 向dvd中开始写terms的原始值（每个词的相同前缀长度及后缀）的起始位置
    entry.termsDataLength = meta.readLong();// // dvd中所有value的长度
    entry.termsAddressesOffset = meta.readLong();// 开始向dvd写一级索引（每16个词的在dvd中存放）的起始位置
    entry.termsAddressesLength = meta.readLong();// dvd中一级索引的长度
    entry.termsDictIndexShift = meta.readInt();// 10， 二级索引区间1<<10=1024    （开始二级索引信息）// 二级索引由两部分构成，一部分是第1024*x个词的相同前缀内容，第二部分是第1024*x个词和第1024*x-1个词的相同前缀累加值（数组会放在dvm和dvd中）
    final long indexSize = (entry.termsDictSize + (1L << entry.termsDictIndexShift) - 1) >>> entry.termsDictIndexShift;//二级索引多少个节点
    entry.termsIndexAddressesMeta = DirectMonotonicReader.loadMeta(meta, 1 + indexSize, blockShift);// 从dvm中加载二级索引第二部分（第1024*x个词和第1024*x-1个词的相同前缀累加值）的元数据部分
    entry.termsIndexOffset = meta.readLong();// 在dvd中存放的第二级索引第一部分（第1024*x个词的相同前缀内容）存放的起始位置
    entry.termsIndexLength = meta.readLong();// 在dvd中存放的第二级索引第一部分（第1024*x个词的相同前缀内容）存放的长度
    entry.termsIndexAddressesOffset = meta.readLong();// 从dvd中加载二级索引第二部分（第1024*x个词和第1024*x-1个词的相同前缀累加值）的数据部分
    entry.termsIndexAddressesLength = meta.readLong();// 从dvd中加载二级索引第二部分（第1024*x个词和第1024*x-1个词的相同前缀累加值）的数据部分长度
  } // profix1 value, profix2 value,, profix2 value,
  // 这个是读取SORTED_NUMBER类型
  private SortedNumericEntry readSortedNumeric(ChecksumIndexInput meta) throws IOException {
    SortedNumericEntry entry = new SortedNumericEntry();
    readNumeric(meta, entry);
    entry.numDocsWithField = meta.readInt();
    if (entry.numDocsWithField != entry.numValues) {
      entry.addressesOffset = meta.readLong();
      final int blockShift = meta.readVInt();
      entry.addressesMeta = DirectMonotonicReader.loadMeta(meta, entry.numDocsWithField + 1, blockShift);
      ramBytesUsed += entry.addressesMeta.ramBytesUsed();
      entry.addressesLength = meta.readLong();
    }
    return entry;
  }

  @Override
  public void close() throws IOException {
    data.close();
  }

  private static class NumericEntry {
    long[] table;
    int blockShift;
    byte bitsPerValue;
    long docsWithFieldOffset;// 若每个文档id都有该字段，那么不用存储docId编号，该值为-1。
    long docsWithFieldLength;
    short jumpTableEntryCount;
    byte denseRankPower;
    long numValues;
    long minValue;
    long gcd;
    long valuesOffset;
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
    long addressesOffset;
    long addressesLength;
    DirectMonotonicReader.Meta addressesMeta;
    int numCompressedChunks;
    int docsPerChunkShift;
    int maxUncompressedChunkSize;
  }
  // 在节点启动时会去dvm加载文件所有数据
  private static class TermsDictEntry {
    long termsDictSize;// 多少个独立的词（相同词算一个）
    int termsDictBlockShift; // 4 ,每16个词会产生一级索引
    DirectMonotonicReader.Meta termsAddressesMeta;// 从dvm加载一级索引（每16个词在dvd中存放起始位置）的元数据
    int maxTermLength;
    long termsDataOffset;// 向dvd中开始写terms词原始值（每个词的相同前缀长度及后缀）的起始位置
    long termsDataLength;
    long termsAddressesOffset;// 开始向dvd写一级索引（每16个词的在dvd中存放）的起始位置
    long termsAddressesLength;// dvd写一级索引的长度
    int termsDictIndexShift; // 10,二级索引间距
    DirectMonotonicReader.Meta termsIndexAddressesMeta; // 从dvm中加载二级索引第二部分（第1024*x个词和第1024*x-1个词的相同前缀累加值）的元数据部分
    long termsIndexOffset;// 在dvd中存放的第二级索引第一部分（第1024*x个词的相同前缀内容）存放的起始位置
    long termsIndexLength;
    long termsIndexAddressesOffset; // 从dvd中加载二级索引第二部分（第1024*x个词和第1024*x-1个词的相同前缀累加值）的数据部分
    long termsIndexAddressesLength; // 从dvd中加载二级索引第二部分的数据部分
  } // dvd中映射二级索引第一部分是在 Lucene80DocValuesProducer$TermsDict构造函数中操作的

  private static class SortedEntry extends TermsDictEntry {
    long docsWithFieldOffset; // 若每个文档id都有该字段，那么不用存储docId编号，该值为-1。
    long docsWithFieldLength; // dvd中存储docId部分使用的总长度
    short jumpTableEntryCount; // 返回多少个block
    byte denseRankPower;  // -1
    int numDocsWithField;// 总文档个数
    byte bitsPerValue;// 每个value的存储占用的 长度
    long ordsOffset;// //  排序好的termId在dvd中存放的起始位置
    long ordsLength;// 排序好的termId在dvd中存放的长度
  }
  // 节点启动时，读取的dvm中整个文件。
  private static class SortedSetEntry extends TermsDictEntry {
    SortedEntry singleValueEntry;
    long docsWithFieldOffset;
    long docsWithFieldLength;// dvd中存储docId部分使用的总长度
    short jumpTableEntryCount; //返回多少个block
    byte denseRankPower;
    int numDocsWithField;
    byte bitsPerValue;
    long ordsOffset;//  排序好的termId在dvd中存放的起始位置
    long ordsLength;// 排序好的termId在dvd中存放的长度
    DirectMonotonicReader.Meta addressesMeta;
    long addressesOffset;
    long addressesLength;
  }

  private static class SortedNumericEntry extends NumericEntry {
    int numDocsWithField;
    DirectMonotonicReader.Meta addressesMeta;
    long addressesOffset;
    long addressesLength;
  }

  @Override
  public long ramBytesUsed() {
    return ramBytesUsed;
  }

  @Override
  public NumericDocValues getNumeric(FieldInfo field) throws IOException {
    NumericEntry entry = numerics.get(field.name);
    return getNumeric(entry);
  }

  private static abstract class DenseNumericDocValues extends NumericDocValues {

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

  }

  private static abstract class SparseNumericDocValues extends NumericDocValues {

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
    public long cost() {
      return disi.cost();
    }
  }

  private NumericDocValues getNumeric(NumericEntry entry) throws IOException {
    if (entry.docsWithFieldOffset == -2) {
      // empty
      return DocValues.emptyNumeric();
    } else if (entry.docsWithFieldOffset == -1) {
      // dense
      if (entry.bitsPerValue == 0) {
        return new DenseNumericDocValues(maxDoc) {
          @Override
          public long longValue() throws IOException {
            return entry.minValue;
          }
        };
      } else {
        final RandomAccessInput slice = data.randomAccessSlice(entry.valuesOffset, entry.valuesLength);
        if (entry.blockShift >= 0) {
          // dense but split into blocks of different bits per value
          return new DenseNumericDocValues(maxDoc) {
            final VaryingBPVReader vBPVReader = new VaryingBPVReader(entry, slice);

            @Override
            public long longValue() throws IOException {
              return vBPVReader.getLongValue(doc);
            }
          };
        } else {
          final LongValues values = DirectReader.getInstance(slice, entry.bitsPerValue);
          if (entry.table != null) {
            final long[] table = entry.table;
            return new DenseNumericDocValues(maxDoc) {
              @Override
              public long longValue() throws IOException {
                return table[(int) values.get(doc)];
              }
            };
          } else {
            final long mul = entry.gcd;
            final long delta = entry.minValue;
            return new DenseNumericDocValues(maxDoc) {
              @Override
              public long longValue() throws IOException {
                return mul * values.get(doc) + delta;
              }
            };
          }
        }
      }
    } else {
      // sparse
      final IndexedDISI disi = new IndexedDISI(data, entry.docsWithFieldOffset, entry.docsWithFieldLength,
          entry.jumpTableEntryCount, entry.denseRankPower, entry.numValues);
      if (entry.bitsPerValue == 0) {
        return new SparseNumericDocValues(disi) {
          @Override
          public long longValue() throws IOException {
            return entry.minValue;
          }
        };
      } else {
        final RandomAccessInput slice = data.randomAccessSlice(entry.valuesOffset, entry.valuesLength);
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
          final LongValues values = DirectReader.getInstance(slice, entry.bitsPerValue);
          if (entry.table != null) {
            final long[] table = entry.table;
            return new SparseNumericDocValues(disi) {
              @Override
              public long longValue() throws IOException {
                return table[(int) values.get(disi.index())];
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
      final RandomAccessInput slice = data.randomAccessSlice(entry.valuesOffset, entry.valuesLength);
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
        final LongValues values = DirectReader.getInstance(slice, entry.bitsPerValue);
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

  private static abstract class DenseBinaryDocValues extends BinaryDocValues {

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
  }

  private static abstract class SparseBinaryDocValues extends BinaryDocValues {

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
  }
  
  // BWC - old binary format 
  private BinaryDocValues getUncompressedBinary(FieldInfo field) throws IOException {
    BinaryEntry entry = binaries.get(field.name);
    if (entry.docsWithFieldOffset == -2) {
      return DocValues.emptyBinary();
    }

    final IndexInput bytesSlice = data.slice("fixed-binary", entry.dataOffset, entry.dataLength);

    if (entry.docsWithFieldOffset == -1) {
      // dense
      if (entry.minLength == entry.maxLength) {
        // fixed length
        final int length = entry.maxLength;
        return new DenseBinaryDocValues(maxDoc) {
          final BytesRef bytes = new BytesRef(new byte[length], 0, length);

          @Override
          public BytesRef binaryValue() throws IOException {
            bytesSlice.seek((long) doc * length);
            bytesSlice.readBytes(bytes.bytes, 0, length);
            return bytes;
          }
        };
      } else {
        // variable length
        final RandomAccessInput addressesData = this.data.randomAccessSlice(entry.addressesOffset, entry.addressesLength);
        final LongValues addresses = DirectMonotonicReader.getInstance(entry.addressesMeta, addressesData);
        return new DenseBinaryDocValues(maxDoc) {
          final BytesRef bytes = new BytesRef(new byte[entry.maxLength], 0, entry.maxLength);

          @Override
          public BytesRef binaryValue() throws IOException {
            long startOffset = addresses.get(doc);
            bytes.length = (int) (addresses.get(doc + 1L) - startOffset);
            bytesSlice.seek(startOffset);
            bytesSlice.readBytes(bytes.bytes, 0, bytes.length);
            return bytes;
          }
        };
      }
    } else {
      // sparse
      final IndexedDISI disi = new IndexedDISI(data, entry.docsWithFieldOffset, entry.docsWithFieldLength,
          entry.jumpTableEntryCount, entry.denseRankPower, entry.numDocsWithField);
      if (entry.minLength == entry.maxLength) {
        // fixed length
        final int length = entry.maxLength;
        return new SparseBinaryDocValues(disi) {
          final BytesRef bytes = new BytesRef(new byte[length], 0, length);

          @Override
          public BytesRef binaryValue() throws IOException {
            bytesSlice.seek((long) disi.index() * length);
            bytesSlice.readBytes(bytes.bytes, 0, length);
            return bytes;
          }
        };
      } else {
        // variable length
        final RandomAccessInput addressesData = this.data.randomAccessSlice(entry.addressesOffset, entry.addressesLength);
        final LongValues addresses = DirectMonotonicReader.getInstance(entry.addressesMeta, addressesData);
        return new SparseBinaryDocValues(disi) {
          final BytesRef bytes = new BytesRef(new byte[entry.maxLength], 0, entry.maxLength);

          @Override
          public BytesRef binaryValue() throws IOException {
            final int index = disi.index();
            long startOffset = addresses.get(index);
            bytes.length = (int) (addresses.get(index + 1L) - startOffset);
            bytesSlice.seek(startOffset);
            bytesSlice.readBytes(bytes.bytes, 0, bytes.length);
            return bytes;
          }
        };
      }
    }
  }  
  
  // Decompresses blocks of binary values to retrieve content
  class BinaryDecoder {
    
    private final LongValues addresses;
    private final IndexInput compressedData;
    // Cache of last uncompressed block 
    private long lastBlockId = -1;
    private final int []uncompressedDocStarts;
    private int uncompressedBlockLength = 0;        
    private final byte[] uncompressedBlock;
    private final BytesRef uncompressedBytesRef;
    private final int docsPerChunk;
    private final int docsPerChunkShift;
    
    public BinaryDecoder(LongValues addresses, IndexInput compressedData, int biggestUncompressedBlockSize, int docsPerChunkShift) {
      super();
      this.addresses = addresses;
      this.compressedData = compressedData;
      // pre-allocate a byte array large enough for the biggest uncompressed block needed.
      this.uncompressedBlock = new byte[biggestUncompressedBlockSize];
      uncompressedBytesRef = new BytesRef(uncompressedBlock);
      this.docsPerChunk = 1 << docsPerChunkShift;
      this.docsPerChunkShift = docsPerChunkShift;
      uncompressedDocStarts = new int[docsPerChunk + 1];
      
    }


    BytesRef decode(int docNumber) throws IOException {
      int blockId = docNumber >> docsPerChunkShift; 
      int docInBlockId = docNumber % docsPerChunk;
      assert docInBlockId < docsPerChunk;
      
      
      // already read and uncompressed?
      if (blockId != lastBlockId) {
        lastBlockId = blockId;
        long blockStartOffset = addresses.get(blockId);
        compressedData.seek(blockStartOffset);
        
        uncompressedBlockLength = 0;        

        int onlyLength = -1;
        for (int i = 0; i < docsPerChunk; i++) {
          if (i == 0) {
            // The first length value is special. It is shifted and has a bit to denote if
            // all other values are the same length
            int lengthPlusSameInd = compressedData.readVInt();
            int sameIndicator = lengthPlusSameInd & 1;
            int firstValLength = lengthPlusSameInd >>>1;
            if (sameIndicator == 1) {
              onlyLength = firstValLength;
            }
            uncompressedBlockLength += firstValLength;            
          } else {
            if (onlyLength == -1) {
              // Various lengths are stored - read each from disk
              uncompressedBlockLength += compressedData.readVInt();            
            } else {
              // Only one length 
              uncompressedBlockLength += onlyLength;
            }
          }
          uncompressedDocStarts[i+1] = uncompressedBlockLength;
        }
        
        if (uncompressedBlockLength == 0) {
          uncompressedBytesRef.offset = 0;
          uncompressedBytesRef.length = 0;
          return uncompressedBytesRef;
        }
        
        assert uncompressedBlockLength <= uncompressedBlock.length;
        LZ4.decompress(compressedData, uncompressedBlockLength, uncompressedBlock);
      }
      
      uncompressedBytesRef.offset = uncompressedDocStarts[docInBlockId];        
      uncompressedBytesRef.length = uncompressedDocStarts[docInBlockId +1] - uncompressedBytesRef.offset;
      return uncompressedBytesRef;
    }    
  }
  

  @Override
  public BinaryDocValues getBinary(FieldInfo field) throws IOException {
    if (version < Lucene80DocValuesFormat.VERSION_BIN_COMPRESSED) {
      return getUncompressedBinary(field);
    }
    
    BinaryEntry entry = binaries.get(field.name);
    if (entry.docsWithFieldOffset == -2) {
      return DocValues.emptyBinary();
    }
    if (entry.docsWithFieldOffset == -1) {
      // dense
      final RandomAccessInput addressesData = this.data.randomAccessSlice(entry.addressesOffset, entry.addressesLength);
      final LongValues addresses = DirectMonotonicReader.getInstance(entry.addressesMeta, addressesData);
      return new DenseBinaryDocValues(maxDoc) {
        BinaryDecoder decoder = new BinaryDecoder(addresses, data.clone(), entry.maxUncompressedChunkSize, entry.docsPerChunkShift);

        @Override
        public BytesRef binaryValue() throws IOException {          
          return decoder.decode(doc);
        }
      };
    } else {
      // sparse
      final IndexedDISI disi = new IndexedDISI(data, entry.docsWithFieldOffset, entry.docsWithFieldLength,
          entry.jumpTableEntryCount, entry.denseRankPower, entry.numDocsWithField);
      final RandomAccessInput addressesData = this.data.randomAccessSlice(entry.addressesOffset, entry.addressesLength);
      final LongValues addresses = DirectMonotonicReader.getInstance(entry.addressesMeta, addressesData);
      return new SparseBinaryDocValues(disi) {
        BinaryDecoder decoder = new BinaryDecoder(addresses, data.clone(), entry.maxUncompressedChunkSize, entry.docsPerChunkShift);

        @Override
        public BytesRef binaryValue() throws IOException {
          return decoder.decode(disi.index());
        }
      };
    }
  }

  @Override
  public SortedDocValues getSorted(FieldInfo field) throws IOException {
    SortedEntry entry = sorted.get(field.name);
    return getSorted(entry);
  }
  // 主要返回termId和docId两部分的映射
  private SortedDocValues getSorted(SortedEntry entry) throws IOException {
    if (entry.docsWithFieldOffset == -2) {
      return DocValues.emptySorted();
    }

    final LongValues ords;// 映射的是dvd中存放排序好的termId部分,下标是文档写入顺序。第0个写入的文档对应词的segment内大小编号为order0
    if (entry.bitsPerValue == 0) {
      ords = new LongValues() {
        @Override
        public long get(long index) {
          return 0L;
        }
      };
    } else {
      final RandomAccessInput slice = data.randomAccessSlice(entry.ordsOffset, entry.ordsLength);// 映射dvd中term OrderId部分
      ords = DirectReader.getInstance(slice, entry.bitsPerValue); //下标是文档写入顺序。第0个写入的文档对应词的大小编号为order0
    }
     // 每个文档都有个该域
    if (entry.docsWithFieldOffset == -1) {
      // dense
      return new BaseSortedDocValues(entry, data) {// 这里会去映射

        int doc = -1;

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
        public boolean advanceExact(int target) {
          doc = target;
          return true;
        }

        @Override
        public int ordValue() {
          return (int) ords.get(doc);
        }
      };
    } else {
      // sparse   // 部分文档都有个该域，需要使用稠密稀疏存储，仅仅来映射jumper和block部分
      final IndexedDISI disi = new IndexedDISI(data, entry.docsWithFieldOffset, entry.docsWithFieldLength,
          entry.jumpTableEntryCount, entry.denseRankPower, entry.numDocsWithField);
      return new BaseSortedDocValues(entry, data) { // 映射dvd中termID的orderId部分、词典一级索引、二级索引部分

        @Override
        public int nextDoc() throws IOException {
          return disi.nextDoc();
        }
        // 当前正在读取的docId
        @Override
        public int docID() {
          return disi.docID();
        }
        // 返回的是总文档数
        @Override
        public long cost() {
          return disi.cost();
        }
        // 获取下一个文档编号
        @Override
        public int advance(int target) throws IOException {
          return disi.advance(target);
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
          return disi.advanceExact(target); // 在dvd的文档hash roarmap中查找该文档id(target)是否存在
        }
        // 获取到这个词在当前segment中的排序号，比如这个文档是第2个文档，词排序后排第5
        @Override
        public int ordValue() {
          return (int) ords.get(disi.index());
        }
      };
    }
  }
  // 查询
  private static abstract class BaseSortedDocValues extends SortedDocValues {

    final SortedEntry entry;// Lucene80DocValuesProduer$StoredEntry
    final IndexInput data;// 映射dvd部分
    final TermsEnum termsEnum; // 一级及二级索引部分

    BaseSortedDocValues(SortedEntry entry, IndexInput data) throws IOException {
      this.entry = entry;
      this.data = data;
      this.termsEnum = termsEnum();
    }

    @Override
    public int getValueCount() {
      return Math.toIntExact(entry.termsDictSize);
    }

    @Override
    public BytesRef lookupOrd(int ord) throws IOException {
      termsEnum.seekExact(ord);
      return termsEnum.term();
    }

    @Override
    public int lookupTerm(BytesRef key) throws IOException {
      SeekStatus status = termsEnum.seekCeil(key);
      switch (status) {
        case FOUND:
          return Math.toIntExact(termsEnum.ord());
        default:
          return Math.toIntExact(-1L - termsEnum.ord());
      }
    }
    // 为啥不是直接将termsEnum用现成的，而在需要重新申请一个新的。
    @Override
    public TermsEnum termsEnum() throws IOException {
      return new TermsDict(entry, data);//
    }
  }
  // 基本SortedSetDocValues
  private static abstract class BaseSortedSetDocValues extends SortedSetDocValues {

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
      return entry.termsDictSize;
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
        default:
          return -1L - termsEnum.ord();
      }
    }

    @Override
    public TermsEnum termsEnum() throws IOException {
      return new TermsDict(entry, data);
    }
  }
  // 会在AggregationPhase.preProcess()中产生
  private static class TermsDict extends BaseTermsEnum {

    final TermsDictEntry entry; // Lucene80DocValuesProduer$StoredEntry
    final LongValues blockAddresses;// 结合了一级索引（每16个词在dvd中存放的起始位置）dvm和dvd中得知，可以容易读取一级索引的值
    final IndexInput bytes;//映射dvd中termId原始值的内容
    final long blockMask;
    final LongValues indexAddresses;// 结合了二级索引第二部分（每第1024x个词和第1024x-1个词相同前缀长度）dvm和dvd中得知，可以容易读取二级第二部分索引的值
    final IndexInput indexBytes;//映射了在dvd中存放的第二级索引第一部分（第1024*x个词的相同前缀内容）的值
    final BytesRef term;// 临时变量，为了方便读取一级索引中每个词的内容
    long ord = -1; // 当前docValue读取到第几个词了
     // 主要是做映射dvd词典一级索引，二级索引索引部分，并没有真正读取值。存在循环多次映射，可以多次读取
    TermsDict(TermsDictEntry entry, IndexInput data) throws IOException {
      this.entry = entry;
      RandomAccessInput addressesSlice = data.randomAccessSlice(entry.termsAddressesOffset, entry.termsAddressesLength);// dvd文件一级索引
      blockAddresses = DirectMonotonicReader.getInstance(entry.termsAddressesMeta, addressesSlice);// 结合了一级索引（每16个词在dvd中存放的起始位置）dvm和dvd中得知，可以容易读取一级索引每个block的值
      bytes = data.slice("terms", entry.termsDataOffset, entry.termsDataLength);
      blockMask = (1L << entry.termsDictBlockShift) - 1;
      RandomAccessInput indexAddressesSlice = data.randomAccessSlice(entry.termsIndexAddressesOffset, entry.termsIndexAddressesLength);
      indexAddresses = DirectMonotonicReader.getInstance(entry.termsIndexAddressesMeta, indexAddressesSlice);// 结合了二级索引第二部分（每第1024x个词和第1024x-1个词相同前缀长度）dvm和dvd中得知，可以容易读取二级第二部分索引的值
      indexBytes = data.slice("terms-index", entry.termsIndexOffset, entry.termsIndexLength);//映射了在dvd中存放的第二级索引第一部分（第1024*x个词的相同前缀内容）的值
      term = new BytesRef(entry.maxTermLength);// 最大那个词的长度
    }

    @Override // 读取每个docValue中词的内容词的原始值。可见 Lucene80DocValuesConsumer.addTermsDict 每个词的存储过程
    public BytesRef next() throws IOException {
      if (++ord >= entry.termsDictSize) {// 读取每个独立的词
        return null;
      }
      if ((ord & blockMask) == 0L) {// 每第16个词，可以读取该词的原始值
        term.length = bytes.readVInt();
        bytes.readBytes(term.bytes, 0, term.length);
      } else { // 
        final int token = Byte.toUnsignedInt(bytes.readByte());// 读取一位，记录该词的相同前缀长度及不同后缀长度
        int prefixLength = token & 0x0F;
        int suffixLength = 1 + (token >>> 4);
        if (prefixLength == 15) { // 若前缀长度大于15
          prefixLength += bytes.readVInt(); // 存储前缀长度
        }
        if (suffixLength == 16) {
          suffixLength += bytes.readVInt();
        }
        term.length = prefixLength + suffixLength;
        bytes.readBytes(term.bytes, prefixLength, suffixLength);// 存放是不同后缀。复用前面一个词的前缀部分
      }
      return term;
    }
    // 定位到词序为ord的位置
    @Override
    public void seekExact(long ord) throws IOException {
      if (ord < 0 || ord >= entry.termsDictSize) {
        throw new IndexOutOfBoundsException();
      }
      final long blockIndex = ord >>> entry.termsDictBlockShift; //哪个一级索引元素下（每16个词作为一个一级索引元素）
      final long blockAddress = blockAddresses.get(blockIndex); // 从dvd中读取第个blockIndex（一个block 16个文档）个数据的起始位置
      bytes.seek(blockAddress);
      this.ord = (blockIndex << entry.termsDictBlockShift) - 1; //该该block（一个block 16个文档）起始位置，开始读取
      do { // 开始读取该ord的词内容
        next();
      } while (this.ord < ord);
    }

    private BytesRef getTermFromIndex(long index) throws IOException {
      assert index >= 0 && index <= (entry.termsDictSize - 1) >>> entry.termsDictIndexShift;
      final long start = indexAddresses.get(index);
      term.length = (int) (indexAddresses.get(index + 1) - start);
      indexBytes.seek(start);
      indexBytes.readBytes(term.bytes, 0, term.length);
      return term;
    }

    private long seekTermsIndex(BytesRef text) throws IOException {
      long lo = 0L;
      long hi = (entry.termsDictSize - 1) >>> entry.termsDictIndexShift;
      while (lo <= hi) {
        final long mid = (lo + hi) >>> 1;
        getTermFromIndex(mid);
        final int cmp = term.compareTo(text);
        if (cmp <= 0) {
          lo = mid + 1;
        } else {
          hi = mid - 1;
        }
      }

      assert hi < 0 || getTermFromIndex(hi).compareTo(text) <= 0;
      assert hi == ((entry.termsDictSize - 1) >>> entry.termsDictIndexShift) || getTermFromIndex(hi + 1).compareTo(text) > 0;

      return hi;
    }

    private BytesRef getFirstTermFromBlock(long block) throws IOException {
      assert block >= 0 && block <= (entry.termsDictSize - 1) >>> entry.termsDictBlockShift;
      final long blockAddress = blockAddresses.get(block);
      bytes.seek(blockAddress);
      term.length = bytes.readVInt();
      bytes.readBytes(term.bytes, 0, term.length);
      return term;
    }

    private long seekBlock(BytesRef text) throws IOException {
      long index = seekTermsIndex(text);
      if (index == -1L) {
        return -1L;
      }

      long ordLo = index << entry.termsDictIndexShift;
      long ordHi = Math.min(entry.termsDictSize, ordLo + (1L << entry.termsDictIndexShift)) - 1L;

      long blockLo = ordLo >>> entry.termsDictBlockShift;
      long blockHi = ordHi >>> entry.termsDictBlockShift;

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
      assert blockHi == ((entry.termsDictSize - 1) >>> entry.termsDictBlockShift) || getFirstTermFromBlock(blockHi + 1).compareTo(text) > 0;

      return blockHi;
    }

    @Override
    public SeekStatus seekCeil(BytesRef text) throws IOException {
      final long block = seekBlock(text);
      if (block == -1) {
        // before the first term
        seekExact(0L);
        return SeekStatus.NOT_FOUND;
      }
      final long blockAddress = blockAddresses.get(block);
      this.ord = block << entry.termsDictBlockShift;
      bytes.seek(blockAddress);
      term.length = bytes.readVInt();
      bytes.readBytes(term.bytes, 0, term.length);
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
    SortedNumericEntry entry = sortedNumerics.get(field.name);
    if (entry.numValues == entry.numDocsWithField) {
      return DocValues.singleton(getNumeric(entry));
    }

    final RandomAccessInput addressesInput = data.randomAccessSlice(entry.addressesOffset, entry.addressesLength);
    final LongValues addresses = DirectMonotonicReader.getInstance(entry.addressesMeta, addressesInput);

    final LongValues values = getNumericValues(entry);

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
      };
    } else {
      // sparse
      final IndexedDISI disi = new IndexedDISI(data, entry.docsWithFieldOffset, entry.docsWithFieldLength,
          entry.jumpTableEntryCount, entry.denseRankPower, entry.numDocsWithField);
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

        private void set() {
          if (set == false) {
            final int index = disi.index();
            start = addresses.get(index);
            end = addresses.get(index + 1L);
            count = (int) (end - start);
            set = true;
          }
        }

      };
    }
  }

  @Override
  public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
    SortedSetEntry entry = sortedSets.get(field.name);//Lucene80DocValueProducer$SortedSetEntry
    if (entry.singleValueEntry != null) {// 就是每个文档只有一个词。那么读取旧跑到这里
      return DocValues.singleton(getSorted(entry.singleValueEntry));// 主要去映射docId部分
    }

    final RandomAccessInput slice = data.randomAccessSlice(entry.ordsOffset, entry.ordsLength);
    final LongValues ords = DirectReader.getInstance(slice, entry.bitsPerValue);

    final RandomAccessInput addressesInput = data.randomAccessSlice(entry.addressesOffset, entry.addressesLength);
    final LongValues addresses = DirectMonotonicReader.getInstance(entry.addressesMeta, addressesInput);

    if (entry.docsWithFieldOffset == -1) {
      // dense
      return new BaseSortedSetDocValues(entry, data) {

        int doc = -1;
        long start;
        long end;

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
          return doc = target;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
          start = addresses.get(target);
          end = addresses.get(target + 1L);
          doc = target;
          return true;
        }

        @Override
        public long nextOrd() throws IOException {
          if (start == end) {
            return NO_MORE_ORDS;
          }
          return ords.get(start++);
        }

      };
    } else {
      // sparse
      final IndexedDISI disi = new IndexedDISI(data, entry.docsWithFieldOffset, entry.docsWithFieldLength,
          entry.jumpTableEntryCount, entry.denseRankPower, entry.numDocsWithField);
      return new BaseSortedSetDocValues(entry, data) {

        boolean set;
        long start;
        long end = 0;

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
        public long nextOrd() throws IOException {
          if (set == false) {
            final int index = disi.index();
            final long start = addresses.get(index);
            this.start = start + 1;
            end = addresses.get(index + 1L);
            set = true;
            return ords.get(start);
          } else if (start == end) {
            return NO_MORE_ORDS;
          } else {
            return ords.get(start++);
          }
        }

      };
    }
  }

  @Override
  public void checkIntegrity() throws IOException {
    CodecUtil.checksumEntireFile(data);
  }

  /**
   * Reader for longs split into blocks of different bits per values.
   * The longs are requested by index and must be accessed in monotonically increasing order.
   */
  // Note: The order requirement could be removed as the jump-tables allow for backwards iteration
  // Note 2: The rankSlice is only used if an advance of > 1 block is called. Its construction could be lazy
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
      this.rankSlice = entry.valueJumpTableOffset == -1 ? null :
          data.randomAccessSlice(entry.valueJumpTableOffset, data.length()-entry.valueJumpTableOffset);
      shift = entry.blockShift;
      mul = entry.gcd;
      mask = (1 << shift) - 1;
    }

    long getLongValue(long index) throws IOException {
      final long block = index >>> shift;
      if (this.block != block) {
        int bitsPerValue;
        do {
          // If the needed block is the one directly following the current block, it is cheaper to avoid the cache
          if (rankSlice != null && block != this.block+1) {
            blockEndOffset = rankSlice.readLong(block*Long.BYTES)-entry.valuesOffset;
            this.block = block-1;
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
        values = bitsPerValue == 0 ? LongValues.ZEROES : DirectReader.getInstance(slice, bitsPerValue, offset);
      }
      return mul * values.get(index & mask) + delta;
    }
  }
}
