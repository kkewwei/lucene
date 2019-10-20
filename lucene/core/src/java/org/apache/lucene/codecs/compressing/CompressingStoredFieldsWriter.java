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
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.codecs.compressing.CompressingStoredFieldsReader.SerializedDocument;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocIDMerger;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.GrowableByteArrayDataOutput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.packed.PackedInts;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
 // 被DocumentsWriterPerThread拥有，每刷新产生segment一次，则该对象就被置空（在merge时也会自动单独创建一个,公用一个shard的IndexWriter）。下次写入就写到另外一个索引文档
/**
 * {@link StoredFieldsWriter} impl for {@link CompressingStoredFieldsFormat}.
 * @lucene.experimental
 */ // fdt: 每个chunk
public final class CompressingStoredFieldsWriter extends StoredFieldsWriter {
  // Lucene8.5.2中会产生三个文件fdt、fdm、fdx。 fdm存放byte[]的元数据，fdx存放byte[]的具体数据
  /** Extension of stored fields file */
  public static final String FIELDS_EXTENSION = "fdt";// 存数据的
  /** Extension of stored fields index */
  public static final String INDEX_EXTENSION_PREFIX = "fd"; // 存元数据的
  /** Codec name for the index. */
  public static final String INDEX_CODEC_NAME = "Lucene85FieldsIndex";

  static final int         STRING = 0x00;
  static final int       BYTE_ARR = 0x01;
  static final int    NUMERIC_INT = 0x02;
  static final int  NUMERIC_FLOAT = 0x03;
  static final int   NUMERIC_LONG = 0x04;
  static final int NUMERIC_DOUBLE = 0x05;

  static final int TYPE_BITS = PackedInts.bitsRequired(NUMERIC_DOUBLE); // 需要几位才可以表示总共的字段类型
  static final int TYPE_MASK = (int) PackedInts.maxValue(TYPE_BITS);

  static final int VERSION_START = 1;
  static final int VERSION_OFFHEAP_INDEX = 2;
  static final int VERSION_CURRENT = VERSION_OFFHEAP_INDEX;

  private final String segment;// _0
  private FieldsIndexWriter indexWriter; //向_fdx写数据wtriter=CompressingStoredFieldsIndexWriter
  private IndexOutput fieldsStream;// 向fdt中写入数据,RateLimitedIndexOutput

  private Compressor compressor;
  private final CompressionMode compressionMode;
  private final int chunkSize;  // 16kb
  private final int maxDocsPerChunk;  // 128

  private final GrowableByteArrayDataOutput bufferedDocs; // 其中之一存放的是fdt里面的值，域编号->域类型->域value
  private int[] numStoredFields; // number of stored fields // 下标为文档号，value是这个文档有几个域
  private int[] endOffsets; // end offsets in bufferedDocs // 下标是文档号，value是这个文档store value在bufferedDocs中存储域值的截止位置。
  private int docBase; // doc ID at the beginning of the chunk  // 内存中chunk的文档起始id。每次新segment就为0
  private int numBufferedDocs; // docBase + numBufferedDocs == current doc ID // 当前chunk缓存的文档个数.
  
  private long numChunks; // number of compressed blocks written   第几个压缩存储到chunk中的。是这个fdt上统计的所有chunk个数
  private long numDirtyChunks; // number of incomplete compressed blocks written

  /** Sole constructor. */
  CompressingStoredFieldsWriter(Directory directory, SegmentInfo si, String segmentSuffix, IOContext context,
      String formatName, CompressionMode compressionMode, int chunkSize, int maxDocsPerChunk, int blockShift) throws IOException {
    assert directory != null;
    this.segment = si.name;   // _0
    this.compressionMode = compressionMode;  // 默认FAST
    this.compressor = compressionMode.newCompressor(); //CompressionMode$LZ4HighCompressor
    this.chunkSize = chunkSize;
    this.maxDocsPerChunk = maxDocsPerChunk;
    this.docBase = 0;
    this.bufferedDocs = new GrowableByteArrayDataOutput(chunkSize);// 分配该 doc 的 store field buffer，实际大小会比 chunkSize 多1/8，因为每次扩容会扩原有大小的1/8
    this.numStoredFields = new int[16];// 长度是缓存的 doc 数量，自扩容，每个元素是单个 doc 中的所有 stored field 的总数量
    this.endOffsets = new int[16];// 长度是缓存的 doc 数量，自扩容，每个元素是单个 doc 的所有 stored field 的 value 的总长度
    this.numBufferedDocs = 0;// flush 之前缓存的 doc 数量

    boolean success = false; // 创建_0.fdx和_0.fdt文件
    try {
      fieldsStream = directory.createOutput(IndexFileNames.segmentFileName(segment, segmentSuffix, FIELDS_EXTENSION), context); // fdt文件
      CodecUtil.writeIndexHeader(fieldsStream, formatName, VERSION_CURRENT, si.getId(), segmentSuffix);
      assert CodecUtil.indexHeaderLength(formatName, segmentSuffix) == fieldsStream.getFilePointer();
      // 里面会建立俩新文件名_eg7_Lucene85FieldsIndex-doc_ids_0.tmp及_eg7_Lucene85FieldsIndexfile_pointers_1.tmp
      indexWriter = new FieldsIndexWriter(directory, segment, segmentSuffix, INDEX_EXTENSION_PREFIX, INDEX_CODEC_NAME, si.getId(), blockShift, context);// 写索引
      // 每个chunkSize的大小
      fieldsStream.writeVInt(chunkSize);// 写 fdt 文件 chunkSize 和 packedInts 版本号
      fieldsStream.writeVInt(PackedInts.VERSION_CURRENT);

      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(fieldsStream, indexWriter);
      }
    }
  }

  @Override
  public void close() throws IOException {
    try {
      IOUtils.close(fieldsStream, indexWriter, compressor);
    } finally {
      fieldsStream = null; // 全部置空
      indexWriter = null;
      compressor = null;
    }
  }

  private int numStoredFieldsInDoc; // 这个文档有几个需要存储的。

  @Override
  public void startDocument() throws IOException {
  }
  // 写完一个文档后，就会调用
  @Override
  public void finishDocument() throws IOException {
    if (numBufferedDocs == this.numStoredFields.length) { // 需要扩容
      final int newLength = ArrayUtil.oversize(numBufferedDocs + 1, 4);
      this.numStoredFields = ArrayUtil.growExact(this.numStoredFields, newLength);
      endOffsets = ArrayUtil.growExact(endOffsets, newLength);
    }
    this.numStoredFields[numBufferedDocs] = numStoredFieldsInDoc; // 该doc有几个域需要存储
    numStoredFieldsInDoc = 0;
    endOffsets[numBufferedDocs] = bufferedDocs.getPosition(); // 是把整个字段结束位置给存储起来
    ++numBufferedDocs;
    if (triggerFlush()) { // 该bufferedDocs使用大小超过chunk限制16k了，或者该bufferedDocs存储文档书超过chunk限制128了
      flush();
    }
  }
  // 如何压缩存储int,全部存储到out中
  private static void saveInts(int[] values, int length, DataOutput out) throws IOException {
    assert length > 0;
    if (length == 1) {
      out.writeVInt(values[0]);
    } else {
      boolean allEqual = true; // 假定存储字段都是一样的，那么只用存储一份
      for (int i = 1; i < length; ++i) {
        if (values[i] != values[0]) {
          allEqual = false;
          break;
        }
      }
      if (allEqual) { // 所有的storefield都是一样的
        out.writeVInt(0);
        out.writeVInt(values[0]); // 存储storefield个数
      } else {
        long max = 0;
        for (int i = 0; i < length; ++i) {
          max |= values[i];
        }
        final int bitsRequired = PackedInts.bitsRequired(max); // 根据单个doc文档大小，确定需要存储的长度
        out.writeVInt(bitsRequired); // 首先写入
        final PackedInts.Writer w = PackedInts.getWriterNoHeader(out, PackedInts.Format.PACKED, length, bitsRequired, 1);
        for (int i = 0; i < length; ++i) {
          w.add(values[i]); // 将数组转化为了byte并写入了文件
        }
        w.finish();
      }
    }
  }

  private void writeHeader(int docBase, int numBufferedDocs, int[] numStoredFields, int[] lengths, boolean sliced) throws IOException {
    final int slicedBit = sliced ? 1 : 0;
    
    // save docBase and numBufferedDocs
    fieldsStream.writeVInt(docBase); // 向fdt中写入当前chunk的起始位置
    fieldsStream.writeVInt((numBufferedDocs) << 1 | slicedBit); //  // 向fdt中写入该chunk的文档个数

    // save numStoredFields
    saveInts(numStoredFields, numBufferedDocs, fieldsStream); // 首先存储所有文件的numStoredFields

    // save lengths
    saveInts(lengths, numBufferedDocs, fieldsStream); // 存储了所有文档的字节长度
  }

  private boolean triggerFlush() {
    return bufferedDocs.getPosition() >= chunkSize || // chunks of at least chunkSize bytes   //大小超过了16kb
        numBufferedDocs >= maxDocsPerChunk; // 128个文档
  } // flush主要还是在构建fdt数据结构， flush里面的writeBlock是在构建fdx数据结构
  //只有当本类内存中缓存的个数或者大小超过16k,或者128个文档，在每个文档addDocument完成时都会检查这两项而刷新。当然flush、commit都会调用
  private void flush() throws IOException { // 两个地方会调用：1. 每次写完128/16K文档时， flush阶段，若还有缓存文档，则会主动调用
    indexWriter.writeIndex(numBufferedDocs, fieldsStream.getFilePointer());  // 主要是向doc_ids和file_pointers中写入文档个数、fdt偏移量信息

    // transform end offsets into lengths
    final int[] lengths = endOffsets;
    for (int i = numBufferedDocs - 1; i > 0; --i) {
      lengths[i] = endOffsets[i] - endOffsets[i - 1]; // 每个文档的长度
      assert lengths[i] >= 0;
    }
    final boolean sliced = bufferedDocs.getPosition() >= 2 * chunkSize; // 若存储总长度大于32kb, 就需要分开压缩
    writeHeader(docBase, numBufferedDocs, numStoredFields, lengths, sliced); // 向fdt写入numStoredFields与lengths

    // compress stored fields to fieldsStream
    if (sliced) {  // 若切分的话，就切分存储，一次写入不能超过chunk大小（16k）
      // big chunk, slice it
      for (int compressed = 0; compressed < bufferedDocs.getPosition(); compressed += chunkSize) {
        compressor.compress(bufferedDocs.getBytes(), compressed, Math.min(chunkSize, bufferedDocs.getPosition() - compressed), fieldsStream);
      }
    } else {// 压缩 stored fields value 并写入文件，压缩是按 chunk 来压缩的
      compressor.compress(bufferedDocs.getBytes(), 0, bufferedDocs.getPosition(), fieldsStream); // LZ4FastCompressor.compress并写入了
    }

    // reset
    docBase += numBufferedDocs; // 记录所有chunk中已经包含的文档数
    numBufferedDocs = 0;
    bufferedDocs.reset();
    numChunks++;
  }
  
  @Override
  public void writeField(FieldInfo info, IndexableField field)
      throws IOException {

    ++numStoredFieldsInDoc;

    int bits = 0;
    final BytesRef bytes;
    final String string;

    Number number = field.numericValue(); // 字段类型
    if (number != null) {
      if (number instanceof Byte || number instanceof Short || number instanceof Integer) {
        bits = NUMERIC_INT;
      } else if (number instanceof Long) {
        bits = NUMERIC_LONG;
      } else if (number instanceof Float) {
        bits = NUMERIC_FLOAT;
      } else if (number instanceof Double) {
        bits = NUMERIC_DOUBLE;
      } else {
        throw new IllegalArgumentException("cannot store numeric type " + number.getClass());
      }
      string = null;
      bytes = null;
    } else {
      bytes = field.binaryValue();
      if (bytes != null) { // 是二进制数的话
        bits = BYTE_ARR;
        string = null;
      } else {
        bits = STRING; // 是字符串的话
        string = field.stringValue();
        if (string == null) {
          throw new IllegalArgumentException("field " + field.name() + " is stored but does not have binaryValue, stringValue nor numericValue");
        }
      }
    }

    final long infoAndBits = (((long) info.number) << TYPE_BITS) | bits; // 低三位代表类型，高5位代表字段编号
    bufferedDocs.writeVLong(infoAndBits); //写入字段编号，字段类型

    if (bytes != null) {
      bufferedDocs.writeVInt(bytes.length);
      bufferedDocs.writeBytes(bytes.bytes, bytes.offset, bytes.length);
    } else if (string != null) {
      bufferedDocs.writeString(string); // 存储字符串真正的值
    } else {
      if (number instanceof Byte || number instanceof Short || number instanceof Integer) {
        bufferedDocs.writeZInt(number.intValue());
      } else if (number instanceof Long) {
        writeTLong(bufferedDocs, number.longValue());
      } else if (number instanceof Float) {
        writeZFloat(bufferedDocs, number.floatValue());
      } else if (number instanceof Double) {
        writeZDouble(bufferedDocs, number.doubleValue());
      } else {
        throw new AssertionError("Cannot get here");
      }
    }
  }

  // -0 isn't compressed.
  static final int NEGATIVE_ZERO_FLOAT = Float.floatToIntBits(-0f);
  static final long NEGATIVE_ZERO_DOUBLE = Double.doubleToLongBits(-0d);

  // for compression of timestamps
  static final long SECOND = 1000L;
  static final long HOUR = 60 * 60 * SECOND;
  static final long DAY = 24 * HOUR;
  static final int SECOND_ENCODING = 0x40;
  static final int HOUR_ENCODING = 0x80;
  static final int DAY_ENCODING = 0xC0;

  /** 
   * Writes a float in a variable-length format.  Writes between one and 
   * five bytes. Small integral values typically take fewer bytes.
   * <p>
   * ZFloat --&gt; Header, Bytes*?
   * <ul>
   *    <li>Header --&gt; {@link DataOutput#writeByte Uint8}. When it is
   *       equal to 0xFF then the value is negative and stored in the next
   *       4 bytes. Otherwise if the first bit is set then the other bits
   *       in the header encode the value plus one and no other
   *       bytes are read. Otherwise, the value is a positive float value
   *       whose first byte is the header, and 3 bytes need to be read to
   *       complete it.
   *    <li>Bytes --&gt; Potential additional bytes to read depending on the
   *       header.
   * </ul>
   */
  static void writeZFloat(DataOutput out, float f) throws IOException {
    int intVal = (int) f;
    final int floatBits = Float.floatToIntBits(f);

    if (f == intVal
        && intVal >= -1
        && intVal <= 0x7D
        && floatBits != NEGATIVE_ZERO_FLOAT) {
      // small integer value [-1..125]: single byte
      out.writeByte((byte) (0x80 | (1 + intVal)));
    } else if ((floatBits >>> 31) == 0) {
      // other positive floats: 4 bytes
      out.writeInt(floatBits);
    } else {
      // other negative float: 5 bytes
      out.writeByte((byte) 0xFF);
      out.writeInt(floatBits);
    }
  }
  
  /** 
   * Writes a float in a variable-length format.  Writes between one and 
   * five bytes. Small integral values typically take fewer bytes.
   * <p>
   * ZFloat --&gt; Header, Bytes*?
   * <ul>
   *    <li>Header --&gt; {@link DataOutput#writeByte Uint8}. When it is
   *       equal to 0xFF then the value is negative and stored in the next
   *       8 bytes. When it is equal to 0xFE then the value is stored as a
   *       float in the next 4 bytes. Otherwise if the first bit is set
   *       then the other bits in the header encode the value plus one and
   *       no other bytes are read. Otherwise, the value is a positive float
   *       value whose first byte is the header, and 7 bytes need to be read
   *       to complete it.
   *    <li>Bytes --&gt; Potential additional bytes to read depending on the
   *       header.
   * </ul>
   */
  static void writeZDouble(DataOutput out, double d) throws IOException {
    int intVal = (int) d;
    final long doubleBits = Double.doubleToLongBits(d);
    
    if (d == intVal &&
        intVal >= -1 && 
        intVal <= 0x7C &&
        doubleBits != NEGATIVE_ZERO_DOUBLE) {
      // small integer value [-1..124]: single byte
      out.writeByte((byte) (0x80 | (intVal + 1)));
      return;
    } else if (d == (float) d) {
      // d has an accurate float representation: 5 bytes
      out.writeByte((byte) 0xFE);
      out.writeInt(Float.floatToIntBits((float) d));
    } else if ((doubleBits >>> 63) == 0) {
      // other positive doubles: 8 bytes
      out.writeLong(doubleBits);
    } else {
      // other negative doubles: 9 bytes
      out.writeByte((byte) 0xFF);
      out.writeLong(doubleBits);
    }
  }

  /** 
   * Writes a long in a variable-length format.  Writes between one and 
   * ten bytes. Small values or values representing timestamps with day,
   * hour or second precision typically require fewer bytes.
   * <p>
   * ZLong --&gt; Header, Bytes*?
   * <ul>
   *    <li>Header --&gt; The first two bits indicate the compression scheme:
   *       <ul>
   *          <li>00 - uncompressed
   *          <li>01 - multiple of 1000 (second)
   *          <li>10 - multiple of 3600000 (hour)
   *          <li>11 - multiple of 86400000 (day)
   *       </ul>
   *       Then the next bit is a continuation bit, indicating whether more
   *       bytes need to be read, and the last 5 bits are the lower bits of
   *       the encoded value. In order to reconstruct the value, you need to
   *       combine the 5 lower bits of the header with a vLong in the next
   *       bytes (if the continuation bit is set to 1). Then
   *       {@link BitUtil#zigZagDecode(int) zigzag-decode} it and finally
   *       multiply by the multiple corresponding to the compression scheme.
   *    <li>Bytes --&gt; Potential additional bytes to read depending on the
   *       header.
   * </ul>
   */
  // T for "timestamp"
  static void writeTLong(DataOutput out, long l) throws IOException {
    int header; 
    if (l % SECOND != 0) {
      header = 0;
    } else if (l % DAY == 0) {
      // timestamp with day precision
      header = DAY_ENCODING;
      l /= DAY;
    } else if (l % HOUR == 0) {
      // timestamp with hour precision, or day precision with a timezone
      header = HOUR_ENCODING;
      l /= HOUR;
    } else {
      // timestamp with second precision
      header = SECOND_ENCODING;
      l /= SECOND;
    }

    final long zigZagL = BitUtil.zigZagEncode(l);
    header |= (zigZagL & 0x1F); // last 5 bits
    final long upperBits = zigZagL >>> 5;
    if (upperBits != 0) {
      header |= 0x20;
    }
    out.writeByte((byte) header);
    if (upperBits != 0) {
      out.writeVLong(upperBits);
    }
  }
  // 只有在主动indexWriter.flush()/若文档数或者内存使用数达到上限，才会主动调用。去关闭旧的fdx。表示写完一个segment了
  @Override
  public void finish(FieldInfos fis, int numDocs) throws IOException {
    if (numBufferedDocs > 0) { // 只要有，就会刷新到磁盘
      flush(); //
      numDirtyChunks++; // incomplete: we had to force this flush
    } else {
      assert bufferedDocs.getPosition() == 0;
    }
    if (docBase != numDocs) {
      throw new RuntimeException("Wrote " + docBase + " docs, finish called with numDocs=" + numDocs);
    } //
    indexWriter.finish(numDocs, fieldsStream.getFilePointer()); // 仅仅向fdx中写入索引信息，处理 fdx 的 finish 流程，写 block， 写文件尾。
    fieldsStream.writeVLong(numChunks); // fdt
    fieldsStream.writeVLong(numDirtyChunks); //
    CodecUtil.writeFooter(fieldsStream);
    assert bufferedDocs.getPosition() == 0;
  }
  
  // bulk merge is scary: its caused corruption bugs in the past.
  // we try to be extra safe with this impl, but add an escape hatch to
  // have a workaround for undiscovered bugs.
  static final String BULK_MERGE_ENABLED_SYSPROP = CompressingStoredFieldsWriter.class.getName() + ".enableBulkMerge";
  static final boolean BULK_MERGE_ENABLED;
  static {
    boolean v = true;
    try {
      v = Boolean.parseBoolean(System.getProperty(BULK_MERGE_ENABLED_SYSPROP, "true"));
    } catch (SecurityException ignored) {}
    BULK_MERGE_ENABLED = v;
  }
  // 将从ElasticsearchConcurrentMergeScheduler.doMerge()->ConcurrentMergeScheduler.doMerge()->IndexWriter.merge()->IndexWriter.mergeMiddle()
  @Override  // 该函数接受一个 MergeState mergeState 对象。其包含了merge 需要的各个 segment 文件的 store field reader 对象
  public int merge(MergeState mergeState) throws IOException {
    int docCount = 0; // 变量是所有segment中全局的
    int numReaders = mergeState.maxDocs.length; // 多少个semgent来合并
    
    MatchingReaders matching = new MatchingReaders(mergeState);
    if (mergeState.needsIndexSort) { // 一般跳过
      /**
       * If all readers are compressed and they have the same fieldinfos then we can merge the serialized document
       * directly.
       */
      List<CompressingStoredFieldsMergeSub> subs = new ArrayList<>();
      for(int i=0;i<mergeState.storedFieldsReaders.length;i++) { // 遍历每一个storedFieldsReaders文件
        if (matching.matchingReaders[i] &&
            mergeState.storedFieldsReaders[i] instanceof CompressingStoredFieldsReader) {
          CompressingStoredFieldsReader storedFieldsReader = (CompressingStoredFieldsReader) mergeState.storedFieldsReaders[i];
          storedFieldsReader.checkIntegrity();
          subs.add(new CompressingStoredFieldsMergeSub(storedFieldsReader, mergeState.docMaps[i], mergeState.maxDocs[i]));
        } else {
          return super.merge(mergeState);
        }
      }

      final DocIDMerger<CompressingStoredFieldsMergeSub> docIDMerger =
          DocIDMerger.of(subs, true);
      while (true) {
        CompressingStoredFieldsMergeSub sub = docIDMerger.next();
        if (sub == null) {
          break;
        }
        assert sub.mappedDocID == docCount;
        SerializedDocument doc = sub.reader.document(sub.docID);
        startDocument();
        bufferedDocs.copyBytes(doc.in, doc.length);
        numStoredFieldsInDoc = doc.numStoredFields;
        finishDocument();
        ++docCount;
      }
      finish(mergeState.mergeFieldInfos, docCount);
      return docCount;
    }
    
    for (int readerIndex=0;readerIndex<numReaders;readerIndex++) { // 开始遍历了，
      MergeVisitor visitor = new MergeVisitor(mergeState, readerIndex);
      CompressingStoredFieldsReader matchingFieldsReader = null; // 指向fdt文件
      if (matching.matchingReaders[readerIndex]) {
        final StoredFieldsReader fieldsReader = mergeState.storedFieldsReaders[readerIndex]; //CompressingStoredFieldsReader
        // we can only bulk-copy if the matching reader is also a CompressingStoredFieldsReader
        if (fieldsReader != null && fieldsReader instanceof CompressingStoredFieldsReader) {
          matchingFieldsReader = (CompressingStoredFieldsReader) fieldsReader;
        }
      }

      final int maxDoc = mergeState.maxDocs[readerIndex]; // 这个segment的文档ID
      final Bits liveDocs = mergeState.liveDocs[readerIndex];
       // 一般跳过if
      // if its some other format, or an older version of this format, or safety switch:
      if (matchingFieldsReader == null || matchingFieldsReader.getVersion() != VERSION_CURRENT || BULK_MERGE_ENABLED == false) {
        // naive merge...
        StoredFieldsReader storedFieldsReader = mergeState.storedFieldsReaders[readerIndex];
        if (storedFieldsReader != null) {
          storedFieldsReader.checkIntegrity();// 检查文件的完整性
        }
        for (int docID = 0; docID < maxDoc; docID++) {// maxDoc为该 segment 中文档总数，docID 在 segment 中从0顺序递增分布
          if (liveDocs != null && liveDocs.get(docID) == false) {
            continue;
          }
          startDocument();
          storedFieldsReader.visitDocument(docID, visitor);
          finishDocument();
          ++docCount;
        }
      } else if (matchingFieldsReader.getCompressionMode() == compressionMode &&
                 matchingFieldsReader.getChunkSize() == chunkSize && 
                 matchingFieldsReader.getPackedIntsVersion() == PackedInts.VERSION_CURRENT &&
                 liveDocs == null && // 没有删除的文档
                 !tooDirty(matchingFieldsReader)) { // 没有脏chunk,脏chunk占比1%
        // optimized merge, raw byte copy
        // its not worth fine-graining this if there are deletions.
        
        // if the format is older, its always handled by the naive merge case above
        assert matchingFieldsReader.getVersion() == VERSION_CURRENT;        
        matchingFieldsReader.checkIntegrity(); // 发现这个比较耗时,直接跳转到文件末尾，检验fdx的正确性
        // 其实这里刷新意义不大，是不是可以最后统一刷新就好了？
        // flush any pending chunks
        if (numBufferedDocs > 0) { // 如果还有未写入的段,
          flush();
          numDirtyChunks++; // incomplete: we had to force this flush
        }
        
        // iterate over each chunk. we use the stored fields index to find chunk boundaries,
        // read the docstart + doccount from the chunk header (we write a new header, since doc numbers will change),
        // and just copy the bytes directly.
        IndexInput rawDocs = matchingFieldsReader.getFieldsStream(); // fdt文件
        FieldsIndex index = matchingFieldsReader.getIndexReader(); // FieldsIndexReader
        rawDocs.seek(index.getStartPointer(0)); // 首先从第0个chunk开始找
        int docID = 0; // 这个segment目前的文档数
        while (docID < maxDoc) { // 每循环一次，读取一个chunk
          // read header
          int base = rawDocs.readVInt();// base没啥用
          if (base != docID) {
            throw new CorruptIndexException("invalid state: base=" + base + ", docID=" + docID, rawDocs);
          }
          int code = rawDocs.readVInt();
          
          // write a new index entry and new header for this chunk.
          int bufferedDocs = code >>> 1;
          indexWriter.writeIndex(bufferedDocs, fieldsStream.getFilePointer()); // 向doc,filepoint写入
          fieldsStream.writeVInt(docBase); // rebase  自己维护一个segment类的全局的docBase
          fieldsStream.writeVInt(code);
          docID += bufferedDocs;
          docBase += bufferedDocs;
          docCount += bufferedDocs;
          
          if (docID > maxDoc) {
            throw new CorruptIndexException("invalid state: base=" + base + ", count=" + bufferedDocs + ", maxDoc=" + maxDoc, rawDocs);
          }
          
          // copy bytes until the next chunk boundary (or end of chunk data).
          // using the stored fields index for this isn't the most efficient, but fast enough
          // and is a source of redundancy for detecting bad things.
          final long end; // 这里有点错位不好理解：doc文件写入0,128,255,385这样的，而filepointer写入n1,n2,n3，实际chunk=1指的是doc中的128、filepointer中的n2
          if (docID == maxDoc) {
            end = matchingFieldsReader.getMaxPointer();
          } else {
            end = index.getStartPointer(docID); // 错位，实际效果找的是chunk+1在fdt中的起始位置。
          } // 发现这个操作也很耗时,从rawDocs读取数据
          fieldsStream.copyBytes(rawDocs, end - rawDocs.getFilePointer()); // 会进行merge中断
        } //是拷贝fdt中每个chunK的后三方面（numStoredFields, Doclength BufferDoc）
               
        if (rawDocs.getFilePointer() != matchingFieldsReader.getMaxPointer()) {
          throw new CorruptIndexException("invalid state: pos=" + rawDocs.getFilePointer() + ", max=" + matchingFieldsReader.getMaxPointer(), rawDocs);
        }
        
        // since we bulk merged all chunks, we inherit any dirty ones from this segment.
        numChunks += matchingFieldsReader.getNumChunks();
        numDirtyChunks += matchingFieldsReader.getNumDirtyChunks();
      } else { // 或者有删除文档、或者有太多脏chunk
        // optimized merge, we copy serialized (but decompressed) bytes directly
        // even on simple docs (1 stored field), it seems to help by about 20%
        
        // if the format is older, its always handled by the naive merge case above
        assert matchingFieldsReader.getVersion() == VERSION_CURRENT;
        matchingFieldsReader.checkIntegrity();

        for (int docID = 0; docID < maxDoc; docID++) {
          if (liveDocs != null && liveDocs.get(docID) == false) { // 这个节点需要被删除，那么直接忽略
            continue;
          }
          SerializedDocument doc = matchingFieldsReader.document(docID);
          startDocument();
          bufferedDocs.copyBytes(doc.in, doc.length); // 读取出来
          numStoredFieldsInDoc = doc.numStoredFields;
          finishDocument();
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
   */ // 因为太多脏chunk,某些情况下，压缩效率会降低
  boolean tooDirty(CompressingStoredFieldsReader candidate) {
    // more than 1% dirty, or more than hard limit of 1024 dirty chunks
    return candidate.getNumDirtyChunks() > 1024 || 
           candidate.getNumDirtyChunks() * 100 > candidate.getNumChunks();
  }

  private static class CompressingStoredFieldsMergeSub extends DocIDMerger.Sub {
    private final CompressingStoredFieldsReader reader;
    private final int maxDoc;
    int docID = -1;

    public CompressingStoredFieldsMergeSub(CompressingStoredFieldsReader reader, MergeState.DocMap docMap, int maxDoc) {
      super(docMap);
      this.maxDoc = maxDoc;
      this.reader = reader;
    }

    @Override
    public int nextDoc() {
      docID++;
      if (docID == maxDoc) {
        return NO_MORE_DOCS;
      } else {
        return docID;
      }
    }
  }
}
