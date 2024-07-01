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
package org.apache.lucene.codecs.lucene90.compressing;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.compressing.Compressor;
import org.apache.lucene.codecs.compressing.MatchingReaders;
import org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressingStoredFieldsReader.SerializedDocument;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocIDMerger;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.StoredFieldDataInput;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.packed.PackedInts;
// 被DocumentsWriterPerThread拥有，每刷新产生segment一次，则该对象就被置空（在merge时也会自动单独创建一个,公用一个shard的IndexWriter）。下次写入就写到另外一个索引文档
/**
 * {@link StoredFieldsWriter} impl for {@link Lucene90CompressingStoredFieldsFormat}.
 *
 * @lucene.experimental
 */ //    单个文档形式的，tvd和fdt都是一样存储方式
public final class Lucene90CompressingStoredFieldsWriter extends StoredFieldsWriter {
// Lucene8.5.2中会产生三个文件fdt、fdm、fdx。 fdm存放byte[]的元数据，fdx存放byte[]的具体数据,fdt存放元数据
  /** Extension of stored fields file */
  public static final String FIELDS_EXTENSION = "fdt";// 存数据的

  /** Extension of stored fields index */
  public static final String INDEX_EXTENSION = "fdx";

  /** Extension of stored fields meta */
  public static final String META_EXTENSION = "fdm";// 存元数据的

  /** Codec name for the index. */
  public static final String INDEX_CODEC_NAME = "Lucene90FieldsIndex";

  static final int STRING = 0x00;
  static final int BYTE_ARR = 0x01;
  static final int NUMERIC_INT = 0x02;
  static final int NUMERIC_FLOAT = 0x03;
  static final int NUMERIC_LONG = 0x04;
  static final int NUMERIC_DOUBLE = 0x05;

  static final int TYPE_BITS = PackedInts.bitsRequired(NUMERIC_DOUBLE);// 需要几位才可以表示总共的字段类型
  static final int TYPE_MASK = (int) PackedInts.maxValue(TYPE_BITS);

  static final int VERSION_START = 1;
  static final int VERSION_CURRENT = VERSION_START;
  static final int META_VERSION_START = 0;

  private final String segment;
  private FieldsIndexWriter indexWriter;//向_fdx写数据wtriter=CompressingStoredFieldsIndexWriter
  private IndexOutput metaStream, fieldsStream;// 只有在merge阶段，向fdt中写入数据时，,才是RateLimitedIndexOutput

  private Compressor compressor;
  private final CompressionMode compressionMode;
  private final int chunkSize; // 16kb
  private final int maxDocsPerChunk; // 128

  private final ByteBuffersDataOutput bufferedDocs;// 其中之一存放的是fdt里面的值，域编号->域类型->域value
  private int[] numStoredFields; // number of stored fields 下标为文档号，value是这个文档有几个域
  private int[] endOffsets; // end offsets in bufferedDocs  下标是文档号，value是这个文档store value在bufferedDocs中存储域值的截止位置。
  private int docBase; // doc ID at the beginning of the chunk // 记录当前chunk在整个segment的起始docId下标。
  private int numBufferedDocs; // docBase + numBufferedDocs == current doc ID// 当前chunk缓存的文档个数.

  private long numChunks;
  private long numDirtyChunks; // number of incomplete compressed blocks written
  private long numDirtyDocs; // cumulative number of missing docs in incomplete chunks

  /** Sole constructor. */
  Lucene90CompressingStoredFieldsWriter(
      Directory directory,
      SegmentInfo si,
      String segmentSuffix,
      IOContext context,
      String formatName,
      CompressionMode compressionMode,
      int chunkSize,
      int maxDocsPerChunk,
      int blockShift)
      throws IOException {
    assert directory != null;
    this.segment = si.name;
    this.compressionMode = compressionMode;
    this.compressor = compressionMode.newCompressor();
    this.chunkSize = chunkSize;
    this.maxDocsPerChunk = maxDocsPerChunk;
    this.docBase = 0;
    this.bufferedDocs = ByteBuffersDataOutput.newResettableInstance();// 分配该 doc 的 store field buffer，实际大小会比 chunkSize 多1/8，因为每次扩容会扩原有大小的1/8
    this.numStoredFields = new int[16];// 长度是缓存的 doc 数量，自扩容，每个元素是单个 doc 中的所有 stored field 的总数量
    this.endOffsets = new int[16];// 长度是缓存的 doc 数量，自扩容，每个元素是单个 doc 的所有 stored field 的 value 的总长度
    this.numBufferedDocs = 0;// flush 之前缓存的 doc 数量

    boolean success = false;// 创建_0.fdx和_0.fdt文件
    try {
      metaStream =// fdt文件，合并时将产生RateLimitedIndexOutput
          directory.createOutput(
              IndexFileNames.segmentFileName(segment, segmentSuffix, META_EXTENSION), context);
      CodecUtil.writeIndexHeader(
          metaStream, INDEX_CODEC_NAME + "Meta", VERSION_CURRENT, si.getId(), segmentSuffix);
      assert CodecUtil.indexHeaderLength(INDEX_CODEC_NAME + "Meta", segmentSuffix)
          == metaStream.getFilePointer();

      fieldsStream =
          directory.createOutput(
              IndexFileNames.segmentFileName(segment, segmentSuffix, FIELDS_EXTENSION), context);
      CodecUtil.writeIndexHeader(
          fieldsStream, formatName, VERSION_CURRENT, si.getId(), segmentSuffix);
      assert CodecUtil.indexHeaderLength(formatName, segmentSuffix)
          == fieldsStream.getFilePointer();
      // 里面会建立俩新文件名_eg7_Lucene85FieldsIndex-doc_ids_0.tmp及_eg7_Lucene85FieldsIndexfile_pointers_1.tmp
      indexWriter = // 这俩文件在refresh前，都会创建好，等待124个doc的刷新
          new FieldsIndexWriter(
              directory,
              segment,
              segmentSuffix,
              INDEX_EXTENSION,
              INDEX_CODEC_NAME,
              si.getId(),
              blockShift,
              context);
      // 每个chunkSize的大小
      metaStream.writeVInt(chunkSize);// 写 fdt 文件 chunkSize 和 packedInts 版本号

      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(metaStream, fieldsStream, indexWriter);
      }
    }
  }

  @Override
  public void close() throws IOException {
    try {
      IOUtils.close(metaStream, fieldsStream, indexWriter, compressor);
    } finally {
      metaStream = null;
      fieldsStream = null; // 全部置空
      indexWriter = null;
      compressor = null;
    }
  }

  private int numStoredFieldsInDoc;// 这个文档有几个需要存储的。

  @Override
  public void startDocument() throws IOException {}
  // 写完一个文档后，就会调用
  @Override
  public void finishDocument() throws IOException {
    if (numBufferedDocs == this.numStoredFields.length) {// 需要扩容
      final int newLength = ArrayUtil.oversize(numBufferedDocs + 1, 4);
      this.numStoredFields = ArrayUtil.growExact(this.numStoredFields, newLength);
      endOffsets = ArrayUtil.growExact(endOffsets, newLength);
    }
    this.numStoredFields[numBufferedDocs] = numStoredFieldsInDoc; // 该doc有几个域需要存储
    numStoredFieldsInDoc = 0;
    endOffsets[numBufferedDocs] = Math.toIntExact(bufferedDocs.size());// 是把整个字段结束位置给存储起来，起始位置是从source部分开始计算的
    ++numBufferedDocs;
    if (triggerFlush()) { // 该bufferedDocs使用大小超过chunk限制16k了，或者该bufferedDocs存储文档书超过chunk限制128了
      flush(false);
    }
  }
  // 如何压缩存储int,全部存储到out中
  private static void saveInts(int[] values, int length, DataOutput out) throws IOException {
    if (length == 1) {
      out.writeVInt(values[0]);// 存储第一个
    } else {
      StoredFieldsInts.writeInts(values, 0, length, out);
    }
  }

  private void writeHeader(
      int docBase,
      int numBufferedDocs,
      int[] numStoredFields,
      int[] lengths,
      boolean sliced,
      boolean dirtyChunk)
      throws IOException {
    final int slicedBit = sliced ? 1 : 0;
    final int dirtyBit = dirtyChunk ? 2 : 0;
    // save docBase and numBufferedDocs
    fieldsStream.writeVInt(docBase);// 向fdt中写入当前chunk的起始位置
    fieldsStream.writeVInt((numBufferedDocs << 2) | dirtyBit | slicedBit); // 向fdt中写入该chunk的文档个数

    // save numStoredFields
    saveInts(numStoredFields, numBufferedDocs, fieldsStream); // 首先存储所有文件的numStoredFields

    // save lengths
    saveInts(lengths, numBufferedDocs, fieldsStream);// 存储了所有文档的字节长度
  }

  private boolean triggerFlush() {
    return bufferedDocs.size() >= chunkSize//大小超过了81kb
        || // chunks of at least chunkSize bytes
        numBufferedDocs >= maxDocsPerChunk; // 1024个文档
  }// flush主要还是在构建fdt数据结构， flush里面的writeBlock是在构建fdx数据结构
  // 只有当本类内存中缓存的个数或者大小超过81kb,或者1024个文档，在每个文档addDocument完成时都会检查这两项而刷新。当然flush、commit都会调用
  private void flush(boolean force) throws IOException {// 两个地方会调用：1. 每次写完128/16K文档时， flush阶段，若还有缓存文档，则会主动调用
    assert triggerFlush() != force;
    numChunks++;
    if (force) {
      numDirtyChunks++; // incomplete: we had to force this flush
      numDirtyDocs += numBufferedDocs;
    }//表示开始这个chunk了
    indexWriter.writeIndex(numBufferedDocs, fieldsStream.getFilePointer()); // 主要是向doc_ids和file_pointers中写入文档个数、fdt偏移量信息

    // transform end offsets into lengths
    final int[] lengths = endOffsets;// 不用计算第0个文档
    for (int i = numBufferedDocs - 1; i > 0; --i) {
      lengths[i] = endOffsets[i] - endOffsets[i - 1]; // 每个文档的长度
      assert lengths[i] >= 0;
    }
    final boolean sliced = bufferedDocs.size() >= 2L * chunkSize;// 若存储总长度大于82kb, 就需要分开压缩
    final boolean dirtyChunk = force;
    writeHeader(docBase, numBufferedDocs, numStoredFields, lengths, sliced, dirtyChunk);// 向fdt写入numStoredFields与lengths
    ByteBuffersDataInput bytebuffers = bufferedDocs.toDataInput();// 原始文档存放的地方
    // compress stored fields to fieldsStream.
    if (sliced) { // 若切分的话，就切分存储，一次写入不能超过chunk大小（82kb）
      // big chunk, slice it, using ByteBuffersDataInput ignore memory copy
      final int capacity = (int) bytebuffers.length();
      for (int compressed = 0; compressed < capacity; compressed += chunkSize) {
        int l = Math.min(chunkSize, capacity - compressed);// 按照slice存储
        ByteBuffersDataInput bbdi = bytebuffers.slice(compressed, l);
        compressor.compress(bbdi, fieldsStream);
      }
    } else {// 压缩 stored fields value 并写入文件，压缩是按 chunk 来压缩的
      compressor.compress(bytebuffers, fieldsStream);
    }

    // reset
    docBase += numBufferedDocs;// 记录所有chunk中已经包含的文档数
    numBufferedDocs = 0;
    bufferedDocs.reset();// 将原始文档释放没问题
  }

  @Override
  public void writeField(FieldInfo info, int value) throws IOException {
    ++numStoredFieldsInDoc;
    final long infoAndBits = (((long) info.number) << TYPE_BITS) | NUMERIC_INT; // 字段类型
    bufferedDocs.writeVLong(infoAndBits);
    bufferedDocs.writeZInt(value);
  }

  @Override
  public void writeField(FieldInfo info, long value) throws IOException {
    ++numStoredFieldsInDoc;
    final long infoAndBits = (((long) info.number) << TYPE_BITS) | NUMERIC_LONG;
    bufferedDocs.writeVLong(infoAndBits);
    writeTLong(bufferedDocs, value);
  }

  @Override
  public void writeField(FieldInfo info, float value) throws IOException {
    ++numStoredFieldsInDoc;
    final long infoAndBits = (((long) info.number) << TYPE_BITS) | NUMERIC_FLOAT;
    bufferedDocs.writeVLong(infoAndBits);
    writeZFloat(bufferedDocs, value);
  }

  @Override
  public void writeField(FieldInfo info, double value) throws IOException {
    ++numStoredFieldsInDoc;
    final long infoAndBits = (((long) info.number) << TYPE_BITS) | NUMERIC_DOUBLE;
    bufferedDocs.writeVLong(infoAndBits);
    writeZDouble(bufferedDocs, value);
  }

  @Override
  public void writeField(FieldInfo info, BytesRef value) throws IOException {
    ++numStoredFieldsInDoc;
    final long infoAndBits = (((long) info.number) << TYPE_BITS) | BYTE_ARR;
    bufferedDocs.writeVLong(infoAndBits); //写入字段编号，字段类型
    bufferedDocs.writeVInt(value.length);
    bufferedDocs.writeBytes(value.bytes, value.offset, value.length);
  }

  @Override
  public void writeField(FieldInfo info, StoredFieldDataInput value) throws IOException {
    int length = value.getLength();
    ++numStoredFieldsInDoc;
    final long infoAndBits = (((long) info.number) << TYPE_BITS) | BYTE_ARR; // 低三位代表类型，高5位代表字段编号
    bufferedDocs.writeVLong(infoAndBits); //写入字段编号，字段类型
    bufferedDocs.writeVInt(length);
    bufferedDocs.copyBytes(value.getDataInput(), length);
  }

  @Override
  public void writeField(FieldInfo info, String value) throws IOException {
    ++numStoredFieldsInDoc;
    final long infoAndBits = (((long) info.number) << TYPE_BITS) | STRING; // 低三位代表类型，高5位代表字段编号
    bufferedDocs.writeVLong(infoAndBits); //写入字段编号，字段类型
    bufferedDocs.writeString(value);
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
   * Writes a float in a variable-length format. Writes between one and five bytes. Small integral
   * values typically take fewer bytes.
   *
   * <p>ZFloat --&gt; Header, Bytes*?
   *
   * <ul>
   *   <li>Header --&gt; {@link DataOutput#writeByte Uint8}. When it is equal to 0xFF then the value
   *       is negative and stored in the next 4 bytes. Otherwise if the first bit is set then the
   *       other bits in the header encode the value plus one and no other bytes are read.
   *       Otherwise, the value is a positive float value whose first byte is the header, and 3
   *       bytes need to be read to complete it.
   *   <li>Bytes --&gt; Potential additional bytes to read depending on the header.
   * </ul>
   */
  static void writeZFloat(DataOutput out, float f) throws IOException {
    int intVal = (int) f;
    final int floatBits = Float.floatToIntBits(f);

    if (f == intVal && intVal >= -1 && intVal <= 0x7D && floatBits != NEGATIVE_ZERO_FLOAT) {
      // small integer value [-1..125]: single byte
      out.writeByte((byte) (0x80 | (1 + intVal)));
    } else if ((floatBits >>> 31) == 0) {
      // other positive floats: 4 bytes
      out.writeByte((byte) (floatBits >> 24));
      out.writeShort((short) (floatBits >>> 8));
      out.writeByte((byte) floatBits);
    } else {
      // other negative float: 5 bytes
      out.writeByte((byte) 0xFF);
      out.writeInt(floatBits);
    }
  }

  /**
   * Writes a float in a variable-length format. Writes between one and five bytes. Small integral
   * values typically take fewer bytes.
   *
   * <p>ZFloat --&gt; Header, Bytes*?
   *
   * <ul>
   *   <li>Header --&gt; {@link DataOutput#writeByte Uint8}. When it is equal to 0xFF then the value
   *       is negative and stored in the next 8 bytes. When it is equal to 0xFE then the value is
   *       stored as a float in the next 4 bytes. Otherwise if the first bit is set then the other
   *       bits in the header encode the value plus one and no other bytes are read. Otherwise, the
   *       value is a positive float value whose first byte is the header, and 7 bytes need to be
   *       read to complete it.
   *   <li>Bytes --&gt; Potential additional bytes to read depending on the header.
   * </ul>
   */
  static void writeZDouble(DataOutput out, double d) throws IOException {
    int intVal = (int) d;
    final long doubleBits = Double.doubleToLongBits(d);

    if (d == intVal && intVal >= -1 && intVal <= 0x7C && doubleBits != NEGATIVE_ZERO_DOUBLE) {
      // small integer value [-1..124]: single byte
      out.writeByte((byte) (0x80 | (intVal + 1)));
      return;
    } else if (d == (float) d) {
      // d has an accurate float representation: 5 bytes
      out.writeByte((byte) 0xFE);
      out.writeInt(Float.floatToIntBits((float) d));
    } else if ((doubleBits >>> 63) == 0) {
      // other positive doubles: 8 bytes
      out.writeByte((byte) (doubleBits >> 56));
      out.writeInt((int) (doubleBits >>> 24));
      out.writeShort((short) (doubleBits >>> 8));
      out.writeByte((byte) (doubleBits));
    } else {
      // other negative doubles: 9 bytes
      out.writeByte((byte) 0xFF);
      out.writeLong(doubleBits);
    }
  }

  /**
   * Writes a long in a variable-length format. Writes between one and ten bytes. Small values or
   * values representing timestamps with day, hour or second precision typically require fewer
   * bytes.
   *
   * <p>ZLong --&gt; Header, Bytes*?
   *
   * <ul>
   *   <li>Header --&gt; The first two bits indicate the compression scheme:
   *       <ul>
   *         <li>00 - uncompressed
   *         <li>01 - multiple of 1000 (second)
   *         <li>10 - multiple of 3600000 (hour)
   *         <li>11 - multiple of 86400000 (day)
   *       </ul>
   *       Then the next bit is a continuation bit, indicating whether more bytes need to be read,
   *       and the last 5 bits are the lower bits of the encoded value. In order to reconstruct the
   *       value, you need to combine the 5 lower bits of the header with a vLong in the next bytes
   *       (if the continuation bit is set to 1). Then {@link BitUtil#zigZagDecode(int)
   *       zigzag-decode} it and finally multiply by the multiple corresponding to the compression
   *       scheme.
   *   <li>Bytes --&gt; Potential additional bytes to read depending on the header.
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
  public void finish(int numDocs) throws IOException {
    if (numBufferedDocs > 0) {
      flush(true);
    } else {
      assert bufferedDocs.size() == 0;
    }
    if (docBase != numDocs) {
      throw new RuntimeException(
          "Wrote " + docBase + " docs, finish called with numDocs=" + numDocs);
    }
    indexWriter.finish(numDocs, fieldsStream.getFilePointer(), metaStream);// 仅仅向fdx中写入索引信息，处理 fdx 的 finish 流程，写 block， 写文件尾。
    metaStream.writeVLong(numChunks);
    metaStream.writeVLong(numDirtyChunks);
    metaStream.writeVLong(numDirtyDocs);
    CodecUtil.writeFooter(metaStream);
    CodecUtil.writeFooter(fieldsStream);
    assert bufferedDocs.size() == 0;
  }

  // bulk merge is scary: its caused corruption bugs in the past.
  // we try to be extra safe with this impl, but add an escape hatch to
  // have a workaround for undiscovered bugs.
  static final String BULK_MERGE_ENABLED_SYSPROP =
      Lucene90CompressingStoredFieldsWriter.class.getName() + ".enableBulkMerge";
  static final boolean BULK_MERGE_ENABLED;

  static {
    boolean v = true;
    try {
      v = Boolean.parseBoolean(System.getProperty(BULK_MERGE_ENABLED_SYSPROP, "true"));
    } catch (
        @SuppressWarnings("unused")
        SecurityException ignored) {
    }
    BULK_MERGE_ENABLED = v;
  }

  private void copyOneDoc(Lucene90CompressingStoredFieldsReader reader, int docID)
      throws IOException {
    assert reader.getVersion() == VERSION_CURRENT;
    SerializedDocument doc = reader.serializedDocument(docID);
    startDocument();
    bufferedDocs.copyBytes(doc.in, doc.length);
    numStoredFieldsInDoc = doc.numStoredFields;
    finishDocument();
  }

  private void copyChunks(
      final MergeState mergeState,
      final CompressingStoredFieldsMergeSub sub,
      final int fromDocID,
      final int toDocID)
      throws IOException {
    final Lucene90CompressingStoredFieldsReader reader =
        (Lucene90CompressingStoredFieldsReader) mergeState.storedFieldsReaders[sub.readerIndex];
    assert reader.getVersion() == VERSION_CURRENT;
    assert reader.getChunkSize() == chunkSize;
    assert reader.getCompressionMode() == compressionMode;
    assert !tooDirty(reader);
    assert mergeState.liveDocs[sub.readerIndex] == null;

    int docID = fromDocID;
    final FieldsIndex index = reader.getIndexReader();

    // copy docs that belong to the previous chunk
    while (docID < toDocID && reader.isLoaded(docID)) {
      copyOneDoc(reader, docID++);
    }
    if (docID >= toDocID) {
      return;
    }
    // copy chunks
    long fromPointer = index.getStartPointer(docID);
    final long toPointer =
        toDocID == sub.maxDoc ? reader.getMaxPointer() : index.getStartPointer(toDocID);
    if (fromPointer < toPointer) {
      if (numBufferedDocs > 0) {
        flush(true);
      }
      final IndexInput rawDocs = reader.getFieldsStream();// fdt文件
      rawDocs.seek(fromPointer);
      do {
        final int base = rawDocs.readVInt();// base没啥用
        final int code = rawDocs.readVInt();
        final int bufferedDocs = code >>> 2;
        if (base != docID) {
          throw new CorruptIndexException(
              "invalid state: base=" + base + ", docID=" + docID, rawDocs);
        }
        // write a new index entry and new header for this chunk.
        indexWriter.writeIndex(bufferedDocs, fieldsStream.getFilePointer());// 向doc,filepoint写入
        fieldsStream.writeVInt(docBase); // rebase  // rebase  自己维护一个segment类的全局的docBase
        fieldsStream.writeVInt(code);
        docID += bufferedDocs;
        docBase += bufferedDocs;
        if (docID > toDocID) {
          throw new CorruptIndexException(
              "invalid state: base=" + base + ", count=" + bufferedDocs + ", toDocID=" + toDocID,
              rawDocs);
        }
        // copy bytes until the next chunk boundary (or end of chunk data).
        // using the stored fields index for this isn't the most efficient, but fast enough
        // and is a source of redundancy for detecting bad things.
        final long endChunkPointer;// 这里有点错位不好理解：doc文件写入0,128,255,385这样的，而filepointer写入n1,n2,n3，实际chunk=1指的是doc中的128、filepointer中的n2
        if (docID == sub.maxDoc) {
          endChunkPointer = reader.getMaxPointer();
        } else {
          endChunkPointer = index.getStartPointer(docID);// 错位，实际效果找的是chunk+1在fdt中的起始位置。
        } // 发现这个操作也很耗时,从rawDocs读取数据
        fieldsStream.copyBytes(rawDocs, endChunkPointer - rawDocs.getFilePointer());
        ++numChunks;
        final boolean dirtyChunk = (code & 2) != 0;
        if (dirtyChunk) {
          assert bufferedDocs < maxDocsPerChunk;
          ++numDirtyChunks;
          numDirtyDocs += bufferedDocs;
        }
        fromPointer = endChunkPointer;
      } while (fromPointer < toPointer);
    }

    // copy leftover docs that don't form a complete chunk
    assert reader.isLoaded(docID) == false;
    while (docID < toDocID) {
      copyOneDoc(reader, docID++);
    }
  }
  // 将从ElasticsearchConcurrentMergeScheduler.doMerge()->ConcurrentMergeScheduler.doMerge()->IndexWriter.merge()->IndexWriter.mergeMiddle()
  @Override// 该函数接受一个 MergeState mergeState 对象。其包含了merge 需要的各个 segment 文件的 store field reader 对象
  public int merge(MergeState mergeState) throws IOException {
    final MatchingReaders matchingReaders = new MatchingReaders(mergeState);
    final MergeVisitor[] visitors = new MergeVisitor[mergeState.storedFieldsReaders.length];// 多少个semgent来合并
    final List<CompressingStoredFieldsMergeSub> subs =
        new ArrayList<>(mergeState.storedFieldsReaders.length);
    for (int i = 0; i < mergeState.storedFieldsReaders.length; i++) { // 遍历每一个storedFieldsReaders文件
      final StoredFieldsReader reader = mergeState.storedFieldsReaders[i];
      mergeState.checkAborted();
      reader.checkIntegrity();
      MergeStrategy mergeStrategy = getMergeStrategy(mergeState, matchingReaders, i);
      if (mergeStrategy == MergeStrategy.VISITOR) {
        visitors[i] = new MergeVisitor(mergeState, i);
      }
      subs.add(new CompressingStoredFieldsMergeSub(mergeState, mergeStrategy, i));
    }
    int docCount = 0; // 变量是所有segment中全局的
    final DocIDMerger<CompressingStoredFieldsMergeSub> docIDMerger =
        DocIDMerger.of(subs, mergeState.needsIndexSort);
    CompressingStoredFieldsMergeSub sub = docIDMerger.next();
    while (sub != null) {// 开始遍历了，
      assert sub.mappedDocID == docCount : sub.mappedDocID + " != " + docCount;
      final StoredFieldsReader reader = mergeState.storedFieldsReaders[sub.readerIndex];
      if (sub.mergeStrategy == MergeStrategy.BULK) {
        final int fromDocID = sub.docID;
        int toDocID = fromDocID;
        final CompressingStoredFieldsMergeSub current = sub;
        while ((sub = docIDMerger.next()) == current) {
          ++toDocID;
          assert sub.docID == toDocID;
        }
        ++toDocID; // exclusive bound
        copyChunks(mergeState, current, fromDocID, toDocID);
        docCount += (toDocID - fromDocID);
      } else if (sub.mergeStrategy == MergeStrategy.DOC) { // 或者有删除文档、或者有太多脏chunk
        copyOneDoc((Lucene90CompressingStoredFieldsReader) reader, sub.docID);
        ++docCount;
        sub = docIDMerger.next();
      } else if (sub.mergeStrategy == MergeStrategy.VISITOR) {
        assert visitors[sub.readerIndex] != null;
        startDocument();
        reader.document(sub.docID, visitors[sub.readerIndex]);
        finishDocument();
        ++docCount;
        sub = docIDMerger.next();
      } else {
        throw new AssertionError("Unknown merge strategy [" + sub.mergeStrategy + "]");
      }
    }
    finish(docCount);// 其实这里刷新意义不大，是不是可以最后统一刷新就好了？
    return docCount;
  }

  /**
   * Returns true if we should recompress this reader, even though we could bulk merge compressed
   * data
   *
   * <p>The last chunk written for a segment is typically incomplete, so without recompressing, in
   * some worst-case situations (e.g. frequent reopen with tiny flushes), over time the compression
   * ratio can degrade. This is a safety switch.
   */// 因为太多脏chunk,某些情况下，压缩效率会降低
  boolean tooDirty(Lucene90CompressingStoredFieldsReader candidate) {
    // A segment is considered dirty only if it has enough dirty docs to make a full block
    // AND more than 1% blocks are dirty.
    return candidate.getNumDirtyDocs() > maxDocsPerChunk
        && candidate.getNumDirtyChunks() * 100 > candidate.getNumChunks();
  }

  private enum MergeStrategy {
    /** Copy chunk by chunk in a compressed format */
    BULK,

    /** Copy document by document in a decompressed format */
    DOC,

    /** Copy field by field of decompressed documents */
    VISITOR
  }

  private MergeStrategy getMergeStrategy(
      MergeState mergeState, MatchingReaders matchingReaders, int readerIndex) {
    final StoredFieldsReader candidate = mergeState.storedFieldsReaders[readerIndex];
    if (matchingReaders.matchingReaders[readerIndex] == false
        || candidate instanceof Lucene90CompressingStoredFieldsReader == false
        || ((Lucene90CompressingStoredFieldsReader) candidate).getVersion() != VERSION_CURRENT) {
      return MergeStrategy.VISITOR;
    }
    Lucene90CompressingStoredFieldsReader reader =
        (Lucene90CompressingStoredFieldsReader) candidate;
    if (BULK_MERGE_ENABLED
        && reader.getCompressionMode() == compressionMode
        && reader.getChunkSize() == chunkSize
        // its not worth fine-graining this if there are deletions.
        && mergeState.liveDocs[readerIndex] == null // 没有删除的话
        && !tooDirty(reader)) {
      return MergeStrategy.BULK;
    } else {
      return MergeStrategy.DOC;
    }
  }

  private static class CompressingStoredFieldsMergeSub extends DocIDMerger.Sub {
    private final int readerIndex;
    private final int maxDoc;
    private final MergeStrategy mergeStrategy;
    int docID = -1;

    CompressingStoredFieldsMergeSub(
        MergeState mergeState, MergeStrategy mergeStrategy, int readerIndex) {
      super(mergeState.docMaps[readerIndex]);
      this.readerIndex = readerIndex;
      this.mergeStrategy = mergeStrategy;
      this.maxDoc = mergeState.maxDocs[readerIndex];
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

  @Override
  public long ramBytesUsed() {
    return bufferedDocs.ramBytesUsed()
        + numStoredFields.length * (long) Integer.BYTES
        + endOffsets.length * (long) Integer.BYTES;
  }
}
