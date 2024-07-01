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

import static org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressingStoredFieldsWriter.BYTE_ARR;
import static org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressingStoredFieldsWriter.DAY;
import static org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressingStoredFieldsWriter.DAY_ENCODING;
import static org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressingStoredFieldsWriter.FIELDS_EXTENSION;
import static org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressingStoredFieldsWriter.HOUR;
import static org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressingStoredFieldsWriter.HOUR_ENCODING;
import static org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressingStoredFieldsWriter.INDEX_CODEC_NAME;
import static org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressingStoredFieldsWriter.INDEX_EXTENSION;
import static org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressingStoredFieldsWriter.META_EXTENSION;
import static org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressingStoredFieldsWriter.META_VERSION_START;
import static org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressingStoredFieldsWriter.NUMERIC_DOUBLE;
import static org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressingStoredFieldsWriter.NUMERIC_FLOAT;
import static org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressingStoredFieldsWriter.NUMERIC_INT;
import static org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressingStoredFieldsWriter.NUMERIC_LONG;
import static org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressingStoredFieldsWriter.SECOND;
import static org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressingStoredFieldsWriter.SECOND_ENCODING;
import static org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressingStoredFieldsWriter.STRING;
import static org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressingStoredFieldsWriter.TYPE_BITS;
import static org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressingStoredFieldsWriter.TYPE_MASK;
import static org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressingStoredFieldsWriter.VERSION_CURRENT;
import static org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressingStoredFieldsWriter.VERSION_START;

import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.compressing.Decompressor;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.StoredFieldDataInput;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FileTypeHint;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LongsRef;

/**
 * {@link StoredFieldsReader} impl for {@link Lucene90CompressingStoredFieldsFormat}.
 *
 * @lucene.experimental
 */
public final class Lucene90CompressingStoredFieldsReader extends StoredFieldsReader {

  private static final int PREFETCH_CACHE_SIZE = 1 << 4;
  private static final int PREFETCH_CACHE_MASK = PREFETCH_CACHE_SIZE - 1;

  private final int version;
  private final FieldInfos fieldInfos;
  private final FieldsIndex indexReader; // 引用较大内存资源,是FieldsIndexReader，fdx
  private final long maxPointer; // 所有chunk写完时的fdt中的位置（再后面就是footer部分了）
  private final IndexInput fieldsStream;// 也是用mmap的打开fdt文件, ByteBufferIndexInput$SingleBufferImpl
  private final int chunkSize;
  private final CompressionMode compressionMode;
  private final Decompressor decompressor;
  private final int numDocs;// 这个segment文档个数
  private final boolean merging;// 是否用于merge
  private final BlockState state; // 干嘛的
  private final long numChunks; // number of written blocks
  private final long numDirtyChunks; // number of incomplete compressed blocks written
  private final long numDirtyDocs; // cumulative number of docs in incomplete chunks
  // Cache of recently prefetched block IDs. This helps reduce chances of prefetching the same block
  // multiple times, which is otherwise likely due to index sorting or recursive graph bisection
  // clustering similar documents together. NOTE: this cache must be small since it's fully scanned.
  private final long[] prefetchedBlockIDCache; // 缓存的预读块，是一个循环块
  private int prefetchedBlockIDCacheIndex;
  private boolean closed;
  // 每个线程读取的时候，都是对这个对这个对象的clone，是非常安全的线程安全操作
  // used by clone
  private Lucene90CompressingStoredFieldsReader(
      Lucene90CompressingStoredFieldsReader reader, boolean merging) {
    this.version = reader.version;
    this.fieldInfos = reader.fieldInfos;
    this.fieldsStream = reader.fieldsStream.clone();// fdt 用的时候都会clone的
    this.indexReader = reader.indexReader.clone(); // 用的时候都会clone的
    this.maxPointer = reader.maxPointer;
    this.chunkSize = reader.chunkSize;
    this.compressionMode = reader.compressionMode;
    this.decompressor = reader.decompressor.clone();
    this.numDocs = reader.numDocs;
    this.numChunks = reader.numChunks;
    this.numDirtyChunks = reader.numDirtyChunks;
    this.numDirtyDocs = reader.numDirtyDocs;
    this.prefetchedBlockIDCache = new long[PREFETCH_CACHE_SIZE];
    Arrays.fill(prefetchedBlockIDCache, -1);
    this.merging = merging;
    this.state = new BlockState();
    this.closed = false;
  } // http://lucene.apache.org/core/8_2_0/core/org/apache/lucene/codecs/lucene50/Lucene50StoredFieldsFormat.html
  // 分别读取fdx和fdt文件的数据
  /** Sole constructor. */
  public Lucene90CompressingStoredFieldsReader(
      Directory d,
      SegmentInfo si,
      String segmentSuffix,
      FieldInfos fn,
      IOContext context,
      String formatName,
      CompressionMode compressionMode)
      throws IOException {
    this.compressionMode = compressionMode;
    final String segment = si.name;
    boolean success = false;
    fieldInfos = fn;
    numDocs = si.maxDoc();

    final String fieldsStreamFN =// 读取的fdt文件
        IndexFileNames.segmentFileName(segment, segmentSuffix, FIELDS_EXTENSION);
    ChecksumIndexInput metaIn = null;
    try {
      // Open the data file
      fieldsStream =
          d.openInput(fieldsStreamFN, context.withHints(FileTypeHint.DATA, DataAccessHint.RANDOM));// 会去读取fdt文件
      version =
          CodecUtil.checkIndexHeader(
              fieldsStream, formatName, VERSION_START, VERSION_CURRENT, si.getId(), segmentSuffix);
      assert CodecUtil.indexHeaderLength(formatName, segmentSuffix)
          == fieldsStream.getFilePointer();

      final String metaStreamFN =
          IndexFileNames.segmentFileName(segment, segmentSuffix, META_EXTENSION);
      metaIn = d.openChecksumInput(metaStreamFN);
      CodecUtil.checkIndexHeader(
          metaIn,
          INDEX_CODEC_NAME + "Meta",
          META_VERSION_START,
          version,
          si.getId(),
          segmentSuffix);

      chunkSize = metaIn.readVInt();

      decompressor = compressionMode.newDecompressor();
      this.prefetchedBlockIDCache = new long[PREFETCH_CACHE_SIZE];
      Arrays.fill(prefetchedBlockIDCache, -1);
      this.merging = false;
      this.state = new BlockState();

      // NOTE: data file is too costly to verify checksum against all the bytes on open,
      // but for now we at least verify proper structure of the checksum footer: which looks
      // for FOOTER_MAGIC + algorithmID. This is cheap and can detect some forms of corruption
      // such as file truncation.
      CodecUtil.retrieveChecksum(fieldsStream);

      long maxPointer = -1;
      FieldsIndex indexReader = null;

      FieldsIndexReader fieldsIndexReader =
          new FieldsIndexReader(
              d,
              si.name,
              segmentSuffix,
              INDEX_EXTENSION,
              INDEX_CODEC_NAME,
              si.getId(),
              metaIn,
              context);
      indexReader = fieldsIndexReader;
      maxPointer = fieldsIndexReader.getMaxPointer();

      this.maxPointer = maxPointer;
      this.indexReader = indexReader;

      numChunks = metaIn.readVLong();
      numDirtyChunks = metaIn.readVLong();
      numDirtyDocs = metaIn.readVLong();

      if (numChunks < numDirtyChunks) {
        throw new CorruptIndexException(
            "Cannot have more dirty chunks than chunks: numChunks="
                + numChunks
                + ", numDirtyChunks="
                + numDirtyChunks,
            metaIn);
      }
      if ((numDirtyChunks == 0) != (numDirtyDocs == 0)) {
        throw new CorruptIndexException(
            "Cannot have dirty chunks without dirty docs or vice-versa: numDirtyChunks="
                + numDirtyChunks
                + ", numDirtyDocs="
                + numDirtyDocs,
            metaIn);
      }
      if (numDirtyDocs < numDirtyChunks) {
        throw new CorruptIndexException(
            "Cannot have more dirty chunks than documents within dirty chunks: numDirtyChunks="
                + numDirtyChunks
                + ", numDirtyDocs="
                + numDirtyDocs,
            metaIn);
      }

      CodecUtil.checkFooter(metaIn, null);
      metaIn.close();

      success = true;
    } catch (Throwable t) {
      if (metaIn != null) {
        CodecUtil.checkFooter(metaIn, t);
        throw new AssertionError("unreachable");
      } else {
        throw t;
      }
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(this, metaIn);
      }
    }
  }

  /**
   * @throws AlreadyClosedException if this FieldsReader is closed
   */
  private void ensureOpen() throws AlreadyClosedException {
    if (closed) {
      throw new AlreadyClosedException("this FieldsReader is closed");
    }
  }

  /** Close the underlying {@link IndexInput}s. */
  @Override
  public void close() throws IOException {
    if (!closed) {
      IOUtils.close(indexReader, fieldsStream);
      closed = true;
    }
  }

  private static void readField(DataInput in, StoredFieldVisitor visitor, FieldInfo info, int bits)
      throws IOException {
    switch (bits & TYPE_MASK) {// get source部分
      case BYTE_ARR:
        int length = in.readVInt();
        visitor.binaryField(info, new StoredFieldDataInput(in, length));
        break;
      case STRING:
        visitor.stringField(info, in.readString());
        break;
      case NUMERIC_INT:
        visitor.intField(info, in.readZInt());
        break;
      case NUMERIC_FLOAT:
        visitor.floatField(info, readZFloat(in));
        break;
      case NUMERIC_LONG:
        visitor.longField(info, readTLong(in));
        break;
      case NUMERIC_DOUBLE:
        visitor.doubleField(info, readZDouble(in));
        break;
      default:
        throw new AssertionError("Unknown type flag: " + Integer.toHexString(bits));
    }
  }

  private static void skipField(DataInput in, int bits) throws IOException {
    switch (bits & TYPE_MASK) {
      case BYTE_ARR:
      case STRING:
        final int length = in.readVInt();
        in.skipBytes(length);
        break;
      case NUMERIC_INT:
        in.readZInt();
        break;
      case NUMERIC_FLOAT:
        readZFloat(in);
        break;
      case NUMERIC_LONG:
        readTLong(in);
        break;
      case NUMERIC_DOUBLE:
        readZDouble(in);
        break;
      default:
        throw new AssertionError("Unknown type flag: " + Integer.toHexString(bits));
    }
  }

  /**
   * Reads a float in a variable-length format. Reads between one and five bytes. Small integral
   * values typically take fewer bytes.
   */
  static float readZFloat(DataInput in) throws IOException {
    int b = in.readByte() & 0xFF;
    if (b == 0xFF) {
      // negative value
      return Float.intBitsToFloat(in.readInt());
    } else if ((b & 0x80) != 0) {
      // small integer [-1..125]
      return (b & 0x7f) - 1;
    } else {
      // positive float
      int bits = b << 24 | ((in.readShort() & 0xFFFF) << 8) | (in.readByte() & 0xFF);
      return Float.intBitsToFloat(bits);
    }
  }

  /**
   * Reads a double in a variable-length format. Reads between one and nine bytes. Small integral
   * values typically take fewer bytes.
   */
  static double readZDouble(DataInput in) throws IOException {
    int b = in.readByte() & 0xFF;
    if (b == 0xFF) {
      // negative value
      return Double.longBitsToDouble(in.readLong());
    } else if (b == 0xFE) {
      // float
      return Float.intBitsToFloat(in.readInt());
    } else if ((b & 0x80) != 0) {
      // small integer [-1..124]
      return (b & 0x7f) - 1;
    } else {
      // positive double
      long bits =
          ((long) b) << 56
              | ((in.readInt() & 0xFFFFFFFFL) << 24)
              | ((in.readShort() & 0xFFFFL) << 8)
              | (in.readByte() & 0xFFL);
      return Double.longBitsToDouble(bits);
    }
  }

  /**
   * Reads a long in a variable-length format. Reads between one andCorePropLo nine bytes. Small
   * values typically take fewer bytes.
   */
  static long readTLong(DataInput in) throws IOException {
    int header = in.readByte() & 0xFF;

    long bits = header & 0x1F;
    if ((header & 0x20) != 0) {
      // continuation bit
      bits |= in.readVLong() << 5;
    }

    long l = BitUtil.zigZagDecode(bits);

    switch (header & DAY_ENCODING) {
      case SECOND_ENCODING:
        l *= SECOND;
        break;
      case HOUR_ENCODING:
        l *= HOUR;
        break;
      case DAY_ENCODING:
        l *= DAY;
        break;
      case 0:
        // uncompressed
        break;
      default:
        throw new AssertionError();
    }

    return l;
  }

  /**
   * A serialized document, you need to decode its input in order to get an actual {@link Document}.
   */
  static class SerializedDocument {

    // the serialized data
    final DataInput in;

    // the number of bytes on which the document is encoded
    final int length;

    // the number of stored fields
    final int numStoredFields;

    private SerializedDocument(DataInput in, int length, int numStoredFields) {
      this.in = in;
      this.length = length;
      this.numStoredFields = numStoredFields;
    }
  }

  /** Keeps state about the current block of documents. */// 存储当前读取chunk的信息
  private class BlockState {
    // 当前chunk起始位置
    private int docBase, chunkDocs; // 这个chunk的起始位置，包含的文档数

    // whether the block has been sliced, this happens for large documents
    private boolean sliced;

    private long[] offsets = LongsRef.EMPTY_LONGS;// 这个chunk每个文档在fdt中存储source部分的偏移量（从source部分开始计算的）
    private long[] numStoredFields = LongsRef.EMPTY_LONGS;

    // the start pointer at which you can read the compressed documents
    private long startPointer;  // 这个docID所在的chunk在fdt中的起始位置

    private final BytesRef spare;// 仅仅在merge期间用，读取每个chunk的内容
    private final BytesRef bytes; // 读取出来解压后存放的地方

    BlockState() {
      if (merging) {
        spare = new BytesRef();
        bytes = new BytesRef();
      } else {
        spare = bytes = null;
      }
    }

    boolean contains(int docID) {
      return docID >= docBase && docID < docBase + chunkDocs;
    }

    /** Reset this block so that it stores state for the block that contains the given doc id. */
    void reset(int docID) throws IOException {
      boolean success = false;
      try {
        doReset(docID); //读取这个docID所在chunk的所有原始数据
        success = true;
      } finally {
        if (success == false) {
          // if the read failed, set chunkDocs to 0 so that it does not
          // contain any docs anymore and is not reused. This should help
          // get consistent exceptions when trying to get several
          // documents which are in the same corrupted block since it will
          // force the header to be decoded again
          chunkDocs = 0;
        }
      }
    }

    private void doReset(int docID) throws IOException {//已经跑到这个chunk所在的起始位置
      docBase = fieldsStream.readVInt();//记录当前chunk在整个segment的起始docId下标。
      final int token = fieldsStream.readVInt();
      chunkDocs = token >>> 2;
      if (contains(docID) == false || docBase + chunkDocs > numDocs) {
        throw new CorruptIndexException(
            "Corrupted: docID="
                + docID
                + ", docBase="
                + docBase
                + ", chunkDocs="
                + chunkDocs
                + ", numDocs="
                + numDocs,
            fieldsStream);
      }

      sliced = (token & 1) != 0;// 是否压缩了

      offsets = ArrayUtil.growNoCopy(offsets, chunkDocs + 1);
      numStoredFields = ArrayUtil.growNoCopy(numStoredFields, chunkDocs); // 每个chunk每个文档的storeField个数

      if (chunkDocs == 1) {
        numStoredFields[0] = fieldsStream.readVInt();
        offsets[1] = fieldsStream.readVInt();
      } else {
        // Number of stored fields per document
        StoredFieldsInts.readInts(fieldsStream, chunkDocs, numStoredFields, 0); // 读取全量numStoredFields
        // The stream encodes the length of each document and we decode
        // it into a list of monotonically increasing offsets
        StoredFieldsInts.readInts(fieldsStream, chunkDocs, offsets, 1);// 从fdt中呢读取
        for (int i = 0; i < chunkDocs; ++i) {
          offsets[i + 1] += offsets[i]; // 这个chunk每个文档在fdt中存储source部分的偏移量（从source部分开始计算的）
        }

        // Additional validation: only the empty document has a serialized length of 0
        for (int i = 0; i < chunkDocs; ++i) {//
          final long len = offsets[i + 1] - offsets[i];// 每个文档的长度
          final long storedFields = numStoredFields[i];
          if ((len == 0) != (storedFields == 0)) {
            throw new CorruptIndexException(
                "length=" + len + ", numStoredFields=" + storedFields, fieldsStream);
          }
        }
      }
       // 开始获取这个docID所在的chcnk在fdt中存储source部分的起始位置
      startPointer = fieldsStream.getFilePointer();

      if (merging) {// 提前读取这个chunk未压缩的长度，SequentialStoredFieldsLeafReader。 merge的优化时：reset时就解压开整个chunk的内容，然后多次doc内容时都直接使用同一个bytes.bytes
        final int totalLength = Math.toIntExact(offsets[chunkDocs]);// 这个chunk的总长度
        // decompress eagerly
        if (sliced) {
          bytes.offset = bytes.length = 0;//开头全部为0
          for (int decompressed = 0; decompressed < totalLength; ) {    //  从头读取这个chunk：从第一个slice的开头读取，跨越多个slice直到读完这个文档所在偏移量
            final int toDecompress = Math.min(totalLength - decompressed, chunkSize);// 读取未压缩前的长度
            decompressor.decompress(fieldsStream, toDecompress, 0, toDecompress, spare);// 里面会置位spare的offset
            bytes.bytes = ArrayUtil.grow(bytes.bytes, bytes.length + spare.length);
            System.arraycopy(spare.bytes, spare.offset, bytes.bytes, bytes.length, spare.length);
            bytes.length += spare.length;
            decompressed += toDecompress;
          }
        } else {
          decompressor.decompress(fieldsStream, totalLength, 0, totalLength, bytes);
        }
        if (bytes.length != totalLength) {
          throw new CorruptIndexException(
              "Corrupted: expected chunk size = " + totalLength + ", got " + bytes.length,
              fieldsStream);
        }
      }
    }

    /**
     * Get the serialized representation of the given docID. This docID has to be contained in the
     * current block.
     */// 首先加载这个doc所在chunk的原始值，然后第二步骤再去读取原始值
    SerializedDocument document(int docID) throws IOException {
      if (contains(docID) == false) {// 若发现docId不在当前block内
        throw new IllegalArgumentException();
      }
// 读取这个docId的原始数据
      final int index = docID - docBase;
      final int offset = Math.toIntExact(offsets[index]);//文档在未压缩字节流的起始位置
      final int length = Math.toIntExact(offsets[index + 1]) - offset;//文档在未压缩字节流的长度
      final int totalLength = Math.toIntExact(offsets[chunkDocs]);//未压缩时，这个chunk的总长度
      final int numStoredFields = Math.toIntExact(this.numStoredFields[index]);

      final BytesRef bytes;
      if (merging) {
        bytes = this.bytes;
      } else {
        bytes = new BytesRef();
      }

      final DataInput documentInput;
      if (length == 0) {
        // empty
        documentInput = new ByteArrayDataInput();
      } else if (merging) {// 如果时mergeing的话，在前面已经解压好了。es也做优化了，若连续doc的话，就使用这里的优化，
        // already decompressed 在fetchPhase时，若检测文档连续，则使用SequentialStoredFieldsLeafReader，假装merge。merge的优化时：reset时就解压开整个chunk的内容，然后多次doc都直接使用同一个bytes.bytes
        documentInput = new ByteArrayDataInput(bytes.bytes, bytes.offset + offset, length);
      } else if (sliced) {
        fieldsStream.seek(startPointer);// 定位到文档对应chunk所在fdt的起始位置
        decompressor.decompress(// 先解析一个chunk的slice
            fieldsStream, chunkSize, offset, Math.min(length, chunkSize - offset), bytes);
        documentInput =
            new DataInput() {

              int decompressed = bytes.length;// 存放的是解压后的数据长度

              void fillBuffer() throws IOException {
                assert decompressed <= length;
                if (decompressed == length) {
                  throw new EOFException();
                }
                final int toDecompress = Math.min(length - decompressed, chunkSize);
                decompressor.decompress(fieldsStream, toDecompress, 0, toDecompress, bytes);
                decompressed += toDecompress;
              }

              @Override
              public byte readByte() throws IOException {
                if (bytes.length == 0) {
                  fillBuffer();
                }
                --bytes.length;
                return bytes.bytes[bytes.offset++];
              }

              @Override
              public void readBytes(byte[] b, int offset, int len) throws IOException {
                while (len > bytes.length) {// 若超过可用长度
                  System.arraycopy(bytes.bytes, bytes.offset, b, offset, bytes.length);
                  len -= bytes.length;
                  offset += bytes.length;
                  fillBuffer();
                }
                System.arraycopy(bytes.bytes, bytes.offset, b, offset, len);
                bytes.offset += len;
                bytes.length -= len;
              }

              @Override
              public void skipBytes(long numBytes) throws IOException {
                if (numBytes < 0) {
                  throw new IllegalArgumentException("numBytes must be >= 0, got " + numBytes);
                }
                while (numBytes > bytes.length) {
                  numBytes -= bytes.length;
                  fillBuffer();// 不停的解析读取
                }
                bytes.offset += numBytes;
                bytes.length -= numBytes;
              }
            };
      } else {
        fieldsStream.seek(startPointer);// // 这个docID所在的chunk在fdt中的起始位置。若多个文档在同一个block的话，就直接startPointer了
        decompressor.decompress(fieldsStream, totalLength, offset, length, bytes);
        assert bytes.length == length;
        documentInput = new ByteArrayDataInput(bytes.bytes, bytes.offset, bytes.length);// 对应的二进制码
      }

      return new SerializedDocument(documentInput, length, numStoredFields);
    }
  }

  @Override
  public void prefetch(int docID) throws IOException {// 预读取
    final long blockID = indexReader.getBlockID(docID);

    for (long prefetchedBlockID : prefetchedBlockIDCache) {
      if (prefetchedBlockID == blockID) {
        return;
      }
    }

    final long blockStartPointer = indexReader.getBlockStartPointer(blockID);
    final long blockLength = indexReader.getBlockLength(blockID);
    fieldsStream.prefetch(blockStartPointer, blockLength);

    prefetchedBlockIDCache[prefetchedBlockIDCacheIndex++ & PREFETCH_CACHE_MASK] = blockID;
  }

  SerializedDocument serializedDocument(int docID) throws IOException {
    if (state.contains(docID) == false) {
      fieldsStream.seek(indexReader.getStartPointer(docID));// 定位fdt跑到这个docId所在chunk的起始位置（是存放的数据）
      state.reset(docID);
    }
    assert state.contains(docID);
    return state.document(docID);
  }

  /** Checks if a given docID was loaded in the current block state. */
  boolean isLoaded(int docID) {
    if (merging == false) {
      throw new IllegalStateException("isLoaded should only ever get called on a merge instance");
    }
    if (version != VERSION_CURRENT) {
      throw new IllegalStateException(
          "isLoaded should only ever get called when the reader is on the current version");
    }
    return state.contains(docID);
  }

  @Override
  public void document(int docID, StoredFieldVisitor visitor) throws IOException {

    final SerializedDocument doc = serializedDocument(docID); // 获取具体文件内容

    for (int fieldIDX = 0; fieldIDX < doc.numStoredFields; fieldIDX++) {// 读取每一个字段值
      final long infoAndBits = doc.in.readVLong();
      final int fieldNumber = (int) (infoAndBits >>> TYPE_BITS);
      final FieldInfo fieldInfo = fieldInfos.fieldInfo(fieldNumber);

      final int bits = (int) (infoAndBits & TYPE_MASK);
      assert bits <= NUMERIC_DOUBLE : "bits=" + Integer.toHexString(bits);

      switch (visitor.needsField(fieldInfo)) {//读取一个
        case YES:
          readField(doc.in, visitor, fieldInfo, bits);// get source部分
          break;
        case NO:
          if (fieldIDX
              == doc.numStoredFields - 1) { // don't skipField on last field value; treat like STOP
            return;
          }
          skipField(doc.in, bits);
          break;
        case STOP:
          return;
      }
    }
  }

  @Override
  public StoredFieldsReader clone() {// 每次读取的是否，都是对这个对象的clone
    ensureOpen();
    return new Lucene90CompressingStoredFieldsReader(this, false);
  }

  @Override
  public StoredFieldsReader getMergeInstance() {
    ensureOpen();
    return new Lucene90CompressingStoredFieldsReader(this, true);//
  }

  int getVersion() {
    return version;
  }

  CompressionMode getCompressionMode() {
    return compressionMode;
  }

  FieldsIndex getIndexReader() {
    return indexReader;
  }

  long getMaxPointer() {
    return maxPointer;
  }

  IndexInput getFieldsStream() {
    return fieldsStream; // 读取fdt文件
  }

  int getChunkSize() {
    return chunkSize;
  }

  long getNumDirtyDocs() {
    if (version != VERSION_CURRENT) {
      throw new IllegalStateException(
          "getNumDirtyDocs should only ever get called when the reader is on the current version");
    }
    assert numDirtyDocs >= 0;
    return numDirtyDocs;
  }

  long getNumDirtyChunks() {
    if (version != VERSION_CURRENT) {
      throw new IllegalStateException(
          "getNumDirtyChunks should only ever get called when the reader is on the current version");
    }
    assert numDirtyChunks >= 0;
    return numDirtyChunks;
  }

  long getNumChunks() {
    if (version != VERSION_CURRENT) {
      throw new IllegalStateException(
          "getNumChunks should only ever get called when the reader is on the current version");
    }
    assert numChunks >= 0;
    return numChunks;
  }

  int getNumDocs() {
    return numDocs;
  }

  @Override
  public void checkIntegrity() throws IOException {
    indexReader.checkIntegrity();//FieldsIndexReader,会去校验fdx，挑的是fdx文件，文件大小占比不到0.1%，占比非常小
    CodecUtil.checksumEntireFile(fieldsStream);// 会去校验fdt,也是逐个读取出来（fdt有多大，就读取多少），效率明显很低。
  }

  @Override
  public String toString() {
    return getClass().getSimpleName()
        + "(mode="
        + compressionMode
        + ",chunksize="
        + chunkSize
        + ")";
  }
}
