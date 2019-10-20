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

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.packed.DirectMonotonicWriter;

// fdt以chunk为单位
/**
 * A {@link StoredFieldsFormat} that compresses documents in chunks in
 * order to improve the compression ratio.
 * <p>
 * For a chunk size of <tt>chunkSize</tt> bytes, this {@link StoredFieldsFormat}
 * does not support documents larger than (<tt>2<sup>31</sup> - chunkSize</tt>)
 * bytes.
 * <p>
 * For optimal performance, you should use a {@link MergePolicy} that returns
 * segments that have the biggest byte size first.
 * @lucene.experimental
 */ // 存放的字段的配置信息，比如字段是否分词，索引，存储等(在写入，merge的时候都会动态生成)
public class CompressingStoredFieldsFormat extends StoredFieldsFormat {
  // fdt以chunk为单位， Fdx 文件会在逻辑上切分多个 chunk 为一个 block，一个 block 最多包含1024个 chunk。这样切分便于快速定位一个文档所在的 chunk 位置，准确的解压对应的 chunk 并提取文档的 store fields
  private final String formatName;  // Lucene50StoredFieldsFormat
  private final String segmentSuffix;
  private final CompressionMode compressionMode;
  private final int chunkSize;// 一个chunk最大的大小，此种模式，为 16kb
  private final int maxDocsPerChunk;// 每个chunk包含的最多文档数 // 最多128个文档
  private final int blockShift; // 一个block最多由多少个chunk组成1024个

  /**
   * Create a new {@link CompressingStoredFieldsFormat} with an empty segment 
   * suffix.
   * 
   * @see CompressingStoredFieldsFormat#CompressingStoredFieldsFormat(String, String, CompressionMode, int, int, int)
   */
  public CompressingStoredFieldsFormat(String formatName, CompressionMode compressionMode, int chunkSize, int maxDocsPerChunk, int blockShift) {
    this(formatName, "", compressionMode, chunkSize, maxDocsPerChunk, blockShift);
  }
  
  /**
   * Create a new {@link CompressingStoredFieldsFormat}.
   * <p>
   * <code>formatName</code> is the name of the format. This name will be used
   * in the file formats to perform
   * {@link CodecUtil#checkIndexHeader codec header checks}.
   * <p>
   * <code>segmentSuffix</code> is the segment suffix. This suffix is added to 
   * the result file name only if it's not the empty string.
   * <p>
   * The <code>compressionMode</code> parameter allows you to choose between
   * compression algorithms that have various compression and decompression
   * speeds so that you can pick the one that best fits your indexing and
   * searching throughput. You should never instantiate two
   * {@link CompressingStoredFieldsFormat}s that have the same name but
   * different {@link CompressionMode}s.
   * <p>
   * <code>chunkSize</code> is the minimum byte size of a chunk of documents.
   * A value of <code>1</code> can make sense if there is redundancy across
   * fields.
   * <code>maxDocsPerChunk</code> is an upperbound on how many docs may be stored
   * in a single chunk. This is to bound the cpu costs for highly compressible data.
   * <p>
   * Higher values of <code>chunkSize</code> should improve the compression
   * ratio but will require more memory at indexing time and might make document
   * loading a little slower (depending on the size of your OS cache compared
   * to the size of your index).
   *
   * @param formatName the name of the {@link StoredFieldsFormat}
   * @param compressionMode the {@link CompressionMode} to use
   * @param chunkSize the minimum number of bytes of a single chunk of stored documents
   * @param maxDocsPerChunk the maximum number of documents in a single chunk
   * @param blockShift the log in base 2 of number of chunks to store in an index block
   * @see CompressionMode
   */
  public CompressingStoredFieldsFormat(String formatName, String segmentSuffix,
                                       CompressionMode compressionMode, int chunkSize, int maxDocsPerChunk, int blockShift) {
    this.formatName = formatName;//  Lucene50StoredFieldsFast
    this.segmentSuffix = segmentSuffix;
    this.compressionMode = compressionMode; //FAST
    if (chunkSize < 1) {
      throw new IllegalArgumentException("chunkSize must be >= 1");
    }
    this.chunkSize = chunkSize; // 16k
    if (maxDocsPerChunk < 1) {
      throw new IllegalArgumentException("maxDocsPerChunk must be >= 1");
    }
    this.maxDocsPerChunk = maxDocsPerChunk; // 128
    if (blockShift < DirectMonotonicWriter.MIN_BLOCK_SHIFT || blockShift > DirectMonotonicWriter.MAX_BLOCK_SHIFT) {
      throw new IllegalArgumentException("blockSize must be in " + DirectMonotonicWriter.MIN_BLOCK_SHIFT + "-" +
          DirectMonotonicWriter.MAX_BLOCK_SHIFT + ", got " + blockShift);
    }
    this.blockShift = blockShift; // 1024
  }

  @Override  // 读取包括fdx和fdt文件的数据： The stored fields for documents
  public StoredFieldsReader fieldsReader(Directory directory, SegmentInfo si,
      FieldInfos fn, IOContext context) throws IOException {
    return new CompressingStoredFieldsReader(directory, si, segmentSuffix, fn, 
        context, formatName, compressionMode);
  }

  @Override  // 是链级别的粒度
  public StoredFieldsWriter fieldsWriter(Directory directory, SegmentInfo si,
      IOContext context) throws IOException {
    return new CompressingStoredFieldsWriter(directory, si, segmentSuffix, context,// 仅仅把头文件给写了
        formatName, compressionMode, chunkSize, maxDocsPerChunk, blockShift);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(compressionMode=" + compressionMode
        + ", chunkSize=" + chunkSize + ", maxDocsPerChunk=" + maxDocsPerChunk + ", blockShift=" + blockShift + ")";
  }

}
