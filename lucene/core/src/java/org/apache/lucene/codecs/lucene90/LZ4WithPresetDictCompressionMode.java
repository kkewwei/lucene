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

import java.io.IOException;
import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.compressing.Compressor;
import org.apache.lucene.codecs.compressing.Decompressor;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.compress.LZ4;

/**
 * A compression mode that compromises on the compression ratio to provide fast compression and
 * decompression.
 *
 * @lucene.internal
 */
public final class LZ4WithPresetDictCompressionMode extends CompressionMode {

  // Shoot for 10 sub blocks
  private static final int NUM_SUB_BLOCKS = 10;
  // And a dictionary whose size is about 2x smaller than sub blocks
  private static final int DICT_SIZE_FACTOR = 2;// 词典大小时普通sub block的1/2

  /** Sole constructor. */
  public LZ4WithPresetDictCompressionMode() {}

  @Override
  public Compressor newCompressor() {
    return new LZ4WithPresetDictCompressor();
  }

  @Override
  public Decompressor newDecompressor() {
    return new LZ4WithPresetDictDecompressor();
  }

  @Override
  public String toString() {
    return "BEST_SPEED";
  }

  private static final class LZ4WithPresetDictDecompressor extends Decompressor {

    private int[] compressedLengths;//每个chunk压缩后的长度
    private byte[] buffer;// 这玩意只有扩容，没有缩容

    LZ4WithPresetDictDecompressor() {
      compressedLengths = new int[0];
      buffer = new byte[0];
    }

    private int readCompressedLengths(
        DataInput in, int originalLength, int dictLength, int blockLength) throws IOException {
      in.readVInt(); // compressed length of the dictionary, unused。词典压缩后的长度，默认无意义
      int totalLength = dictLength;
      int i = 0;
      compressedLengths = ArrayUtil.growNoCopy(compressedLengths, originalLength / blockLength + 1);
      while (totalLength < originalLength) {

        compressedLengths[i++] = in.readVInt();//每个chunk压缩后的长度
        totalLength += blockLength;
      }
      return i;
    }
    // originalLength：这个chunk未压缩前的总长度       length：这个文档未压缩前在二进制流的长度，offset这个文档在未压缩前在二进制流的长度，bytes存放解压后的数据
    @Override
    public void decompress(DataInput in, int originalLength, int offset, int length, BytesRef bytes)
        throws IOException {
      assert offset + length <= originalLength;

      if (length == 0) {
        bytes.length = 0;
        return;
      }

      final int dictLength = in.readVInt();// 压缩前的词典大小
      final int blockLength = in.readVInt();// 每个子chunk压缩前的平均大小
     // 压缩是多少个子block
      final int numBlocks = readCompressedLengths(in, originalLength, dictLength, blockLength);
      // 把buffer给扩上去
      buffer = ArrayUtil.growNoCopy(buffer, dictLength + blockLength);
      bytes.length = 0;// 先将长度置位
      // Read the dictionary
      if (LZ4.decompress(in, dictLength, buffer, 0) != dictLength) {// 读取字典
        throw new CorruptIndexException("Illegal dict length", in);
      }

      int offsetInBlock = dictLength;// // 压缩前需要跳过的长度
      int offsetInBytesRef = offset; // 这个block中只能从哪个offset开始使用
      if (offset >= dictLength) {// 读取的不是字典
        offsetInBytesRef -= dictLength;

        // Skip unneeded blocks
        int numBytesToSkip = 0;// 压缩后长度需要跳过的
        for (int i = 0; i < numBlocks && offsetInBlock + blockLength < offset; ++i) {// 跳过一些不必要的子block
          int compressedBlockLength = compressedLengths[i];
          numBytesToSkip += compressedBlockLength;
          offsetInBlock += blockLength;// 压缩前需要跳过的
          offsetInBytesRef -= blockLength;
        }
        in.skipBytes(numBytesToSkip);//跳过一些子block，都是压缩后的长度
      } else {// 字典都满足了我们的需求
        // The dictionary contains some bytes we need, copy its content to the BytesRef
        bytes.bytes = ArrayUtil.growNoCopy(bytes.bytes, dictLength);
        System.arraycopy(buffer, 0, bytes.bytes, 0, dictLength);
        bytes.length = dictLength;
      }

      // Read blocks that intersect with the interval we need
      if (offsetInBlock < offset + length) {//
        bytes.bytes = ArrayUtil.grow(bytes.bytes, bytes.length + offset + length - offsetInBlock);
      }
      while (offsetInBlock < offset + length) {// 一直压缩，指导我们需要的子block中我们需要的offset+length
        final int bytesToDecompress = Math.min(blockLength, offset + length - offsetInBlock);// 这个子chunk需要解压的长度(压缩前的长度)
        LZ4.decompress(in, bytesToDecompress, buffer, dictLength);
        System.arraycopy(buffer, dictLength, bytes.bytes, bytes.length, bytesToDecompress);// 从词典后的二进制开始读取解压后的字节流
        bytes.length += bytesToDecompress;
        offsetInBlock += blockLength;
      }

      bytes.offset = offsetInBytesRef;
      bytes.length = length;
      assert bytes.isValid();
    }

    @Override
    public Decompressor clone() {
      return new LZ4WithPresetDictDecompressor();
    }
  }

  private static class LZ4WithPresetDictCompressor extends Compressor {

    final ByteBuffersDataOutput compressed;
    final LZ4.FastCompressionHashTable hashTable;
    byte[] buffer;

    LZ4WithPresetDictCompressor() {
      compressed = ByteBuffersDataOutput.newResettableInstance(); // 压缩后的数据，是放在这里的，词典也在这里
      hashTable = new LZ4.FastCompressionHashTable(); // 仅仅是记录重复数据起始位置，辅助使用的
      buffer = BytesRef.EMPTY_BYTES;
    }
    // dictLen字典长度，dictLen：词典其实位置， 当前压缩前长度：len。  // 仅仅是压缩，并未存储真正压缩后的bytes
    private void doCompress(byte[] bytes, int dictLen, int len, DataOutput out) throws IOException {
      long prevCompressedSize = compressed.size();
      LZ4.compressWithDictionary(bytes, 0, dictLen, len, compressed, hashTable);// 向compressed写入压缩后的内容
      // Write the number of compressed bytes
      out.writeVInt(Math.toIntExact(compressed.size() - prevCompressedSize));//先向文件写入词典压缩后的长度
    }

    @Override
    public void compress(ByteBuffersDataInput buffersInput, DataOutput out) throws IOException {
      final int len = (int) (buffersInput.length() - buffersInput.position());
      final int dictLength = Math.min(LZ4.MAX_DISTANCE, len / (NUM_SUB_BLOCKS * DICT_SIZE_FACTOR)); // 词典长度，最大64k
      final int blockLength = (len - dictLength + NUM_SUB_BLOCKS - 1) / NUM_SUB_BLOCKS;
      buffer = ArrayUtil.growNoCopy(buffer, dictLength + blockLength);//使用buffer利用
      out.writeVInt(dictLength);// 压缩前词典大小
      out.writeVInt(blockLength);// 压缩前的block大小

      compressed.reset();// 压缩后数据存放地方。起始不用reset，每次进来时都是一个新的LZ4WithPresetDictCompressor。
      // Compress the dictionary first
      buffersInput.readBytes(buffer, 0, dictLength);// 先读取词典
      doCompress(buffer, 0, dictLength, out);// 仅仅是压缩，并未存储真正压缩后的bytes

      // And then sub blocks
      for (int start = dictLength; start < len; start += blockLength) {
        int l = Math.min(blockLength, len - start);
        buffersInput.readBytes(buffer, dictLength, l);
        doCompress(buffer, dictLength, l, out); //
      }
      //到此为止，仅仅先写入了每个subchunk的length,现在真正开始写入压缩后的数据了。
      // We only wrote lengths so far, now write compressed data
      compressed.copyTo(out);
    }

    @Override
    public void close() throws IOException {
      // no-op
    }
  }
}
