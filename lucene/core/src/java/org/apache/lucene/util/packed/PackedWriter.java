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
package org.apache.lucene.util.packed;


import org.apache.lucene.store.DataOutput;

import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;

// Packs high order byte first, to match
// IndexOutput.writeInt/Long/Short byte order

final class PackedWriter extends PackedInts.Writer {

  boolean finished;
  final PackedInts.Format format;
  final BulkOperation encoder;
  final byte[] nextBlocks; // block存放数据的地方
  final long[] nextValues;
  final int iterations; // 主要是控制nextValues和nextBlocks大小的。最少一个byteBlock。可以一次写入2、3个block，主要看可用的mem
  int off;
  int written; // 完成了一次向byte写入的存储过程
  // 这批数据写入是所私用的PakcWriter，下一批数据就换了PackedWriter
  PackedWriter(PackedInts.Format format, DataOutput out, int valueCount, int bitsPerValue, int mem) {
    super(out, valueCount, bitsPerValue);
    this.format = format;
    encoder = BulkOperation.of(format, bitsPerValue); // format=PACKED, encoder = BulkOperationPacked6
    iterations = encoder.computeIterations(valueCount, mem); // 一次可以写入几个block
    nextBlocks = new byte[iterations * encoder.byteBlockCount()];// // byteBlockCount: 1个block需要多少个byte组成
    nextValues = new long[iterations * encoder.byteValueCount()]; // byteValueCount: 1个block能存放多少个bitsPerValue，临时存放的地方，最终是存放在nextBlocks
    off = 0;
    written = 0;
    finished = false;
  }

  @Override
  protected PackedInts.Format getFormat() {
    return format;
  }

  @Override
  public void add(long v) throws IOException {
    assert PackedInts.unsignedBitsRequired(v) <= bitsPerValue;
    assert !finished;
    if (valueCount != -1 && written >= valueCount) {
      throw new EOFException("Writing past end of stream");
    }
    nextValues[off++] = v;
    if (off == nextValues.length) { // 只要把目前缓存使用完了，立马向磁盘写入
      flush();
    }
    ++written;
  }
  // 完成所有字段写入时会调用，主要是为了把缓存中数据主动给刷到文件中
  @Override
  public void finish() throws IOException {
    assert !finished;
    if (valueCount != -1) {
      while (written < valueCount) {
        add(0L); // 最终将不到一个block的数据也刷入文件中
      }
    }
    flush(); //
    finished = true;
  }

  private void flush() throws IOException { // 只要达到一个block就写入
    encoder.encode(nextValues, 0, nextBlocks, 0, iterations); // 把数据以byte形式存储在了nextBlocks中
    final int blockCount = (int) format.byteCount(PackedInts.VERSION_CURRENT, off, bitsPerValue); // block存储一个block，需要几个byte
    out.writeBytes(nextBlocks, blockCount); // 已经写入了文件中
    Arrays.fill(nextValues, 0L);
    off = 0;
  }

  @Override
  public int ord() {
    return written - 1;
  }
}
