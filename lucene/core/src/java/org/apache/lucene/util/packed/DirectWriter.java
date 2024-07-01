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

import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.BitUtil;

/**
 * Class for writing packed integers to be directly read from Directory. Integers can be read
 * on-the-fly via {@link DirectReader}.
 *
 * <p>Unlike PackedInts, it optimizes for read i/o operations and supports &gt; 2B values. Example
 * usage:
 *
 * <pre class="prettyprint">
 *   int bitsPerValue = DirectWriter.bitsRequired(100); // values up to and including 100
 *   IndexOutput output = dir.createOutput("packed", IOContext.DEFAULT);
 *   DirectWriter writer = DirectWriter.getInstance(output, numberOfValues, bitsPerValue);
 *   for (int i = 0; i &lt; numberOfValues; i++) {
 *     writer.add(value);
 *   }
 *   writer.finish();
 *   output.close();
 * </pre>
 *
 * @see DirectReader
 */
public final class DirectWriter {
  final int bitsPerValue;
  final long numValues;
  final DataOutput output; // 可能是dvd文件，也可能是ByteBuffer类型的存储

  long count;
  boolean finished;

  // for now, just use the existing writer under the hood
  int off;
  final byte[] nextBlocks; // 真正存放压缩
  final long[] nextValues;// 一次只能存放128个value

  DirectWriter(DataOutput output, long numValues, int bitsPerValue) {// 就是普通的长度压缩，找最大数字使用的那个长度
    this.output = output;
    this.numValues = numValues;
    this.bitsPerValue = bitsPerValue;

    final int memoryBudgetInBits = Math.multiplyExact(Byte.SIZE, PackedInts.DEFAULT_BUFFER_SIZE);
    // For every value we need 64 bits for the value and bitsPerValue for the encoded value
    int bufferSize = memoryBudgetInBits / (Long.SIZE + bitsPerValue);
    assert bufferSize > 0;
    // Round to the next multiple of 64
    bufferSize = Math.toIntExact(bufferSize + 63) & 0xFFFFFFC0;
    nextValues = new long[bufferSize];
    // add 7 bytes in the end so that any value could be written as a long
    nextBlocks = new byte[bufferSize * bitsPerValue / Byte.SIZE + Long.BYTES - 1];//都是使用byte来存放数据
  }

  /** Adds a value to this writer */
  public void add(long l) throws IOException {
    assert bitsPerValue == 64 || (l >= 0 && l <= PackedInts.maxValue(bitsPerValue)) : bitsPerValue;
    assert !finished;
    if (count >= numValues) {
      throw new EOFException("Writing past end of stream");
    }
    nextValues[off++] = l;
    if (off == nextValues.length) {
      flush(); // 已经是一个block了
    }
    count++;
  }

  private void flush() throws IOException {
    if (off == 0) {
      return;
    }
    // Avoid writing bits from values that are outside of the range we need to encode
    Arrays.fill(nextValues, off, nextValues.length, 0L); // 清空
    encode(nextValues, off, nextBlocks, bitsPerValue);//// 开始编码，比较傻瓜。可以理解直接按照类似BitMap编码
    final int blockCount =
        (int) PackedInts.Format.PACKED.byteCount(PackedInts.VERSION_CURRENT, off, bitsPerValue);
    output.writeBytes(nextBlocks, blockCount);// 每个文档的termId压缩存储起来了  dvd。在merge阶段就是限速写入的
    off = 0;
  }
   // 开始编码，比较傻瓜。可以理解直接按照类似 BitMap编码
  private static void encode(long[] nextValues, int upTo, byte[] nextBlocks, int bitsPerValue) {
    if ((bitsPerValue & 7) == 0) {//至少8的整数倍
      // bitsPerValue is a multiple of 8: 8, 16, 24, 32, 30, 48, 56, 64
      final int bytesPerValue = bitsPerValue / Byte.SIZE;
      for (int i = 0, o = 0; i < upTo; ++i, o += bytesPerValue) {
        final long l = nextValues[i];
        if (bitsPerValue > Integer.SIZE) {
          BitUtil.VH_LE_LONG.set(nextBlocks, o, l);
        } else if (bitsPerValue > Short.SIZE) {
          BitUtil.VH_LE_INT.set(nextBlocks, o, (int) l);
        } else if (bitsPerValue > Byte.SIZE) {
          BitUtil.VH_LE_SHORT.set(nextBlocks, o, (short) l);//按照shard编码之际存储
        } else {
          nextBlocks[o] = (byte) l;
        }
      }
    } else if (bitsPerValue < 8) {
      // bitsPerValue is 1, 2 or 4
      final int valuesPerLong = Long.SIZE / bitsPerValue;
      for (int i = 0, o = 0; i < upTo; i += valuesPerLong, o += Long.BYTES) {
        long v = 0;
        for (int j = 0; j < valuesPerLong; ++j) {
          v |= nextValues[i + j] << (bitsPerValue * j);
        }
        BitUtil.VH_LE_LONG.set(nextBlocks, o, v);
      }
    } else {// 若大于8，不是7的倍数
      // bitsPerValue is 12, 20 or 28
      // Write values 2 by 2
      final int numBytesFor2Values = bitsPerValue * 2 / Byte.SIZE;
      for (int i = 0, o = 0; i < upTo; i += 2, o += numBytesFor2Values) {
        final long l1 = nextValues[i];
        final long l2 = nextValues[i + 1];
        final long merged = l1 | (l2 << bitsPerValue);
        if (bitsPerValue <= Integer.SIZE / 2) {// 若每位都是小于int/2
          BitUtil.VH_LE_INT.set(nextBlocks, o, (int) merged);// 合成一个int存储
        } else {
          BitUtil.VH_LE_LONG.set(nextBlocks, o, merged);// 若合起来大于int,以long存储
        }
      }
    }
  }

  /** finishes writing */
  public void finish() throws IOException {
    if (count != numValues) {
      throw new IllegalStateException(
          "Wrong number of values added, expected: " + numValues + ", got: " + count);
    }
    assert !finished;
    flush();// 将每个文档的termId存储到dvd文件中

    // add padding bytes for fast io
    final int paddingBytesNeeded = paddingBytesNeeded(bitsPerValue);
    for (int i = 0; i < paddingBytesNeeded; i++) {
      output.writeByte((byte) 0);
    }
    finished = true;
  }

  private static int paddingBytesNeeded(int bitsPerValue) {
    // for every number of bits per value, we want to be able to read the entire value in a single
    // read e.g. for 20 bits per value, we want to be able to read values using ints so we need
    // 32 - 20 = 12 bits of padding
    final int paddingBitsNeeded;
    if (bitsPerValue > Integer.SIZE) {
      paddingBitsNeeded = Long.SIZE - bitsPerValue;
    } else if (bitsPerValue > Short.SIZE) {
      paddingBitsNeeded = Integer.SIZE - bitsPerValue;
    } else if (bitsPerValue > Byte.SIZE) {
      paddingBitsNeeded = Short.SIZE - bitsPerValue;
    } else {
      paddingBitsNeeded = 0;
    }
    assert paddingBitsNeeded >= 0;
    final int paddingBytesNeeded = (paddingBitsNeeded + Byte.SIZE - 1) / Byte.SIZE;
    assert paddingBytesNeeded <= 3;
    return paddingBytesNeeded;
  }

  /** Returns an instance suitable for encoding {@code numValues} using {@code bitsPerValue} */
  public static DirectWriter getInstance(DataOutput output, long numValues, int bitsPerValue) {
    checkBitsPerValue(bitsPerValue);
    return new DirectWriter(output, numValues, bitsPerValue);
  }

  private static void checkBitsPerValue(int bitsPerValue) {
    if (Arrays.binarySearch(SUPPORTED_BITS_PER_VALUE, bitsPerValue) < 0) {
      throw new IllegalArgumentException(
          "Unsupported bitsPerValue " + bitsPerValue + ". Did you use bitsRequired?");
    }
  }

  /**
   * Round a number of bits per value to the next amount of bits per value that is supported by this
   * writer.
   *
   * @param bitsRequired the amount of bits required
   * @return the next number of bits per value that is gte the provided value and supported by this
   *     writer
   */
  private static int roundBits(int bitsRequired) {
    int index = Arrays.binarySearch(SUPPORTED_BITS_PER_VALUE, bitsRequired); // 支持的没有10， 则只能选择每个12
    if (index < 0) {
      return SUPPORTED_BITS_PER_VALUE[-index - 1];
    } else {
      return bitsRequired;
    }
  }

  /**
   * Returns how many bits are required to hold values up to and including maxValue
   *
   * @param maxValue the maximum value that should be representable.
   * @return the amount of bits needed to represent values from 0 to maxValue.
   * @see PackedInts#bitsRequired(long)
   */
  public static int bitsRequired(long maxValue) {
    return roundBits(PackedInts.bitsRequired(maxValue));
  }

  /**
   * Returns how many bits are required to hold values up to and including maxValue, interpreted as
   * an unsigned value.
   *
   * @param maxValue the maximum value that should be representable.
   * @return the amount of bits needed to represent values from 0 to maxValue.
   * @see PackedInts#unsignedBitsRequired(long)
   */
  public static int unsignedBitsRequired(long maxValue) {
    return roundBits(PackedInts.unsignedBitsRequired(maxValue));
  }

  static final int[] SUPPORTED_BITS_PER_VALUE =
      new int[] {1, 2, 4, 8, 12, 16, 20, 24, 28, 32, 40, 48, 56, 64};

  /**
   * Returns how many bytes are written for encoding {@code numValues} using {@code bitsPerValue}.
   *
   * @param numValues total number of values
   * @param bitsPerValue the number of bits required per value
   * @return The amount of bytes written
   */
  public static long bytesRequired(long numValues, int bitsPerValue) {
    checkBitsPerValue(bitsPerValue);
    final long bytes = (numValues * bitsPerValue + Byte.SIZE - 1) / 8;
    return bytes + paddingBytesNeeded(bitsPerValue);
  }
}
