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



/**
 * Non-specialized {@link BulkOperation} for {@link PackedInts.Format#PACKED}.
 */// block的概念：假如每个数据长度为6，用long存储，一个long为64，那么存储3*2*32=192个字节，可以恰好是64的整数倍，存储不浪费一点空间，192个字节需要3个long来组成。
class BulkOperationPacked extends BulkOperation {
  // BulkOperationPacked文章可以看下这里：https://blog.csdn.net/asdfsadfasdfsa/article/details/88689381
  private final int bitsPerValue; //每个长度为12  若由long来储存， 192bit ，一个block由16个long，192bit构成
  private final int longBlockCount;   // 一个block需要几个long来存储。当前为3，
  private final int longValueCount;    //一个block可以存储多少个bitsPerValue长度的数据， 当前为16
  private final int byteBlockCount; // 若我们换成byte来存储，一个block需要几个byte来存储     24的话  3
  private final int byteValueCount; // 若我们换成byte来存储，一个block可以存储多少个bitsPerValue长度的数据。当前为2
  private final long mask;
  private final int intMask;

  public BulkOperationPacked(int bitsPerValue) { // 12
    this.bitsPerValue = bitsPerValue;
    assert bitsPerValue > 0 && bitsPerValue <= 64;
    int blocks = bitsPerValue;
    while ((blocks & 1) == 0) {// 右边的数为0的全部去掉，就是为了求bitsPerValue和64之间的最大公约数
      blocks >>>= 1;
    }
    this.longBlockCount = blocks; // 三个long构成1个block
    this.longValueCount = 64 * longBlockCount / bitsPerValue; //一个block有16个bitsPerValue
    int byteBlockCount = 8 * longBlockCount; // 若由byte来存储，一个block需要24个byte来充当
    int byteValueCount = longValueCount; // 由long来存储，可以存16个bitsPerValue
    while ((byteBlockCount & 1) == 0 && (byteValueCount & 1) == 0) { // 此时一个block需要24个byte存放，可以存储32个byte。那好，大家一起降级公约数。
      byteBlockCount >>>= 1;
      byteValueCount >>>= 1;
    }
    this.byteBlockCount = byteBlockCount; // 右byte来存储，需要三个block
    this.byteValueCount = byteValueCount; // 由byte来存储，一个block可以存2个bitsPerValue
    if (bitsPerValue == 64) {
      this.mask = ~0L;
    } else {
      this.mask = (1L << bitsPerValue) - 1;
    }
    this.intMask = (int) mask;
    assert longValueCount * bitsPerValue == 64 * longBlockCount; // 验证公约数相等
  }

  @Override
  public int longBlockCount() {
    return longBlockCount;
  }

  @Override
  public int longValueCount() {
    return longValueCount; // 一个block需要多少位长度
  }
  //若我们换成byte来存储，一个block需要几个byte来存储
  @Override
  public int byteBlockCount() {
    return byteBlockCount;
  }

  @Override
  public int byteValueCount() {
    return byteValueCount;
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, long[] values,
      int valuesOffset, int iterations) {
    int bitsLeft = 64;
    for (int i = 0; i < longValueCount * iterations; ++i) {
      bitsLeft -= bitsPerValue;
      if (bitsLeft < 0) { // 在0-63之内
        values[valuesOffset++] =
            ((blocks[blocksOffset++] & ((1L << (bitsPerValue + bitsLeft)) - 1)) << -bitsLeft)
            | (blocks[blocksOffset] >>> (64 + bitsLeft));
        bitsLeft += 64;
      } else {
        values[valuesOffset++] = (blocks[blocksOffset] >>> bitsLeft) & mask;
      }
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, long[] values,
      int valuesOffset, int iterations) {
    long nextValue = 0L;
    int bitsLeft = bitsPerValue;
    for (int i = 0; i < iterations * byteBlockCount; ++i) {
      final long bytes = blocks[blocksOffset++] & 0xFFL;
      if (bitsLeft > 8) {
        // just buffer
        bitsLeft -= 8;
        nextValue |= bytes << bitsLeft;
      } else {
        // flush
        int bits = 8 - bitsLeft;
        values[valuesOffset++] = nextValue | (bytes >>> bits);
        while (bits >= bitsPerValue) {
          bits -= bitsPerValue;
          values[valuesOffset++] = (bytes >>> bits) & mask;
        }
        // then buffer
        bitsLeft = bitsPerValue - bits;
        nextValue = (bytes & ((1L << bits) - 1)) << bitsLeft;
      }
    }
    assert bitsLeft == bitsPerValue;
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, int[] values,
      int valuesOffset, int iterations) {
    if (bitsPerValue > 32) {
      throw new UnsupportedOperationException("Cannot decode " + bitsPerValue + "-bits values into an int[]");
    }
    int bitsLeft = 64;
    for (int i = 0; i < longValueCount * iterations; ++i) {
      bitsLeft -= bitsPerValue;
      if (bitsLeft < 0) {
        values[valuesOffset++] = (int)
            (((blocks[blocksOffset++] & ((1L << (bitsPerValue + bitsLeft)) - 1)) << -bitsLeft)
            | (blocks[blocksOffset] >>> (64 + bitsLeft)));
        bitsLeft += 64;
      } else {
        values[valuesOffset++] = (int) ((blocks[blocksOffset] >>> bitsLeft) & mask);
      }
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, int[] values,
      int valuesOffset, int iterations) {
    int nextValue = 0;
    int bitsLeft = bitsPerValue;
    for (int i = 0; i < iterations * byteBlockCount; ++i) {
      final int bytes = blocks[blocksOffset++] & 0xFF;
      if (bitsLeft > 8) {
        // just buffer
        bitsLeft -= 8;
        nextValue |= bytes << bitsLeft;
      } else {
        // flush
        int bits = 8 - bitsLeft;
        values[valuesOffset++] = nextValue | (bytes >>> bits);
        while (bits >= bitsPerValue) {
          bits -= bitsPerValue;
          values[valuesOffset++] = (bytes >>> bits) & intMask;
        }
        // then buffer
        bitsLeft = bitsPerValue - bits;
        nextValue = (bytes & ((1 << bits) - 1)) << bitsLeft;
      }
    }
    assert bitsLeft == bitsPerValue;
  }

  @Override
  public void encode(long[] values, int valuesOffset, long[] blocks,
      int blocksOffset, int iterations) {
    long nextBlock = 0;
    int bitsLeft = 64;
    for (int i = 0; i < longValueCount * iterations; ++i) {//可以存储的数
      bitsLeft -= bitsPerValue;
      if (bitsLeft > 0) { // 够存一个，还有富余
        nextBlock |= values[valuesOffset++] << bitsLeft; // 放高位
      } else if (bitsLeft == 0) { // 刚好够存储1个
        nextBlock |= values[valuesOffset++];
        blocks[blocksOffset++] = nextBlock;
        nextBlock = 0;
        bitsLeft = 64;
      } else { //当前long不够存一个，那么需要先存储
        nextBlock |= values[valuesOffset] >>> -bitsLeft;
        blocks[blocksOffset++] = nextBlock;// 先存储一部分
        nextBlock = (values[valuesOffset++] & ((1L << -bitsLeft) - 1)) << (64 + bitsLeft);// 再存储另外一部分
        bitsLeft += 64;
      }
    }
  }

  @Override
  public void encode(int[] values, int valuesOffset, long[] blocks,
      int blocksOffset, int iterations) {
    long nextBlock = 0;
    int bitsLeft = 64;
    for (int i = 0; i < longValueCount * iterations; ++i) {
      bitsLeft -= bitsPerValue;
      if (bitsLeft > 0) {
        nextBlock |= (values[valuesOffset++] & 0xFFFFFFFFL) << bitsLeft;
      } else if (bitsLeft == 0) {
        nextBlock |= (values[valuesOffset++] & 0xFFFFFFFFL);
        blocks[blocksOffset++] = nextBlock;
        nextBlock = 0;
        bitsLeft = 64;
      } else { // bitsLeft < 0
        nextBlock |= (values[valuesOffset] & 0xFFFFFFFFL) >>> -bitsLeft;
        blocks[blocksOffset++] = nextBlock;
        nextBlock = (values[valuesOffset++] & ((1L << -bitsLeft) - 1)) << (64 + bitsLeft);
        bitsLeft += 64;
      }
    }
  }
  // 开始对数组进行编码，怎样将4个长度为6的数据放入到3个长度为8的byte中
  @Override
  public void encode(long[] values, int valuesOffset, byte[] blocks, //blocks 真正存放byte的地方
      int blocksOffset, int iterations) {
    int nextBlock = 0; // 缓存的block
    int bitsLeft = 8;
    for (int i = 0; i < byteValueCount * iterations; ++i) {
      final long v = values[valuesOffset++]; // 读取出来
      assert PackedInts.unsignedBitsRequired(v) <= bitsPerValue;
      if (bitsPerValue < bitsLeft) { // 该byte内可用长度够用
        // just buffer
        nextBlock |= v << (bitsLeft - bitsPerValue);
        bitsLeft -= bitsPerValue;
      } else {
        // flush as many blocks as possible
        int bits = bitsPerValue - bitsLeft; // 除了使用剩余的俩位置，还需要4个byte存储
        blocks[blocksOffset++] = (byte) (nextBlock | (v >>> bits)); // 将之前存储的6位+2位一起存储起来
        while (bits >= 8) {
          bits -= 8;
          blocks[blocksOffset++] = (byte) (v >>> bits);
        }
        // then buffer
        bitsLeft = 8 - bits; // 把这个数据剩余的4个bit长度给存储在缓存中
        nextBlock = (int) ((v & ((1L << bits) - 1)) << bitsLeft);
      }
    }
    assert bitsLeft == 8;
  }

  @Override
  public void encode(int[] values, int valuesOffset, byte[] blocks,
      int blocksOffset, int iterations) {
    int nextBlock = 0;
    int bitsLeft = 8;
    for (int i = 0; i < byteValueCount * iterations; ++i) {
      final int v = values[valuesOffset++];
      assert PackedInts.bitsRequired(v & 0xFFFFFFFFL) <= bitsPerValue;
      if (bitsPerValue < bitsLeft) {
        // just buffer
        nextBlock |= v << (bitsLeft - bitsPerValue);
        bitsLeft -= bitsPerValue;
      } else {
        // flush as many blocks as possible
        int bits = bitsPerValue - bitsLeft;
        blocks[blocksOffset++] = (byte) (nextBlock | (v >>> bits));
        while (bits >= 8) {
          bits -= 8;
          blocks[blocksOffset++] = (byte) (v >>> bits);
        }
        // then buffer
        bitsLeft = 8 - bits;
        nextBlock = (v & ((1 << bits) - 1)) << bitsLeft;
      }
    }
    assert bitsLeft == 8;
  }

}
