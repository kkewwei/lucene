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
package org.apache.lucene.util;

import static org.apache.lucene.util.RamUsageEstimator.NUM_BYTES_OBJECT_REF;

import java.util.Arrays;

/**
 * This class enables the allocation of fixed-size buffers and their management as part of a buffer
 * array. Allocation is done through the use of an {@link Allocator} which can be customized, e.g.
 * to allow recycling old buffers. There are methods for writing ({@link #append(BytesRef)} and
 * reading from the buffers (e.g. {@link #readBytes(long, byte[], int, int)}, which handle
 * read/write operations across buffer boundaries.
 *
 * @lucene.internal// 维护多个字节数组，可自动扩容，来对外提供基本的字节类型数据存储的功能，类似于jdk ArrayList数据的实现，调用者可以把ByteBlockPool当成是一个无限扩容的数组使用
 */ //是Lucene实现 高效的可变长的基本类型数组 ，但实际上数组一旦初始化之后长度是固定的，因为数组申请的内存必须是连续分配的，以致能够提供快速随机访问的能力。那么ByteBlockPool是如何实现的
public final class ByteBlockPool implements Accountable {// https://www.cnblogs.com/forfuture1978/archive/2010/02/02/1661441.html https://blog.csdn.net/asdfsadfasdfsa/article/details/88723443      https://blog.csdn.net/dang414238645/article/details/89517851
  private static final long BASE_RAM_BYTES =
      RamUsageEstimator.shallowSizeOfInstance(ByteBlockPool.class);

  /**
   * Use this to find the index of the buffer containing a byte, given an offset to that byte.
   *
   * <p>bufferUpto = globalOffset &gt;&gt; BYTE_BLOCK_SHIFT
   *
   * <p>bufferUpto = globalOffset / BYTE_BLOCK_SIZE
   */
  public static final int BYTE_BLOCK_SHIFT = 15;

  /** The size of each buffer in the pool. */
  public static final int BYTE_BLOCK_SIZE = 1 << BYTE_BLOCK_SHIFT;// 块大小32K, 单个词的词块这里做了限制。一个block最大32KB

  /**
   * Use this to find the position of a global offset in a particular buffer.
   *
   * <p>positionInCurrentBuffer = globalOffset &amp; BYTE_BLOCK_MASK
   *
   * <p>positionInCurrentBuffer = globalOffset % BYTE_BLOCK_SIZE
   */
  public static final int BYTE_BLOCK_MASK = BYTE_BLOCK_SIZE - 1;

  /** Abstract class for allocating and freeing byte blocks. */
  public abstract static class Allocator {
    // TODO: ByteBlockPool assume the blockSize is always {@link BYTE_BLOCK_SIZE}, but this class
    // allow arbitrary value of blockSize. We should make them consistent.
    protected final int blockSize;// 32k

    protected Allocator(int blockSize) {
      this.blockSize = blockSize;
    }

    public abstract void recycleByteBlocks(byte[][] blocks, int start, int end);

    public byte[] getByteBlock() {
      return new byte[blockSize];
    }
  }

  /** A simple {@link Allocator} that never recycles. */
  public static final class DirectAllocator extends Allocator {

    public DirectAllocator() {
      super(BYTE_BLOCK_SIZE);
    }

    @Override
    public void recycleByteBlocks(byte[][] blocks, int start, int end) {}
  }

  /** A simple {@link Allocator} that never recycles, but tracks how much total RAM is in use. */
  public static class DirectTrackingAllocator extends Allocator {
    private final Counter bytesUsed; // 统计当前byte使用了多少内存，是从DocumentsWriterPerThread初始化中传递过来的

    public DirectTrackingAllocator(Counter bytesUsed) {
      super(BYTE_BLOCK_SIZE);
      this.bytesUsed = bytesUsed;
    }

    @Override
    public byte[] getByteBlock() {  //得到一个新的block
      bytesUsed.addAndGet(blockSize);
      return new byte[blockSize];
    }

    @Override
    public void recycleByteBlocks(byte[][] blocks, int start, int end) {
      bytesUsed.addAndGet(-((end - start) * blockSize));
      for (int i = start; i < end; i++) {
        blocks[i] = null;
      }
    }
  }

  /** Array of buffers currently used in the pool. Buffers are allocated if needed. */
  private byte[][] buffers = new byte[10][];// 10还是可以扩容的，扩容后，最多也只有10 level

  /** index into the buffers array pointing to the current buffer used as the head */
  private int bufferUpto = -1; // Which buffer we are upto 二元数据的第一元指针，当前使用的哪里了  当前块的第几个byte[]

  /** Where we are in the head buffer. */
  public int byteUpto = BYTE_BLOCK_SIZE; //byte[]内的偏移量，当前buffer内可分配的起始位置。初始化时假定已经用完一个byte[]。接着还要使用的话，就重新创建byte[]

  /** Current head buffer. */
  public byte[] buffer; //当前块

  /**
   * Offset from the start of the first buffer to the start of the current buffer, which is
   * bufferUpto * BYTE_BLOCK_SIZE. The buffer pool maintains this offset because it is the first to
   * overflow if there are too many allocated blocks.
   */// 为-BYTE_BLOCK_SIZE是为了增加一个byte[]时比较方面
  public int byteOffset = -BYTE_BLOCK_SIZE;// 当前buffer在所有buffers的位置（当前buffer在全局的偏移量）， (bufferUpto -1)*BYTE_BLOCK_SIZE

  private final Allocator allocator; // 默认使用DirectTrackingAllocator

  public ByteBlockPool(Allocator allocator) {
    this.allocator = allocator;  //DirectTrackingAllocator
  }

  /**
   * Expert: Resets the pool to its initial state, while optionally reusing the first buffer.
   * Buffers that are not reused are reclaimed by {@link Allocator#recycleByteBlocks(byte[][], int,
   * int)}. Buffers can be filled with zeros before recycling them. This is useful if a slice pool
   * works on top of this byte pool and relies on the buffers being filled with zeros to find the
   * non-zero end of slices.
   *
   * @param zeroFillBuffers if {@code true} the buffers are filled with {@code 0}. This should be
   *     set to {@code true} if this pool is used with slices.
   * @param reuseFirst if {@code true} the first buffer will be reused and calling {@link
   *     ByteBlockPool#nextBuffer()} is not needed after reset iff the block pool was used before
   *     ie. {@link ByteBlockPool#nextBuffer()} was called before.
   */
  public void reset(boolean zeroFillBuffers, boolean reuseFirst) {
    if (bufferUpto != -1) {
      // We allocated at least one buffer

      if (zeroFillBuffers) {
        for (int i = 0; i < bufferUpto; i++) {
          // Fully zero fill buffers that we fully used
          Arrays.fill(buffers[i], (byte) 0);
        }
        // Partial zero fill the final buffer
        Arrays.fill(buffers[bufferUpto], 0, byteUpto, (byte) 0);
      }

      if (bufferUpto > 0 || !reuseFirst) {
        final int offset = reuseFirst ? 1 : 0;
        // Recycle all but the first buffer
        allocator.recycleByteBlocks(buffers, offset, 1 + bufferUpto);
        Arrays.fill(buffers, offset, 1 + bufferUpto, null); // 还回去
      }
      if (reuseFirst) {
        // Re-use the first buffer
        bufferUpto = 0;
        byteUpto = 0;
        byteOffset = 0;
        buffer = buffers[0];
      } else {
        bufferUpto = -1;
        byteUpto = BYTE_BLOCK_SIZE;
        byteOffset = -BYTE_BLOCK_SIZE;
        buffer = null;
      }
    }
  }

  /**
   * Allocates a new buffer and advances the pool to it. This method should be called once after the
   * constructor to initialize the pool. In contrast to the constructor, a {@link
   * ByteBlockPool#reset(boolean, boolean)} call will advance the pool to its first buffer
   * immediately.
   */
  public void nextBuffer() {//buffers写入位置bufferUpto 达到buffers的最大长度时 对buffers拷贝 扩容
    if (1 + bufferUpto == buffers.length) {
      // The buffer array is full - expand it
      byte[][] newBuffers =
          new byte[ArrayUtil.oversize(buffers.length + 1, NUM_BYTES_OBJECT_REF)][];//根据当前机器64/32位和对象引用占用字节数获得最新长度
      System.arraycopy(buffers, 0, newBuffers, 0, buffers.length);// 只是拷贝了数组引用，并没有copy数组。这里体现了ByteBlockPool的优势
      buffers = newBuffers;
    }
    // Allocate new buffer and advance the pool to it
    buffer = buffers[1 + bufferUpto] = allocator.getByteBlock();//allocator分配器负责初始化buffers中每个数组的大小, 一次申请32kb
    bufferUpto++;//buffers写入位置+1
    byteUpto = 0;//buffer写入位置
    byteOffset = Math.addExact(byteOffset, BYTE_BLOCK_SIZE);//指针位置初始化
  }
  //索引构建过程需要为每个Term分配一块相对独立的空间来存储Posting信息
  /**
   * Fill the provided {@link BytesRef} with the bytes at the specified offset and length. This will
   * avoid copying the bytes if the slice fits into a single block; otherwise, it uses the provided
   * {@link BytesRefBuilder} to copy bytes over.
   *///从本ByteBlockPool中的textStart读取一个字符，放入term中
  void setBytesRef(BytesRefBuilder builder, BytesRef result, long offset, int length) {//通过指定位置开始读取一个BytesRes，textStart就是这个单词实际存储位置
    result.length = length;

    int bufferIndex = Math.toIntExact(offset >> BYTE_BLOCK_SHIFT);
    byte[] buffer = buffers[bufferIndex];
    int pos = (int) (offset & BYTE_BLOCK_MASK);
    if (pos + length <= BYTE_BLOCK_SIZE) {//小于128 一个字节 表示长度
      // Common case: The slice lives in a single block. Reference the buffer directly.
      result.bytes = buffer;
      result.offset = pos;
    } else {// 大于128两个字节表示长度
      // Uncommon case: The slice spans at least 2 blocks, so we must copy the bytes.
      builder.growNoCopy(length);
      result.bytes = builder.get().bytes;
      result.offset = 0;
      readBytes(offset, result.bytes, 0, length);
    }
  }

  /** Appends the bytes in the provided {@link BytesRef} at the current position. */
  public void append(final BytesRef bytes) {
    append(bytes.bytes, bytes.offset, bytes.length);
  }

  /**
   * Append the bytes from a source {@link ByteBlockPool} at a given offset and length
   *
   * @param srcPool the source pool to copy from
   * @param srcOffset the source pool offset
   * @param length the number of bytes to copy
   */
  public void append(ByteBlockPool srcPool, long srcOffset, int length) {
    int bytesLeft = length;
    while (bytesLeft > 0) {
      int bufferLeft = BYTE_BLOCK_SIZE - byteUpto;
      if (bytesLeft < bufferLeft) { // fits within current buffer
        appendBytesSingleBuffer(srcPool, srcOffset, bytesLeft);
        break;
      } else { // fill up this buffer and move to next one
        if (bufferLeft > 0) {
          appendBytesSingleBuffer(srcPool, srcOffset, bufferLeft);
          bytesLeft -= bufferLeft;
          srcOffset += bufferLeft;
        }
        nextBuffer();
      }
    }
  }

  // copy from source pool until no bytes left. length must be fit within the current head buffer
  private void appendBytesSingleBuffer(ByteBlockPool srcPool, long srcOffset, int length) {
    assert length <= BYTE_BLOCK_SIZE - byteUpto;
    // doing a loop as the bytes to copy might span across multiple byte[] in srcPool
    while (length > 0) {
      byte[] srcBytes = srcPool.buffers[Math.toIntExact(srcOffset >> BYTE_BLOCK_SHIFT)];
      int srcPos = Math.toIntExact(srcOffset & BYTE_BLOCK_MASK);
      int bytesToCopy = Math.min(length, BYTE_BLOCK_SIZE - srcPos);
      System.arraycopy(srcBytes, srcPos, buffer, byteUpto, bytesToCopy);
      length -= bytesToCopy;
      srcOffset += bytesToCopy;
      byteUpto += bytesToCopy;
    }
  }

  /**
   * Append the provided byte array at the current position.
   *
   * @param bytes the byte array to write
   */
  public void append(final byte[] bytes) {
    append(bytes, 0, bytes.length);
  }

  /**
   * Append some portion of the provided byte array at the current position.
   *
   * @param bytes the byte array to write
   * @param offset the offset of the byte array
   * @param length the number of bytes to write
   */
  public void append(final byte[] bytes, int offset, int length) {
    int bytesLeft = length;
    while (bytesLeft > 0) {
      int bufferLeft = BYTE_BLOCK_SIZE - byteUpto;
      if (bytesLeft < bufferLeft) { // 剩下的还够装
        // fits within current buffer
        System.arraycopy(bytes, offset, buffer, byteUpto, bytesLeft);
        byteUpto += bytesLeft;
        break;
      } else { // 不够装的话，新建一个BytePool
        // fill up this buffer and move to next one
        if (bufferLeft > 0) {
          System.arraycopy(bytes, offset, buffer, byteUpto, bufferLeft);
        }
        nextBuffer();
        bytesLeft -= bufferLeft;
        offset += bufferLeft;
      }
    }
  }

  /**
   * Reads bytes out of the pool starting at the given offset with the given length into the given
   * byte array at offset <code>off</code>.
   *
   * <p>Note: this method allows to copy across block boundaries.
   */
  public void readBytes(final long offset, final byte[] bytes, int bytesOffset, int bytesLength) {
    int bytesLeft = bytesLength;
    int bufferIndex = Math.toIntExact(offset >> BYTE_BLOCK_SHIFT);//offset 得到buffers中的其实位置
    int pos = (int) (offset & BYTE_BLOCK_MASK);//得到buffer中的位置
    while (bytesLeft > 0) {//处理数据属于两个buffer的情况
      byte[] buffer = buffers[bufferIndex++];
      assert buffer != null;
      int chunk = Math.min(bytesLeft, BYTE_BLOCK_SIZE - pos);
      System.arraycopy(buffer, pos, bytes, bytesOffset, chunk);
      bytesOffset += chunk;
      bytesLeft -= chunk;
      pos = 0;
    }
  }

  /**
   * Read a single byte at the given offset
   *
   * @param offset the offset to read
   * @return the byte
   */
  public byte readByte(final long offset) {
    int bufferIndex = (int) (offset >> BYTE_BLOCK_SHIFT);
    int pos = (int) (offset & BYTE_BLOCK_MASK);
    return buffers[bufferIndex][pos];
  }

  @Override
  public long ramBytesUsed() {
    long size = BASE_RAM_BYTES;
    size += RamUsageEstimator.shallowSizeOf(buffers);
    for (byte[] buf : buffers) {
      size += RamUsageEstimator.sizeOfObject(buf);
    }
    return size;
  }

  /** the current position (in absolute value) of this byte pool */
  public long getPosition() {
    return bufferUpto * allocator.blockSize + byteUpto;
  }

  /** Retrieve the buffer at the specified index from the buffer pool. */
  public byte[] getBuffer(int bufferIndex) {
    return buffers[bufferIndex];
  }
}
