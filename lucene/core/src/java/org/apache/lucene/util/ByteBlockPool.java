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


import java.util.Arrays;
import java.util.List;

import static org.apache.lucene.util.RamUsageEstimator.NUM_BYTES_OBJECT_REF;

/** 
 * Class that Posting and PostingVector use to write byte
 * streams into shared fixed-size byte[] arrays.  The idea
 * is to allocate slices of increasing lengths For
 * example, the first slice is 5 bytes, the next slice is
 * 14, etc.  We start by writing our bytes into the first
 * 5 bytes.  When we hit the end of the slice, we allocate
 * the next slice and then write the address of the new
 * slice into the last 4 bytes of the previous slice (the
 * "forwarding address").
 *
 * Each slice is filled with 0's initially, and we mark
 * the end with a non-zero byte.  This way the methods
 * that are writing into the slice don't need to record
 * its length and instead allocate a new slice once they
 * hit a non-zero byte. 
 *  // 维护多个字节数组，可自动扩容，来对外提供基本的字节类型数据存储的功能，类似于jdk ArrayList数据的实现，调用者可以把ByteBlockPool当成是一个无限扩容的数组使用
 * @lucene.internal    是Lucene实现 高效的可变长的基本类型数组 ，但实际上数组一旦初始化之后长度是固定的，因为数组申请的内存必须是连续分配的，以致能够提供快速随机访问的能力。那么ByteBlockPool是如何实现的
 **/ // https://www.cnblogs.com/forfuture1978/archive/2010/02/02/1661441.html https://blog.csdn.net/asdfsadfasdfsa/article/details/88723443      https://blog.csdn.net/dang414238645/article/details/89517851
public final class ByteBlockPool implements Accountable {
  private static final long BASE_RAM_BYTES = RamUsageEstimator.shallowSizeOfInstance(ByteBlockPool.class);

  public final static int BYTE_BLOCK_SHIFT = 15;
  public final static int BYTE_BLOCK_SIZE = 1 << BYTE_BLOCK_SHIFT;  // 块大小32K, 单个词的词块这里做了限制。一个block最大32K
  public final static int BYTE_BLOCK_MASK = BYTE_BLOCK_SIZE - 1;

  /** Abstract class for allocating and freeing byte
   *  blocks. */
  public abstract static class Allocator {
    protected final int blockSize;  // 32k

    public Allocator(int blockSize) {
      this.blockSize = blockSize;
    }

    public abstract void recycleByteBlocks(byte[][] blocks, int start, int end);

    public void recycleByteBlocks(List<byte[]> blocks) {
      final byte[][] b = blocks.toArray(new byte[blocks.size()][]);
      recycleByteBlocks(b, 0, b.length);
    }

    public byte[] getByteBlock() {
      return new byte[blockSize];
    }
  }
  
  /** A simple {@link Allocator} that never recycles. */
  public static final class DirectAllocator extends Allocator {
    
    public DirectAllocator() {
      this(BYTE_BLOCK_SIZE);
    }

    public DirectAllocator(int blockSize) {
      super(blockSize);
    }

    @Override
    public void recycleByteBlocks(byte[][] blocks, int start, int end) {
    }
  }
  
  /** A simple {@link Allocator} that never recycles, but
   *  tracks how much total RAM is in use. */
  public static class DirectTrackingAllocator extends Allocator {
    private final Counter bytesUsed;  // 统计当前byte使用了多少内存，是从DocumentsWriterPerThread初始化中传递过来的
    
    public DirectTrackingAllocator(Counter bytesUsed) {
      this(BYTE_BLOCK_SIZE, bytesUsed);
    }

    public DirectTrackingAllocator(int blockSize, Counter bytesUsed) {
      super(blockSize);
      this.bytesUsed = bytesUsed;
    }

    @Override
    public byte[] getByteBlock() {  //得到一个新的block
      bytesUsed.addAndGet(blockSize);
      return new byte[blockSize];
    }

    @Override
    public void recycleByteBlocks(byte[][] blocks, int start, int end) {
      bytesUsed.addAndGet(-((end-start)* blockSize));
      for (int i = start; i < end; i++) {
        blocks[i] = null;
      }
    }
  };

  /**
   * array of buffers currently used in the pool. Buffers are allocated if
   * needed don't modify this outside of this class.
   */
  public byte[][] buffers = new byte[10][]; // 10还是可以扩容的，扩容后，最多也只有10 level
  
  /** index into the buffers array pointing to the current buffer used as the head */
  private int bufferUpto = -1;  // Which buffer we are upto，二元数据的第一元指针，当前使用的哪里了  当前块的第几个byte[]
  /** Where we are in head buffer */
  public int byteUpto = BYTE_BLOCK_SIZE;  //byte[]内的偏移量，当前buffer内可分配的起始位置。初始化时假定已经用完一个byte[]。接着还要使用的话，就重新创建byte[]

  /** Current head buffer */
  public byte[] buffer;  //当前块
  /** Current head offset */ // 为-BYTE_BLOCK_SIZE是为了增加一个byte[]时比较方面
  public int byteOffset = -BYTE_BLOCK_SIZE; // 当前buffer在所有buffers的位置（当前buffer在全局的偏移量）， (bufferUpto -1)*BYTE_BLOCK_SIZE

  private final Allocator allocator; // 默认使用DirectTrackingAllocator

  public ByteBlockPool(Allocator allocator) {
    this.allocator = allocator;  //DirectTrackingAllocator
  }
  
  /**
   * Resets the pool to its initial state reusing the first buffer and fills all
   * buffers with <tt>0</tt> bytes before they reused or passed to
   * {@link Allocator#recycleByteBlocks(byte[][], int, int)}. Calling
   * {@link ByteBlockPool#nextBuffer()} is not needed after reset.
   */
  public void reset() {
    reset(true, true);
  }
  
  /**
   * Expert: Resets the pool to its initial state reusing the first buffer. Calling
   * {@link ByteBlockPool#nextBuffer()} is not needed after reset. 
   * @param zeroFillBuffers if <code>true</code> the buffers are filled with <tt>0</tt>. 
   *        This should be set to <code>true</code> if this pool is used with slices.
   * @param reuseFirst if <code>true</code> the first buffer will be reused and calling
   *        {@link ByteBlockPool#nextBuffer()} is not needed after reset iff the 
   *        block pool was used before ie. {@link ByteBlockPool#nextBuffer()} was called before.
   */
  public void reset(boolean zeroFillBuffers, boolean reuseFirst) {
    if (bufferUpto != -1) {
      // We allocated at least one buffer

      if (zeroFillBuffers) {
        for(int i=0;i<bufferUpto;i++) {
          // Fully zero fill buffers that we fully used
          Arrays.fill(buffers[i], (byte) 0);
        }
        // Partial zero fill the final buffer
        Arrays.fill(buffers[bufferUpto], 0, byteUpto, (byte) 0);
      }
     
     if (bufferUpto > 0 || !reuseFirst) {
       final int offset = reuseFirst ? 1 : 0;  
       // Recycle all but the first buffer
       allocator.recycleByteBlocks(buffers, offset, 1+bufferUpto);
       Arrays.fill(buffers, offset, 1+bufferUpto, null); // 还回去
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
   * Advances the pool to its next buffer. This method should be called once
   * after the constructor to initialize the pool. In contrast to the
   * constructor a {@link ByteBlockPool#reset()} call will advance the pool to
   * its first buffer immediately.
   */
  public void nextBuffer() {//buffers写入位置bufferUpto 达到buffers的最大长度时 对buffers拷贝 扩容
    if (1+bufferUpto == buffers.length) {
      byte[][] newBuffers = new byte[ArrayUtil.oversize(buffers.length+1,//根据当前机器64/32位和对象引用占用字节数获得最新长度
                                                        NUM_BYTES_OBJECT_REF)][];
      System.arraycopy(buffers, 0, newBuffers, 0, buffers.length); // 只是拷贝了数组引用，并没有copy数组。这里体现了ByteBlockPool的优势
      buffers = newBuffers;
    } //
    buffer = buffers[1+bufferUpto] = allocator.getByteBlock();//allocator分配器负责初始化buffers中每个数组的大小, 一次申请32kb
    bufferUpto++;//buffers写入位置+1

    byteUpto = 0;//buffer写入位置
    byteOffset += BYTE_BLOCK_SIZE;//指针位置初始化
  }
  //索引构建过程需要为每个Term分配一块相对独立的空间来存储Posting信息
  /**
   * Allocates a new slice with the given size. 
   * @see ByteBlockPool#FIRST_LEVEL_SIZE
   */ // 需要产生一个新的slice，申请多少，就给多少，绝不给对
  public int newSlice(final int size) { //最后一个只能是size-1个byte可用，第size个字符存的是当前slice的level
    if (byteUpto > BYTE_BLOCK_SIZE-size) // 当前buffer装不下
      nextBuffer();// 那么这个buffer剩余空间全部作废
    final int upto = byteUpto;// 若buffer换新的了，则该值为0
    byteUpto += size;
    buffer[byteUpto-1] = 16;//赋值为16 调用时 会从byteUpto开始写入，当遇到buffer[pos]位置不为0 时，会调用allocSlice方法 通过 16&15得到当前NEXT_LEVEL_ARRAY中level
    return upto; //申请的slice的相对起始位置
  }

  // Size of each slice.  These arrays should be at most 16
  // elements (index is encoded with 4 bits).  First array
  // is just a compact way to encode X+1 with a max.  Second
  // array is the length of each slice, ie first slice is 5
  // bytes, next slice is 14 bytes, etc.
  
  /**
   * An array holding the offset into the {@link ByteBlockPool#LEVEL_SIZE_ARRAY}
   * to quickly navigate to the next slice level.
   */  //表示的是当前层的下一层是第几层，可见第9层的下一层还是第9层，也就是说最高有9层。大于第9层也只能取第9层
  public final static int[] NEXT_LEVEL_ARRAY = {1, 2, 3, 4, 5, 6, 7, 8, 9, 9};
  
  /**
   * An array holding the level sizes for byte slices.
   */
  public final static int[] LEVEL_SIZE_ARRAY = {5, 14, 20, 30, 40, 40, 80, 80, 120, 200};
  
  /**
   * The first level size for new slices
   * @see ByteBlockPool#newSlice(int)
   */
  public final static int FIRST_LEVEL_SIZE = LEVEL_SIZE_ARRAY[0];
  // https://www.iteye.com/blog/hxraid-642737 import
  /**
   * Creates a new byte slice with the given starting size and 
   * returns the slices offset in the pool. slice是当前buffer
   */  // 对当前slice继续扩容到下一个级别。由于即将分配的点slice[upto]是level节点，需要再对该slice扩容一个新的slice加上去。返回当前buffer节点内的可用节点
  public int allocSlice(final byte[] slice, final int upto) {
    //slice[upto]初始为16 在写入slice的时候 初始一定都为0 遇到不为0的就是slice的结束位置
    final int level = slice[upto] & 15;  // //可根据块的结束符来得到块所在的层次。从而我们可以推断，每个层次的块都有不同的结束符，第1层为16，第2层位17，第3层18，依次类推。大于第
    final int newLevel = NEXT_LEVEL_ARRAY[level]; // 从数组中得到下一个层次及下一层块的大小。
    final int newSize = LEVEL_SIZE_ARRAY[newLevel];// 第九层还是第九层
    // 如果当前缓存总量不够大，则从DocumentsWriter的freeByteBlocks中分配。
    // Maybe allocate another block
    if (byteUpto > BYTE_BLOCK_SIZE-newSize) { // 当前buffer不够分了，则丢弃当前buffer剩余所有未使用的
      nextBuffer();
    }

    final int newUpto = byteUpto; // 当前块的起始位置
    final int offset = newUpto + byteOffset;  // 实际偏移量，整个pool打通的偏移量，迁移量之后的就可以使用了
    byteUpto += newSize;
    //当分配了新的块的时候，需要有一个指针从本块指向下一个块，使得读取此信息的时候，能够在此块读取结束后，到下一个块继续读取。
    // Copy forward the past 3 bytes (which we are about
    // to overwrite with the forwarding address):
    buffer[newUpto] = slice[upto-3];//这个指针需要4个byte，在本块中，除了结束符所占用的一个byte之外，之前的三个byte的数据都应该移到新的块中，从而四个byte连起来形成一个指针。
    buffer[newUpto+1] = slice[upto-2];
    buffer[newUpto+2] = slice[upto-1];
   // 空出来的3位连同级别位存放位置
    // Write forwarding address at end of last slice:
    slice[upto-3] = (byte) (offset >>> 24);
    slice[upto-2] = (byte) (offset >>> 16);
    slice[upto-1] = (byte) (offset >>> 8);
    slice[upto] = (byte) offset;
    // 在新扩容的slice结尾填下下一级别
    // Write new level:
    buffer[byteUpto-1] = (byte) (16|newLevel);  ///每一层的结束符，16-17-18-19-20-21-......，一直延伸

    return newUpto+3;
  }

  /** Fill the provided {@link BytesRef} with the bytes at the specified offset/length slice.
   *  This will avoid copying the bytes, if the slice fits into a single block; otherwise, it uses
   *  the provided {@link BytesRefBuilder} to copy bytes over. */
  void setBytesRef(BytesRefBuilder builder, BytesRef result, long offset, int length) {
    result.length = length;

    int bufferIndex = (int) (offset >> BYTE_BLOCK_SHIFT);
    byte[] buffer = buffers[bufferIndex];
    int pos = (int) (offset & BYTE_BLOCK_MASK);
    if (pos + length <= BYTE_BLOCK_SIZE) {
      // common case where the slice lives in a single block: just reference the buffer directly without copying
      result.bytes = buffer;
      result.offset = pos;
    } else {
      // uncommon case: the slice spans at least 2 blocks, so we must copy the bytes:
      builder.grow(length);
      result.bytes = builder.get().bytes;
      result.offset = 0;
      readBytes(offset, result.bytes, 0, length);
    }
  }

  // Fill in a BytesRef from term's length & bytes encoded in
  // byte block  //从本ByteBlockPool中的textStart读取一个字符，放入term中
  public void setBytesRef(BytesRef term, int textStart) {//通过指定位置开始读取一个BytesRes，textStart就是这个单词实际存储位置
    final byte[] bytes = term.bytes = buffers[textStart >> BYTE_BLOCK_SHIFT];
    int pos = textStart & BYTE_BLOCK_MASK;
    if ((bytes[pos] & 0x80) == 0) {//小于128 一个字节 表示长度
      // length is 1 byte
      term.length = bytes[pos];
      term.offset = pos+1;
    } else {
      // length is 2 bytes  大于128两个字节表示长度
      term.length = (bytes[pos]&0x7f) + ((bytes[pos+1]&0xff)<<7);
      term.offset = pos+2;
    }
    assert term.length >= 0;
  }
  
  /**
   * Appends the bytes in the provided {@link BytesRef} at
   * the current position.
   */
  public void append(final BytesRef bytes) {
    int bytesLeft = bytes.length;
    int offset = bytes.offset;
    while (bytesLeft > 0) {
      int bufferLeft = BYTE_BLOCK_SIZE - byteUpto;
      if (bytesLeft < bufferLeft) { // 剩下的还够装
        // fits within current buffer
        System.arraycopy(bytes.bytes, offset, buffer, byteUpto, bytesLeft);
        byteUpto += bytesLeft;
        break;
      } else { // 不够装的话，新建一个BytePool
        // fill up this buffer and move to next one
        if (bufferLeft > 0) {
          System.arraycopy(bytes.bytes, offset, buffer, byteUpto, bufferLeft);
        }
        nextBuffer();
        bytesLeft -= bufferLeft;
        offset += bufferLeft;
      }
    }
  }
  
  /**
   * Reads bytes out of the pool starting at the given offset with the given  
   * length into the given byte array at offset <tt>off</tt>.
   * <p>Note: this method allows to copy across block boundaries.</p>
   */
  public void readBytes(final long offset, final byte bytes[], int bytesOffset, int bytesLength) {
    int bytesLeft = bytesLength;
    int bufferIndex = (int) (offset >> BYTE_BLOCK_SHIFT);//offset 得到buffers中的其实位置
    int pos = (int) (offset & BYTE_BLOCK_MASK);//得到buffer中的位置
    while (bytesLeft > 0) {//处理数据属于两个buffer的情况
      byte[] buffer = buffers[bufferIndex++];
      int chunk = Math.min(bytesLeft, BYTE_BLOCK_SIZE - pos);
      System.arraycopy(buffer, pos, bytes, bytesOffset, chunk);
      bytesOffset += chunk;
      bytesLeft -= chunk;
      pos = 0;
    }
  }

  /**
   * Set the given {@link BytesRef} so that its content is equal to the
   * {@code ref.length} bytes starting at {@code offset}. Most of the time this
   * method will set pointers to internal data-structures. However, in case a
   * value crosses a boundary, a fresh copy will be returned.
   * On the contrary to {@link #setBytesRef(BytesRef, int)}, this does not
   * expect the length to be encoded with the data.
   */  // 从当前BytePool中的offset处读取ref长度的内容，放入ref中
  public void setRawBytesRef(BytesRef ref, final long offset) {
    int bufferIndex = (int) (offset >> BYTE_BLOCK_SHIFT);
    int pos = (int) (offset & BYTE_BLOCK_MASK);
    if (pos + ref.length <= BYTE_BLOCK_SIZE) {
      ref.bytes = buffers[bufferIndex];
      ref.offset = pos; // 把这个buffer给拿过来，只是自己保存了它的属性值而已
    } else {
      ref.bytes = new byte[ref.length];
      ref.offset = 0;// 从下一个
      readBytes(offset, ref.bytes, 0, ref.length);
    }
  }

  /** Read a single byte at the given {@code offset}. */
  public byte readByte(long offset) {
    int bufferIndex = (int) (offset >> BYTE_BLOCK_SHIFT);
    int pos = (int) (offset & BYTE_BLOCK_MASK);
    byte[] buffer = buffers[bufferIndex];
    return buffer[pos];
  }

  @Override
  public long ramBytesUsed() {
    long size = BASE_RAM_BYTES;
    size += RamUsageEstimator.sizeOfObject(buffer);
    size += RamUsageEstimator.shallowSizeOf(buffers);
    for (byte[] buf : buffers) {
      if (buf == buffer) {
        continue;
      }
      size += RamUsageEstimator.sizeOfObject(buf);
    }
    return size;
  }
}

