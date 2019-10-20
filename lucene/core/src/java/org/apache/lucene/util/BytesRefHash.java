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
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.util.ByteBlockPool.DirectAllocator;

import static org.apache.lucene.util.ByteBlockPool.BYTE_BLOCK_MASK;
import static org.apache.lucene.util.ByteBlockPool.BYTE_BLOCK_SHIFT;
import static org.apache.lucene.util.ByteBlockPool.BYTE_BLOCK_SIZE;

/**
 * {@link BytesRefHash} is a special purpose hash-map like data-structure
 * optimized for {@link BytesRef} instances. BytesRefHash maintains mappings of
 * byte arrays to ids (Map&lt;BytesRef,int&gt;) storing the hashed bytes
 * efficiently in continuous storage. The mapping to the id is
 * encapsulated inside {@link BytesRefHash} and is guaranteed to be increased
 * for each added {@link BytesRef}.
 * 
 * <p>
 * Note: The maximum capacity {@link BytesRef} instance passed to
 * {@link #add(BytesRef)} must not be longer than {@link ByteBlockPool#BYTE_BLOCK_SIZE}-2. 
 * The internal storage is limited to 2GB total byte storage.
 * </p>
 * 
 * @lucene.internal
 */ // 一个类HashMap的存储结果，但是存储传统的数组+链表形式，而是只有数组，当有冲突时使用线性探测方法解决。
public final class BytesRefHash implements Accountable {
  private static final long BASE_RAM_BYTES = RamUsageEstimator.shallowSizeOfInstance(BytesRefHash.class) +
      // size of scratch1
      RamUsageEstimator.shallowSizeOfInstance(BytesRef.class) +
      // size of Counter
      RamUsageEstimator.primitiveSizes.get(long.class);

  public static final int DEFAULT_CAPACITY = 16;

  // the following fields are needed by comparator,
  // so package private to prevent access$-methods:
  final ByteBlockPool pool;   //要存储的值(BytesRef)的最终位置， TermVectorsConsumerPerField.pool和FreqProxTermsWriterPerField.pool是同一个对象
  int[] bytesStart;  // 实际上的作用为了来找到每个词组在pool中的起始位置，下标是是termId, 存放term长度+term内容。= ParallelPostingsArray.textStarts, 每个域都有一个.
  // TermVectorsConsumerPerField.BytesRefHash的bytesStart时刻会和FreqProxTermsWriterPerField.BytesRefHash的bytesStart保持一致
  private final BytesRef scratch1 = new BytesRef();
  private int hashSize;// 就是ids数组的长度
  private int hashHalfSize;
  private int hashMask;
  private int count;//count是存储的这个文档distinct(单词)的个数
  private int lastCount = -1;  // TermVectorsConsumerPerField.BytesRefHash的ids在每写完一个doc时，会清空。详见set()函数。lastCount是上个文档的distinct个数
  private int[] ids; // key根据term的hashcode获得的， 不为0，则说明里面存储的是termId，当hash冲突后，通过探针法解决。所有field之间的term还不共享。一个域内是共享的, 这里在每个文档被写完后，都会调用，清理掉ids
  private final BytesStartArray bytesStartArray; // PostingsBytesStartArray，对FreqProxTermsWriterPerField和TermVectorsConsumerPerField分别产生一个
  private Counter bytesUsed;

  /**
   * Creates a new {@link BytesRefHash} with a {@link ByteBlockPool} using a
   * {@link DirectAllocator}.
   */
  public BytesRefHash() { 
    this(new ByteBlockPool(new DirectAllocator()));
  }
  
  /**
   * Creates a new {@link BytesRefHash}
   */
  public BytesRefHash(ByteBlockPool pool) {
    this(pool, DEFAULT_CAPACITY, new DirectBytesStartArray(DEFAULT_CAPACITY));
  }

  /**
   * Creates a new {@link BytesRefHash}
   */
  public BytesRefHash(ByteBlockPool pool, int capacity, BytesStartArray bytesStartArray) {
    hashSize = capacity;
    hashHalfSize = hashSize >> 1;
    hashMask = hashSize - 1;
    this.pool = pool;
    ids = new int[hashSize];
    Arrays.fill(ids, -1);
    this.bytesStartArray = bytesStartArray; // 俩都是PostingsBytesStartArray, 最普通的
    bytesStart = bytesStartArray.init(); // 两个的bytesStart都是从2个PostingsBytesStartArray.init中取的=postingsArray.textStarts。在这里单独生成的
    bytesUsed = bytesStartArray.bytesUsed() == null? Counter.newCounter() : bytesStartArray.bytesUsed();
    bytesUsed.addAndGet(hashSize * Integer.BYTES); // 注意这里统计ids使用
  }

  /**
   * Returns the number of {@link BytesRef} values in this {@link BytesRefHash}.
   * 
   * @return the number of {@link BytesRef} values in this {@link BytesRefHash}.
   */
  public int size() {
    return count;
  }

  /**
   * Populates and returns a {@link BytesRef} with the bytes for the given
   * bytesID.
   * <p>
   * Note: the given bytesID must be a positive integer less than the current
   * size ({@link #size()})
   * 
   * @param bytesID
   *          the id
   * @param ref
   *          the {@link BytesRef} to populate
   * 
   * @return the given BytesRef instance populated with the bytes for the given
   *         bytesID
   */
  public BytesRef get(int bytesID, BytesRef ref) {
    assert bytesStart != null : "bytesStart is null - not initialized";
    assert bytesID < bytesStart.length: "bytesID exceeds byteStart len: " + bytesStart.length;
    pool.setBytesRef(ref, bytesStart[bytesID]);
    return ref;
  }

  /**
   * Returns the ids array in arbitrary order. Valid ids start at offset of 0
   * and end at a limit of {@link #size()} - 1
   * <p>
   * Note: This is a destructive operation. {@link #clear()} must be called in
   * order to reuse this {@link BytesRefHash} instance.
   * </p>
   *
   * @lucene.internal
   */
  public int[] compact() {
    assert bytesStart != null : "bytesStart is null - not initialized";
    int upto = 0;
    for (int i = 0; i < hashSize; i++) {
      if (ids[i] != -1) { // 把ids不为空的向前摞一摞，紧凑点
        if (upto < i) {
          ids[upto] = ids[i];
          ids[i] = -1;
        }
        upto++;
      }
    }

    assert upto == count;
    lastCount = count;
    return ids;
  }

  /**
   * Returns the values array sorted by the referenced byte values.
   * <p>
   * Note: This is a destructive operation. {@link #clear()} must be called in
   * order to reuse this {@link BytesRefHash} instance.
   * </p> // 根据termdId对应的值排序，返回的是termdId
   */ //  仅仅根据在pool中存储的每个termId的byte从小到大进行排序
  public int[] sort() { // 返回的是每个termId
    final int[] compact = compact();
    new StringMSBRadixSorter() {

      BytesRef scratch = new BytesRef();

      @Override
      protected void swap(int i, int j) { // 交换第i和j个termId
        int tmp = compact[i]; // 仅仅根据在pool中存储的每个termId的byte从小到大进行排序
        compact[i] = compact[j];
        compact[j] = tmp;
      }

      @Override
      protected BytesRef get(int i) {
        pool.setBytesRef(scratch, bytesStart[compact[i]]); // 这里是取出第i个termId的byteValue
        return scratch;
      }

    }.sort(0, count);
    return compact;
  }

  private boolean equals(int id, BytesRef b) {
    pool.setBytesRef(scratch1, bytesStart[id]);
    return scratch1.bytesEquals(b);
  }
 // 是否可以将ids收缩
  private boolean shrink(int targetSize) {
    // Cannot use ArrayUtil.shrink because we require power
    // of 2:
    int newSize = hashSize;
    while (newSize >= 8 && newSize / 4 > targetSize) {
      newSize /= 2;
    }
    if (newSize != hashSize) {
      bytesUsed.addAndGet(Integer.BYTES * -(hashSize - newSize));
      hashSize = newSize;
      ids = new int[hashSize];
      Arrays.fill(ids, -1);
      hashHalfSize = newSize / 2;
      hashMask = newSize - 1;
      return true;
    } else {
      return false;
    }
  }

  /**
   * Clears the {@link BytesRef} which maps to the given {@link BytesRef}
   */  //这里在每个文档被写完后，都会调用，清理掉ids
  public void clear(boolean resetPool) {
    lastCount = count;
    count = 0;
    if (resetPool) { // 跳过
      pool.reset(false, false); // we don't need to 0-fill the buffers
    }
    bytesStart = bytesStartArray.clear();
    if (lastCount != -1 && shrink(lastCount)) {
      // shrink clears the hash entries
      return;
    }
    Arrays.fill(ids, -1); // 对ids清空
  }

  public void clear() {
    clear(true);
  }
  
  /**
   * Closes the BytesRefHash and releases all internally used memory
   */
  public void close() {
    clear(true);
    ids = null;
    bytesUsed.addAndGet(Integer.BYTES * -hashSize);
  }

  /**
   * Adds a new {@link BytesRef}
   * 
   * @param bytes
   *          the bytes to hash
   * @return the id the given bytes are hashed if there was no mapping for the
   *         given bytes, otherwise <code>(-(id)-1)</code>. This guarantees
   *         that the return value will always be &gt;= 0 if the given bytes
   *         haven't been hashed before.
   * 
   * @throws MaxBytesLengthExceededException
   *           if the given bytes are {@code > 2 +}
   *           {@link ByteBlockPool#BYTE_BLOCK_SIZE}
   */ //作用是向pool中存放这个bytes
  public int add(BytesRef bytes) { // 返回给这个term分配id
    assert bytesStart != null : "Bytesstart is null - not initialized";
    final int length = bytes.length;
    // final position
    final int hashPos = findHash(bytes);// 找到当前要存储的bytes所在的有效槽位，若不存在，遍历到后续无值的位置
    int e = ids[hashPos]; // 所有field之间的term还不共享  1173
    //如果为-1，则是新的term
    if (e == -1) {
      // new entry    存储的时候，在ByteBlockPool中的结构是：长度+具体的term。至少预留2byte存放长度
      final int len2 = 2 + bytes.length;// lucene支持的term长度不超过2个字节，长度采用变长整数表示，因此需要申请的存储空间为2 + bytes.length。
      if (len2 + pool.byteUpto > BYTE_BLOCK_SIZE) {
        if (len2 > BYTE_BLOCK_SIZE) {
          throw new MaxBytesLengthExceededException("bytes can be at most " // 这里报的我们经常见到的异样
              + (BYTE_BLOCK_SIZE - 2) + " in length; got " + bytes.length);
        }
        pool.nextBuffer();
      }
      final byte[] buffer = pool.buffer;
      final int bufferUpto = pool.byteUpto;// 获取内存池的起始可用位置
      if (count >= bytesStart.length) {// byteStart用来记录termId在内存池中存储的起始位置，
        bytesStart = bytesStartArray.grow(); // 是通过
        assert count < bytesStart.length + 1 : "count: " + count + " len: "
            + bytesStart.length;
      }
      e = count++;
   // 记录对应termId在ByteStartPool中的起始位置。
      bytesStart[e] = bufferUpto + pool.byteOffset;

      // We first encode the length, followed by the
      // bytes. Length is encoded as vInt, but will consume
      // 1 or 2 bytes at most (we reject too-long terms,
      // above).  // 长度小于128，则长度用一个字节的vInt即可存储。
      if (length < 128) {
        // 1 byte to store length
        buffer[bufferUpto] = (byte) length;
        pool.byteUpto += length + 1;
        assert length >= 0: "Length must be positive: " + length;
        System.arraycopy(bytes.bytes, bytes.offset, buffer, bufferUpto + 1,
            length);
      } else {
        // 2 byte to store length
        buffer[bufferUpto] = (byte) (0x80 | (length & 0x7f));
        buffer[bufferUpto + 1] = (byte) ((length >> 7) & 0xff);
        pool.byteUpto += length + 2;
        System.arraycopy(bytes.bytes, bytes.offset, buffer, bufferUpto + 2,
            length);
      }
      assert ids[hashPos] == -1;
      ids[hashPos] = e; // 记录hashPos对应的属于第几个count。重复的term进不来

      if (count == hashHalfSize) {
        rehash(2 * hashSize, true);// rehash，不展开叙述。
      }
      return e;
    }
    return -(e + 1);// 如果不是新的term，则直接返回。返回的是负数
  }
  
  /**
   * Returns the id of the given {@link BytesRef}.
   * 
   * @param bytes
   *          the bytes to look for
   * 
   * @return the id of the given bytes, or {@code -1} if there is no mapping for the
   *         given bytes.
   */
  public int find(BytesRef bytes) {
    return ids[findHash(bytes)];
  }
  // 探针法
  private int findHash(BytesRef bytes) {
    assert bytesStart != null : "bytesStart is null - not initialized";

    int code = doHash(bytes.bytes, bytes.offset, bytes.length);

    // final position
    int hashPos = code & hashMask;
    int e = ids[hashPos]; //
    if (e != -1 && !equals(e, bytes)) { // 没找到
      // Conflict; use linear probe to find an open slot
      // (see LUCENE-5604):
      do {
        code++;
        hashPos = code & hashMask;
        e = ids[hashPos];
      } while (e != -1 && !equals(e, bytes)); // 线性探测所有的没空的槽位
    }
    
    return hashPos;
  }

  /** Adds a "arbitrary" int offset instead of a BytesRef
   *  term.  This is used in the indexer to hold the hash for term
   *  vectors, because they do not redundantly store the byte[] term
   *  directly and instead reference the byte[] term
   *  already stored by the postings BytesRefHash.  See
   *  add(int textStart) in TermsHashPerField. */
  public int addByPoolOffset(int offset) {
    assert bytesStart != null : "Bytesstart is null - not initialized";
    // final position
    int code = offset;
    int hashPos = offset & hashMask;
    int e = ids[hashPos]; //这里在每个文档被写完后，都会调用，清理掉ids
    if (e != -1 && bytesStart[e] != offset) {
      // Conflict; use linear probe to find an open slot
      // (see LUCENE-5604):
      do {
        code++;
        hashPos = code & hashMask;
        e = ids[hashPos];
      } while (e != -1 && bytesStart[e] != offset);
    }
    if (e == -1) { // 是新的，
      // new entry
      if (count >= bytesStart.length) {
        bytesStart = bytesStartArray.grow();
        assert count < bytesStart.length + 1 : "count: " + count + " len: "
            + bytesStart.length;
      }
      e = count++; // 但是这里统计这，一样的
      bytesStart[e] = offset; // offset是pool所用第e个TermId到的位置。FreqProxTermsWriterPerField是全局的编号，TermVectorsConsumerPerField是局部的编号。
      assert ids[hashPos] == -1; // 统计的是该域对每个term的重新编号
      ids[hashPos] = e;

      if (count == hashHalfSize) {
        rehash(2 * hashSize, false);
      }
      return e;
    }
    return -(e + 1);
  }

  /**
   * Called when hash is too small ({@code > 50%} occupied) or too large ({@code < 20%}
   * occupied).
   */
  private void rehash(final int newSize, boolean hashOnData) {
    final int newMask = newSize - 1;
    bytesUsed.addAndGet(Integer.BYTES * (newSize));
    final int[] newHash = new int[newSize];
    Arrays.fill(newHash, -1);
    for (int i = 0; i < hashSize; i++) {
      final int e0 = ids[i];
      if (e0 != -1) {
        int code;
        if (hashOnData) {
          final int off = bytesStart[e0];
          final int start = off & BYTE_BLOCK_MASK;
          final byte[] bytes = pool.buffers[off >> BYTE_BLOCK_SHIFT];
          final int len;
          int pos;
          if ((bytes[start] & 0x80) == 0) {
            // length is 1 byte
            len = bytes[start];
            pos = start + 1;
          } else {
            len = (bytes[start] & 0x7f) + ((bytes[start + 1] & 0xff) << 7);
            pos = start + 2;
          }
          code = doHash(bytes, pos, len);
        } else {
          code = bytesStart[e0];
        }

        int hashPos = code & newMask;
        assert hashPos >= 0;
        if (newHash[hashPos] != -1) {
          // Conflict; use linear probe to find an open slot
          // (see LUCENE-5604):
          do {
            code++;
            hashPos = code & newMask;
          } while (newHash[hashPos] != -1);
        }
        newHash[hashPos] = e0;
      }
    }

    hashMask = newMask;
    bytesUsed.addAndGet(Integer.BYTES * (-ids.length));
    ids = newHash;
    hashSize = newSize;
    hashHalfSize = newSize / 2;
  }

  // TODO: maybe use long?  But our keys are typically short...
  private int doHash(byte[] bytes, int offset, int length) {
    return StringHelper.murmurhash3_x86_32(bytes, offset, length, StringHelper.GOOD_FAST_HASH_SEED);
  }

  /**
   * reinitializes the {@link BytesRefHash} after a previous {@link #clear()}
   * call. If {@link #clear()} has not been called previously this method has no
   * effect.
   */
  public void reinit() {
    if (bytesStart == null) {
      bytesStart = bytesStartArray.init();
    }
    
    if (ids == null) {
      ids = new int[hashSize];
      bytesUsed.addAndGet(Integer.BYTES * hashSize);
    }
  }

  /**
   * Returns the bytesStart offset into the internally used
   * {@link ByteBlockPool} for the given bytesID
   * 
   * @param bytesID
   *          the id to look up
   * @return the bytesStart offset into the internally used
   *         {@link ByteBlockPool} for the given id
   */
  public int byteStart(int bytesID) {
    assert bytesStart != null : "bytesStart is null - not initialized";
    assert bytesID >= 0 && bytesID < count : bytesID;
    return bytesStart[bytesID];
  }

  @Override
  public long ramBytesUsed() {
    long size = BASE_RAM_BYTES +
        RamUsageEstimator.sizeOfObject(bytesStart) +
        RamUsageEstimator.sizeOfObject(ids) +
        RamUsageEstimator.sizeOfObject(pool);
    return size;
  }

  /**
   * Thrown if a {@link BytesRef} exceeds the {@link BytesRefHash} limit of
   * {@link ByteBlockPool#BYTE_BLOCK_SIZE}-2.
   */
  @SuppressWarnings("serial")
  public static class MaxBytesLengthExceededException extends RuntimeException {
    MaxBytesLengthExceededException(String message) {
      super(message);
    }
  }

  /** Manages allocation of the per-term addresses. */
  public abstract static class BytesStartArray {
    /**
     * Initializes the BytesStartArray. This call will allocate memory
     * 
     * @return the initialized bytes start array
     */
    public abstract int[] init();

    /**
     * Grows the {@link BytesStartArray}
     * 
     * @return the grown array
     */
    public abstract int[] grow();

    /**
     * clears the {@link BytesStartArray} and returns the cleared instance.
     * 
     * @return the cleared instance, this might be <code>null</code>
     */
    public abstract int[] clear();

    /**
     * A {@link Counter} reference holding the number of bytes used by this
     * {@link BytesStartArray}. The {@link BytesRefHash} uses this reference to
     * track it memory usage
     * 
     * @return a {@link AtomicLong} reference holding the number of bytes used
     *         by this {@link BytesStartArray}.
     */
    public abstract Counter bytesUsed();
  }

  /** A simple {@link BytesStartArray} that tracks
   *  memory allocation using a private {@link Counter}
   *  instance.  */
  public static class DirectBytesStartArray extends BytesStartArray {
    // TODO: can't we just merge this w/
    // TrackingDirectBytesStartArray...?  Just add a ctor
    // that makes a private bytesUsed?

    protected final int initSize;
    private int[] bytesStart;
    private final Counter bytesUsed;
    
    public DirectBytesStartArray(int initSize, Counter counter) {
      this.bytesUsed = counter;
      this.initSize = initSize;      
    }
    
    public DirectBytesStartArray(int initSize) {
      this(initSize, Counter.newCounter());
    }

    @Override
    public int[] clear() {
      return bytesStart = null;
    }

    @Override
    public int[] grow() {
      assert bytesStart != null;
      return bytesStart = ArrayUtil.grow(bytesStart, bytesStart.length + 1);
    }

    @Override
    public int[] init() {
      return bytesStart = new int[ArrayUtil.oversize(initSize, Integer.BYTES)];
    }

    @Override
    public Counter bytesUsed() {
      return bytesUsed;
    }
  }
}
