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

import java.io.DataInput;
import java.io.IOException;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.search.AbstractDocIdSetIterator;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.MathUtil;
import org.apache.lucene.util.RoaringDocIdSet;

/**
 * Disk-based implementation of a {@link DocIdSetIterator} which can return the index of the current
 * document, i.e. the ordinal of the current document among the list of documents that this iterator
 * can return. This is useful to implement sparse doc values by only having to encode values for
 * documents that actually have a value.
 *
 * <p>Implementation-wise, this {@link DocIdSetIterator} is inspired of {@link RoaringDocIdSet
 * roaring bitmaps} and encodes ranges of {@code 65536} documents independently and picks between 3
 * encodings depending on the density of the range:
 *
 * <ul>
 *   <li>{@code ALL} if the range contains 65536 documents exactly,
 *   <li>{@code DENSE} if the range contains 4096 documents or more; in that case documents are
 *       stored in a bit set,
 *   <li>{@code SPARSE} otherwise, and the lower 16 bits of the doc IDs are stored in a {@link
 *       DataInput#readShort() short}.
 * </ul>
 *
 * <p>Only ranges that contain at least one value are encoded.
 *
 * <p>This implementation uses 6 bytes per document in the worst-case, which happens in the case
 * that all ranges contain exactly one document.
 *
 * <p>To avoid O(n) lookup time complexity, with n being the number of documents, two lookup tables
 * are used: A lookup table for block offset and index, and a rank structure for DENSE block index
 * lookups.
 *
 * <p>The lookup table is an array of {@code int}-pairs, with a pair for each block. It allows for
 * direct jumping to the block, as opposed to iteration from the current position and forward one
 * block at a time.
 *
 * <p>Each int-pair entry consists of 2 logical parts:
 *
 * <p>The first 32 bit int holds the index (number of set bits in the blocks) up to just before the
 * wanted block. The maximum number of set bits is the maximum number of documents, which is less
 * than 2^31.
 *
 * <p>The next int holds the offset in bytes into the underlying slice. As there is a maximum of
 * 2^16 blocks, it follows that the maximum size of any block must not exceed 2^15 bytes to avoid
 * overflow (2^16 bytes if the int is treated as unsigned). This is currently the case, with the
 * largest block being DENSE and using 2^13 + 36 bytes.
 *
 * <p>The cache overhead is numDocs/1024 bytes.
 *
 * <p>Note: There are 4 types of blocks: ALL, DENSE, SPARSE and non-existing (0 set bits). In the
 * case of non-existing blocks, the entry in the lookup table has index equal to the previous entry
 * and offset equal to the next non-empty block.
 *
 * <p>The block lookup table is stored at the end of the total block structure.
 *
 * <p>The rank structure for DENSE blocks is an array of byte-pairs with an entry for each sub-block
 * (default 512 bits) out of the 65536 bits in the outer DENSE block.
 *
 * <p>Each rank-entry states the number of set bits within the block up to the bit before the bit
 * positioned at the start of the sub-block. Note that that the rank entry of the first sub-block is
 * always 0 and that the last entry can at most be 65536-2 = 65634 and thus will always fit into an
 * byte-pair of 16 bits.
 *
 * <p>The rank structure for a given DENSE block is stored at the beginning of the DENSE block. This
 * ensures locality and keeps logistics simple.
 *
 * @lucene.internal
 */
public final class IndexedDISI extends AbstractDocIdSetIterator {//DISI是DocIdSetIterator的缩写

  // jump-table time/space trade-offs to consider:
  // The block offsets and the block indexes could be stored in more compressed form with
  // two PackedInts or two MonotonicDirectReaders.
  // The DENSE ranks (default 128 shorts = 256 bytes) could likewise be compressed. But as there is
  // at least 4096 set bits in DENSE blocks, there will be at least one rank with 2^12 bits, so it
  // is doubtful if there is much to gain here.

  private static final int BLOCK_SIZE =
      65536; // The number of docIDs that a single block represents

  private static final int DENSE_BLOCK_LONGS = BLOCK_SIZE / Long.SIZE; // 1024    稠密型bitmap，需要多少个long来装
  public static final byte DEFAULT_DENSE_RANK_POWER = 9; // Every 512 docIDs / 8 longs  哪怕512个位置，只有1个文档存在，也占用8个long。算是一个rank的统计

  static final int MAX_ARRAY_LENGTH = (1 << 12) - 1;   // 4095  65536需要使用1024.0个long，空间8192个Byte, short占2个Byte，可以存放8192/2=4095个short
  // 写dvd文件 文档id为：0-65536就flush一次
  private static void flush(
      int block, FixedBitSet buffer, int cardinality, byte denseRankPower, IndexOutput out)
      throws IOException {
    assert block >= 0 && block < BLOCK_SIZE;
    out.writeShort((short) block);// 第几个block
    assert cardinality > 0 && cardinality <= BLOCK_SIZE;
    out.writeShort((short) (cardinality - 1)); // 这个block多少个词
    if (cardinality > MAX_ARRAY_LENGTH) {// 若大于4095个词，使用
      if (cardinality != BLOCK_SIZE) { // all docs are set
        if (denseRankPower != -1) {// 512个词，产生一个二级索引（rank，记录累加的文档数），512个词/64=8个long
          final byte[] rank = createRank(buffer, denseRankPower);//使用rank结构（structure）实现在block的稠密度为DENSE中的word之间的跳转
          out.writeBytes(rank, rank.length);
        }
        for (long word : buffer.getBits()) {
          out.writeLong(word);// 存储这个long
        }
      }
    } else {
      BitSetIterator it = new BitSetIterator(buffer, cardinality);
      for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
        out.writeShort((short) doc);// 或者存储short
      }
    }
  }
  // 使用rank结构（structure）实现在block的稠密度为DENSE中的word之间的跳转
  // Creates a DENSE rank-entry (the number of set bits up to a given point) for the buffer.
  // One rank-entry for every {@code 2^denseRankPower} bits, with each rank-entry using 2 bytes.
  // Represented as a byte[] for fast flushing and mirroring of the retrieval representation.
  private static byte[] createRank(FixedBitSet buffer, byte denseRankPower) {// 针对一个block来统计一次
    final int longsPerRank = 1 << (denseRankPower - 6);// 需要几个long，来产生一个索引（rank）,比如默认8个词
    final int rankMark = longsPerRank - 1;// 只要检查每存储7个long，就产生一个（rank）
    final int rankIndexShift = denseRankPower - 7; // 6 for the long (2^6) + 1 for 2 bytes/entry// 每多少个long,产生一个二级索引（rank）。共128个rank，len(rank)=256
    final byte[] rank = new byte[DENSE_BLOCK_LONGS >> rankIndexShift];// 需要多少个rank,512个文档间隔弄一个rank
    final long[] bits = buffer.getBits();
    int bitCount = 0;// 这个block内的累加文档数
    for (int word = 0; word < DENSE_BLOCK_LONGS; word++) {// 需要多少个long来存储，就遍历多少次（1000个long，1<<16/64=1024）
      if ((word & rankMark) == 0) { // Every longsPerRank longs 每存储8个long(64*8=512个文档)，统计下
        rank[word >> rankIndexShift] = (byte) (bitCount >> 8);//  一个block最多存储2^16个文档，需要16位存储，高8位
        rank[(word >> rankIndexShift) + 1] = (byte) (bitCount & 0xFF);// 低8位
      }
      bitCount += Long.bitCount(bits[word]);// 这个block内的累加文档数
    }
    return rank;
  }

  /**
   * Writes the docIDs from it to out, in logical blocks, one for each 65536 docIDs in monotonically
   * increasing gap-less order. DENSE blocks uses {@link #DEFAULT_DENSE_RANK_POWER} of 9 (every 512
   * docIDs / 8 longs). The caller must keep track of the number of jump-table entries (returned by
   * this method) as well as the denseRankPower (9 for this method) and provide them when
   * constructing an IndexedDISI for reading.
   *
   * @param it the document IDs.
   * @param out destination for the blocks.
   * @return the number of jump-table entries following the blocks, -1 for no entries. This should
   *     be stored in meta and used when creating an instance of IndexedDISI.
   * @throws IOException if there was an error writing to out.
   */// out: dvd文件中写文档id，每个block是划分好了固定区间，为了保证同一批处理的数据，都落到同一个区间，返回多少个block
  static short writeBitSet(DocIdSetIterator it, IndexOutput out) throws IOException {
    return writeBitSet(it, out, DEFAULT_DENSE_RANK_POWER);
  }

  /**
   * Writes the docIDs from it to out, in logical blocks, one for each 65536 docIDs in monotonically
   * increasing gap-less order. The caller must keep track of the number of jump-table entries
   * (returned by this method) as well as the denseRankPower and provide them when constructing an
   * IndexedDISI for reading.
   *
   * @param it the document IDs.
   * @param out destination for the blocks.
   * @param denseRankPower for {@link Method#DENSE} blocks, a rank will be written every {@code
   *     2^denseRankPower} docIDs. Values &lt; 7 (every 128 docIDs) or &gt; 15 (every 32768 docIDs)
   *     disables DENSE rank. Recommended values are 8-12: Every 256-4096 docIDs or 4-64 longs.
   *     {@link #DEFAULT_DENSE_RANK_POWER} is 9: Every 512 docIDs. This should be stored in meta and
   *     used when creating an instance of IndexedDISI.
   * @return the number of jump-table entries following the blocks, -1 for no entries. This should
   *     be stored in meta and used when creating an instance of IndexedDISI.
   * @throws IOException if there was an error writing to out.
   */
  public static short writeBitSet(DocIdSetIterator it, IndexOutput out, byte denseRankPower)
      throws IOException {
    final long origo = out.getFilePointer(); // All jumps are relative to the origo。所有的跳表都是从这里的相对位置开始计算的
    if ((denseRankPower < 7 || denseRankPower > 15) && denseRankPower != -1) {
      throw new IllegalArgumentException(
          "Acceptable values for denseRankPower are 7-15 (every 128-32768 docIDs). "
              + "The provided power was "
              + denseRankPower
              + " (every "
              + (int) Math.pow(2, denseRankPower)
              + " docIDs)");
    }
    int totalCardinality = 0;//所有block存在多少个词
    int blockCardinality = 0;// 当前block存在多少个词
    final FixedBitSet buffer = new FixedBitSet(1 << 16);// 66636个词，稀疏矩阵存储每个文档id
    int[] jumps = new int[ArrayUtil.oversize(1, Integer.BYTES * 2)];// 首先用4位来存储，实现在block之间的跳转
    int lastBlock = 0;

    for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.docID()) { // 文档号是递增的
      final int block = doc >>> 16;// 以65536为区间合并 每个block区间docId间隔
      final int upTo = (int) Math.min(Integer.MAX_VALUE, (doc | 0xFFFF) + 1L);// 获取的就是这个区间的65536的最大值
      it.intoBitSet(upTo, buffer, doc & 0xFFFF0000);//连续读取着饿的最大
      blockCardinality = buffer.cardinality();
      jumps = addJumps(jumps, out.getFilePointer() - origo, totalCardinality, lastBlock, block + 1);// 构建block之间的跳表jumper
      lastBlock = block + 1;
      // Flush block
      flush(block, buffer, blockCardinality, denseRankPower, out);//。写入rank（block内的索引结构）
      // Reset for next block
      buffer.clear();
      totalCardinality += blockCardinality;
    }

    // There will always be at least 1 block (NO_MORE_DOCS)
    // Last entry is a SPARSE with blockIndex == 32767 and the single entry 65535, which becomes the
    // docID NO_MORE_DOCS
    // To avoid creating 65K jump-table entries, only a single entry is created pointing to the
    // offset of the NO_MORE_DOCS block, with the jumpBlockIndex set to the logical EMPTY block
    // after all real blocks.
    jumps =// 记录最后一个block的的：block之间的跳表jumper
        addJumps(jumps, out.getFilePointer() - origo, totalCardinality, lastBlock, lastBlock + 1);
    buffer.set(DocIdSetIterator.NO_MORE_DOCS & 0xFFFF); // 将NO_MORE_DOCS置位最后一个block
    flush(DocIdSetIterator.NO_MORE_DOCS >>> 16, buffer, 1, denseRankPower, out);
    // offset+index jump-table stored at the end
    return flushBlockJumps(jumps, lastBlock + 1, out);
  }
  // 每间隔一个区间（65536）
  // Adds entries to the offset & index jump-table for blocks
  private static int[] addJumps(int[] jumps, long offset, int index, int startBlock, int endBlock) {
    assert offset < Integer.MAX_VALUE
        : "Logically the offset should not exceed 2^30 but was >= Integer.MAX_VALUE";
    jumps = ArrayUtil.grow(jumps, (endBlock + 1) * 2);
    for (int b = startBlock; b < endBlock; b++) {// 包含没有任何doc的block，也会记录进来位置
      jumps[b * 2] = index;// 当前segment 当前该block之前写的总文档数（总的block打通统计）
      jumps[b * 2 + 1] = (int) offset; // // 当前segment，当前该block写之前截止写入dvd的偏移量（整个跳表起始为何只）
    }
    return jumps;
  }

  // Flushes the offset & index jump-table for blocks. This should be the last data written to out
  // This method returns the blockCount for the blocks reachable for the jump_table or -1 for no
  // jump-table
  private static short flushBlockJumps(int[] jumps, int blockCount, IndexOutput out)
      throws IOException {
    if (blockCount
        == 2) { // Jumps with a single real entry + NO_MORE_DOCS is just wasted space so we ignore
      // that
      blockCount = 0;
    }
    for (int i = 0; i < blockCount; i++) {
      out.writeInt(jumps[i * 2]); // index shard粒度：截止当前block，总的文档数
      out.writeInt(jumps[i * 2 + 1]); // offset， shard粒度：截止当前blcok的文件偏移量
    }
    // As there are at most 32k blocks, the count is a short
    // The jumpTableOffset will be at lastPos - (blockCount * Long.BYTES)
    return (short) blockCount;
  }

  // Members are pkg-private to avoid synthetic accessors when accessed from the `Method` enum

  /** The slice that stores the {@link DocIdSetIterator}. */
  final IndexInput slice; // 映射docIds中非尾部跳表前面的内容

  final int jumpTableEntryCount; // jumper元素个数为17个
  final byte denseRankPower;// 9
  final RandomAccessInput jumpTable; // Skip blocks of 64K bits   映射jumps部分
  final byte[] denseRankTable;//rank的长度，每个rank占用2 bit。共有128个rank,每8个long产生一个rank
  final long cost;// 存储的总文档数

  /**
   * This constructor always creates a new blockSlice and a new jumpTable from in, to ensure that
   * operations are independent from the caller. See {@link #IndexedDISI(IndexInput,
   * RandomAccessInput, int, byte, long)} for re-use of blockSlice and jumpTable.
   *
   * @param in backing data.
   * @param offset starting offset for blocks in the backing data.
   * @param length the number of bytes holding blocks and jump-table in the backing data.
   * @param jumpTableEntryCount the number of blocks covered by the jump-table. This must match the
   *     number returned by {@link #writeBitSet(DocIdSetIterator, IndexOutput, byte)}.
   * @param denseRankPower the number of docIDs covered by each rank entry in DENSE blocks,
   *     expressed as {@code 2^denseRankPower}. This must match the power given in {@link
   *     #writeBitSet(DocIdSetIterator, IndexOutput, byte)}
   * @param cost normally the number of logical docIDs.
   */
  public IndexedDISI(
      IndexInput in,
      long offset,
      long length,
      int jumpTableEntryCount,
      byte denseRankPower,
      long cost)
      throws IOException {
    this(
        createBlockSlice(in, "docs", offset, length, jumpTableEntryCount),// 没有映射末尾的jumps的内容
        createJumpTable(in, offset, length, jumpTableEntryCount),// 映射jumps的内容
        jumpTableEntryCount,
        denseRankPower,
        cost);
  }

  /**
   * This constructor allows to pass the slice and jumpTable directly in case it helps reuse. see
   * eg. Lucene80 norms producer's merge instance.
   *
   * @param blockSlice data blocks, normally created by {@link #createBlockSlice}.
   * @param jumpTable table holding jump-data for block-skips, normally created by {@link
   *     #createJumpTable}.
   * @param jumpTableEntryCount the number of blocks covered by the jump-table. This must match the
   *     number returned by {@link #writeBitSet(DocIdSetIterator, IndexOutput, byte)}.
   * @param denseRankPower the number of docIDs covered by each rank entry in DENSE blocks,
   *     expressed as {@code 2^denseRankPower}. This must match the power given in {@link
   *     #writeBitSet(DocIdSetIterator, IndexOutput, byte)}
   * @param cost normally the number of logical docIDs.
   */
  IndexedDISI(
      IndexInput blockSlice,
      RandomAccessInput jumpTable,
      int jumpTableEntryCount,
      byte denseRankPower,
      long cost)
      throws IOException {
    if ((denseRankPower < 7 || denseRankPower > 15) && denseRankPower != -1) {
      throw new IllegalArgumentException(
          "Acceptable values for denseRankPower are 7-15 (every 128-32768 docIDs). "
              + "The provided power was "
              + denseRankPower
              + " (every "
              + (int) Math.pow(2, denseRankPower)
              + " docIDs). ");
    }

    this.slice = blockSlice;
    this.jumpTable = jumpTable;
    // Prefetch the first pages of data. Following pages are expected to get prefetched through
    // read-ahead.
    if (slice.length() > 0) {
      slice.prefetch(0, 1);
    }
    if (jumpTable != null && jumpTable.length() > 0) {
      jumpTable.prefetch(0, 1);
    }
    this.jumpTableEntryCount = jumpTableEntryCount;
    this.denseRankPower = denseRankPower;//9 （每512个间隔，使用一个rank）
    final int rankIndexShift = denseRankPower - 7;// 每个rank占用2 bite
    this.denseRankTable =
        denseRankPower == -1 ? null : new byte[DENSE_BLOCK_LONGS >> rankIndexShift]; // rank长度为256bit=(65536/512*2)
    this.cost = cost;
  }

  /**
   * Helper method for using {@link #IndexedDISI(IndexInput, RandomAccessInput, int, byte, long)}.
   * Creates a disiSlice for the IndexedDISI data blocks, without the jump-table.
   *
   * @param slice backing data, holding both blocks and jump-table.
   * @param sliceDescription human readable slice designation.
   * @param offset relative to the backing data.
   * @param length full length of the IndexedDISI, including blocks and jump-table data.
   * @param jumpTableEntryCount the number of blocks covered by the jump-table.
   * @return a jumpTable containing the block jump-data or null if no such table exists.
   * @throws IOException if a RandomAccessInput could not be created from slice.
   */
  public static IndexInput createBlockSlice(
      IndexInput slice, String sliceDescription, long offset, long length, int jumpTableEntryCount)
      throws IOException {
    long jumpTableBytes = jumpTableEntryCount < 0 ? 0 : jumpTableEntryCount * Integer.BYTES * 2;
    return slice.slice(sliceDescription, offset, length - jumpTableBytes);
  } // 将进入 MemorySegmentIndexInput$SingleSegmentImpl

  /**
   * Helper method for using {@link #IndexedDISI(IndexInput, RandomAccessInput, int, byte, long)}.
   * Creates a RandomAccessInput covering only the jump-table data or null.
   *
   * @param slice backing data, holding both blocks and jump-table.
   * @param offset relative to the backing data.
   * @param length full length of the IndexedDISI, including blocks and jump-table data.
   * @param jumpTableEntryCount the number of blocks covered by the jump-table.
   * @return a jumpTable containing the block jump-data or null if no such table exists.
   * @throws IOException if a RandomAccessInput could not be created from slice.
   */
  public static RandomAccessInput createJumpTable(
      IndexInput slice, long offset, long length, int jumpTableEntryCount) throws IOException {
    if (jumpTableEntryCount <= 0) {
      return null;
    } else {
      int jumpTableBytes = jumpTableEntryCount * Integer.BYTES * 2;
      return slice.randomAccessSlice(offset + length - jumpTableBytes, jumpTableBytes);
    }
  }

  int block = -1;// 当前正在读取到的blockdId<<16位
  long blockEnd;// 下个block的起始位置
  long denseBitmapOffset = -1; // Only used for DENSE blocks 跑到存放1024个long的long区起始位置
  int nextBlockIndex = -1; // 截止到目前block（包含），读取到多少个docId-1
  Method method;// 稀疏还是稠密bitmap

  int index = -1;// 统计所有block合起来，截止到当前是第几个写入的。在全局排序中需要根据该segment第几个写入的，判断对应的ordId，以获取到全局globalId
  // index是读取到的文档个数-1，也就是下标个数
  // SPARSE variables
  boolean exists;
  int nextExistDocInBlock = -1;

  // DENSE variables
  long word;// 当前wordIndex对应的long的值(读取roarmap中的一个，1024个long中的一个)
  int wordIndex = -1;// 该block 1024个读取到的是第几个workd。// 这个文档所在rank在word的起始下标
  // number of one bits encountered so far, including those of `word`
  int numberOfOnes; // 截止到目前wordIndex（包含当前wordIndex），所有long读取到多少个docId
  // Used with rank for jumps inside of DENSE as they are absolute instead of relative
  int denseOrigoIndex;// 截止到目前block（不包含当前block），所有block读取到多少个docId。仅当移动block才会修改。
  FixedBitSet bitSet;

  // ALL variables
  int gap;

  /**
   * Returns an iterator that delegates to the IndexedDISI. Advancing this iterator will advance the
   * underlying IndexedDISI, and vice-versa.
   */
  public static KnnVectorValues.DocIndexIterator asDocIndexIterator(IndexedDISI disi) {
    // can we replace with fromDISI?
    return new KnnVectorValues.DocIndexIterator() {
      @Override
      public int docID() {
        return disi.docID();
      }

      @Override
      public int index() {
        return disi.index();
      }

      @Override
      public int nextDoc() throws IOException {
        return disi.nextDoc();
      }

      @Override
      public int advance(int target) throws IOException {
        return disi.advance(target);
      }

      @Override
      public long cost() {
        return disi.cost();
      }
    };
  }

  @Override
  public int advance(int target) throws IOException {
    final int targetBlock = target & 0xFFFF0000;// >> 16
    if (block < targetBlock) {// 不是同一个block
      advanceBlock(targetBlock);
    }
    if (block == targetBlock) {// 若在同一个block了
      if (method.advanceWithinBlock(this, target)) {// 返回该doc是否存在
        return doc;
      }
      readBlockHeader();
    }
    boolean found = method.advanceWithinBlock(this, block);
    assert found;
    return doc;
  }

  @Override
  public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
    assert doc >= offset : "offset=" + offset + " doc=" + doc;
    while (doc < upTo && method.intoBitSetWithinBlock(this, upTo, bitSet, offset) == false) {
      readBlockHeader();
      boolean found = method.advanceWithinBlock(this, block);
      assert found;
    }
  }

  public boolean advanceExact(int target) throws IOException {
    final int targetBlock = target & 0xFFFF0000;
    if (block < targetBlock) {
      advanceBlock(targetBlock);
    }
    boolean found = block == targetBlock && method.advanceExactWithinBlock(this, target);
    this.doc = target;
    return found;
  }

  private void advanceBlock(int targetBlock) throws IOException {
    final int blockIndex = targetBlock >> 16;// 属于哪个block
    // If the destination block is 2 blocks or more ahead, we use the jump-table.
    if (jumpTable != null && blockIndex >= (block >> 16) + 2) {
      // If the jumpTableEntryCount is exceeded, there are no further bits. Last entry is always
      // NO_MORE_DOCS
      final int inRangeBlockIndex = // block下表
          blockIndex < jumpTableEntryCount ? blockIndex : jumpTableEntryCount - 1;
      final int index = jumpTable.readInt(inRangeBlockIndex * (long) Integer.BYTES * 2);// 先读取这个的index
      final int offset =
          jumpTable.readInt(inRangeBlockIndex * (long) Integer.BYTES * 2 + Integer.BYTES);
      this.nextBlockIndex = index - 1; // -1 to compensate for the always-added 1 in readBlockHeader
      slice.seek(offset);// 该block的rank的起始位置
      readBlockHeader();
      return;
    }

    // Fallback to iteration of blocks
    do {
      slice.seek(blockEnd);
      readBlockHeader();
    } while (block < targetBlock);
  }

  private void readBlockHeader() throws IOException {
    block = Short.toUnsignedInt(slice.readShort()) << 16;
    assert block >= 0;
    final int numValues = 1 + Short.toUnsignedInt(slice.readShort());// 是稀疏矩阵
    index = nextBlockIndex;
    nextBlockIndex = index + numValues;
    if (numValues <= MAX_ARRAY_LENGTH) {
      method = Method.SPARSE;
      blockEnd = slice.getFilePointer() + (numValues << 1);
      nextExistDocInBlock = -1;
    } else if (numValues == BLOCK_SIZE) {
      method = Method.ALL;
      blockEnd = slice.getFilePointer();
      gap = block - index - 1;
    } else {
      method = Method.DENSE;
      denseBitmapOffset =
          slice.getFilePointer() + (denseRankTable == null ? 0 : denseRankTable.length);
      blockEnd = denseBitmapOffset + (1 << 13);
      // Performance consideration: All rank (default 128 * 16 bits) are loaded up front. This
      // should be fast with the
      // reusable byte[] buffer, but it is still wasted if the DENSE block is iterated in small
      // steps.
      // If this results in too great a performance regression, a heuristic strategy might work
      // where the rank data
      // are loaded on first in-block advance, if said advance is > X docIDs. The hope being that a
      // small first
      // advance means that subsequent advances will be small too.
      // Another alternative is to maintain an extra slice for DENSE rank, but IndexedDISI is
      // already slice-heavy.
      if (denseRankPower != -1) {
        slice.readBytes(denseRankTable, 0, denseRankTable.length);
      }
      wordIndex = -1;
      numberOfOnes = index + 1;
      denseOrigoIndex = numberOfOnes;
    }
  }

  @Override
  public int nextDoc() throws IOException {
    return advance(doc + 1);
  }

  public int index() {
    return index;
  }

  @Override
  public long cost() {
    return cost;
  }

  @Override
  public int docIDRunEnd() throws IOException {
    return method.docIDRunEnd(this);
  }

  enum Method {
    SPARSE {
      @Override
      boolean advanceWithinBlock(IndexedDISI disi, int target) throws IOException {
        final int targetInBlock = target & 0xFFFF;
        // TODO: binary search
        for (; disi.index < disi.nextBlockIndex; ) {
          int doc = Short.toUnsignedInt(disi.slice.readShort());
          disi.index++;
          if (doc >= targetInBlock) {
            disi.doc = disi.block | doc;
            disi.exists = true;
            disi.nextExistDocInBlock = doc;
            return true;
          }
        }
        return false;
      }

      @Override
      boolean advanceExactWithinBlock(IndexedDISI disi, int target) throws IOException {
        final int targetInBlock = target & 0xFFFF;
        // TODO: binary search
        if (disi.nextExistDocInBlock > targetInBlock) {
          assert !disi.exists;
          return false;
        }
        if (target == disi.doc) {
          return disi.exists;
        }
        for (; disi.index < disi.nextBlockIndex; ) {
          int doc = Short.toUnsignedInt(disi.slice.readShort());
          disi.index++;
          if (doc >= targetInBlock) {
            disi.nextExistDocInBlock = doc;
            if (doc != targetInBlock) {
              disi.index--;
              disi.slice.seek(disi.slice.getFilePointer() - Short.BYTES);
              break;
            }
            disi.exists = true;
            return true;
          }
        }
        disi.exists = false;
        return false;
      }

      @Override
      boolean intoBitSetWithinBlock(IndexedDISI disi, int upTo, FixedBitSet bitSet, int offset)//稀疏矩阵
          throws IOException {
        bitSet.set(disi.doc - offset);
        for (; disi.index < disi.nextBlockIndex; ) {
          int docInBlock = disi.slice.readShort() & 0xFFFF;
          int doc = disi.block | docInBlock;
          disi.index++;
          if (doc >= upTo) {
            disi.doc = doc;
            disi.exists = true;
            disi.nextExistDocInBlock = docInBlock;
            return true;
          }
          bitSet.set(doc - offset);
        }
        return false;
      }

      @Override
      int docIDRunEnd(IndexedDISI disi) throws IOException {
        return disi.doc + 1;
      }
    },
    DENSE {// 找到下一个，大于等于该target的docId
      @Override
      boolean advanceWithinBlock(IndexedDISI disi, int target) throws IOException {//已经定位到了一个block内了(1<<16的文档内)，target是文档id
        final int targetInBlock = target & 0xFFFF;// 取低16位
        final int targetWordIndex = targetInBlock >>> 6;// 确定rank内在第几个word

        // If possible, skip ahead using the rank cache
        // If the distance between the current position and the target is < rank-longs
        // there is no sense in using rank
        if (disi.denseRankPower != -1
            && targetWordIndex - disi.wordIndex >= (1 << (disi.denseRankPower - 6))) {// 如果当前基础word（disi.targetWordIndex）和targetWordIndex大于8个word（一个rank）
          rankSkip(disi, targetInBlock);// 那么就需要跳到对应的rank
        }

        for (int i = disi.wordIndex + 1; i <= targetWordIndex; ++i) {// 从这个rank第二个work开始读取
          disi.word = disi.slice.readLong();// 读取每个rank
          disi.numberOfOnes += Long.bitCount(disi.word);
        }
        disi.wordIndex = targetWordIndex;

        long leftBits = disi.word >>> target;
        if (leftBits != 0L) {// 说明存在
          disi.doc = target + Long.numberOfTrailingZeros(leftBits);// 找到下一个大于等于该target的值，若target=doc，那么Long.numberOfTrailingZeros(leftBits)=0
          disi.index = disi.numberOfOnes - Long.bitCount(leftBits);
          return true;// doc是否存在
        }

        // There were no set bits at the wanted position. Move forward until one is reached
        while (++disi.wordIndex < 1024) {
          // This could use the rank cache to skip empty spaces >= 512 bits, but it seems
          // unrealistic
          // that such blocks would be DENSE
          disi.word = disi.slice.readLong();
          if (disi.word != 0) {
            disi.index = disi.numberOfOnes;
            disi.numberOfOnes += Long.bitCount(disi.word);
            disi.doc = disi.block | (disi.wordIndex << 6) | Long.numberOfTrailingZeros(disi.word);
            return true;
          }
        }
        // No set bits in the block at or after the wanted position.
        return false;
      }
      // 看target是否存在
      @Override
      boolean advanceExactWithinBlock(IndexedDISI disi, int target) throws IOException {
        final int targetInBlock = target & 0xFFFF;
        final int targetWordIndex = targetInBlock >>> 6;

        // If possible, skip ahead using the rank cache
        // If the distance between the current position and the target is < rank-longs
        // there is no sense in using rank
        if (disi.denseRankPower != -1
            && targetWordIndex - disi.wordIndex >= (1 << (disi.denseRankPower - 6))) {
          rankSkip(disi, targetInBlock);
        }

        for (int i = disi.wordIndex + 1; i <= targetWordIndex; ++i) {
          disi.word = disi.slice.readLong();
          disi.numberOfOnes += Long.bitCount(disi.word);
        }
        disi.wordIndex = targetWordIndex;

        long leftBits = disi.word >>> target;
        disi.index = disi.numberOfOnes - Long.bitCount(leftBits);
        return (leftBits & 1L) != 0;
      }

      @Override
      boolean intoBitSetWithinBlock(IndexedDISI disi, int upTo, FixedBitSet bitSet, int offset)
          throws IOException {
        if (disi.bitSet == null) {
          disi.bitSet = new FixedBitSet(BLOCK_SIZE);
        }
        int destFrom = disi.doc - offset;
        int destTo = MathUtil.unsignedMin(upTo, offset + bitSet.length());
        int sourceFrom = disi.doc & 0xFFFF;
        int sourceTo = Math.min(destTo - disi.block, BLOCK_SIZE);

        long fp = disi.slice.getFilePointer();
        disi.slice.seek(fp - Long.BYTES); // seek back a long to include current word (disi.word).
        int numWords = FixedBitSet.bits2words(sourceTo) - disi.wordIndex;
        disi.slice.readLongs(disi.bitSet.getBits(), disi.wordIndex, numWords);
        FixedBitSet.orRange(disi.bitSet, sourceFrom, bitSet, destFrom, sourceTo - sourceFrom);

        int blockEnd = disi.block | 0xFFFF;
        if (destTo > blockEnd) {
          disi.slice.seek(disi.blockEnd);
          disi.index += disi.bitSet.cardinality(sourceFrom, sourceTo);
          return false;
        } else {
          disi.slice.seek(fp);
          boolean found = advanceWithinBlock(disi, destTo);
          if (found && disi.doc < upTo) {
            throw new IllegalStateException(
                "There are bits set in the source bitset that are not accounted for."
                    + " doc="
                    + disi.doc
                    + " upTo="
                    + upTo
                    + " disi.block="
                    + disi.block);
          }
          return found;
        }
      }

      @Override
      int docIDRunEnd(IndexedDISI disi) throws IOException {
        if (disi.word == -1L) {
          return (disi.doc | 0x3F) + 1;
        }
        return disi.doc + 1;
      }
    },
    ALL {
      @Override
      boolean advanceWithinBlock(IndexedDISI disi, int target) {
        disi.doc = target;
        disi.index = target - disi.gap;
        return true;
      }

      @Override
      boolean advanceExactWithinBlock(IndexedDISI disi, int target) {
        disi.index = target - disi.gap;
        return true;
      }

      @Override
      boolean intoBitSetWithinBlock(IndexedDISI disi, int upTo, FixedBitSet bitSet, int offset) {
        final int blockEnd = disi.block | 0xFFFF;
        if (upTo <= blockEnd) {
          bitSet.set(disi.doc - offset, upTo - offset);
          advanceWithinBlock(disi, upTo);
          return true;
        } else {
          bitSet.set(disi.doc - offset, blockEnd - offset + 1);
          return false;
        }
      }

      @Override
      int docIDRunEnd(IndexedDISI disi) throws IOException {
        return (disi.doc | 0xFFFF) + 1;
      }
    };

    /**
     * Advance to the first doc from the block that is equal to or greater than {@code target}.
     * Return true if there is such a doc and false otherwise.
     */
    abstract boolean advanceWithinBlock(IndexedDISI disi, int target) throws IOException;

    /**
     * Advance the iterator exactly to the position corresponding to the given {@code target} and
     * return whether this document exists.
     */
    abstract boolean advanceExactWithinBlock(IndexedDISI disi, int target) throws IOException;

    /**
     * Similar to {@link DocIdSetIterator#intoBitSet}, load docs in this block into a bitset. This
     * method returns true if there are remaining docs (gte upTo) in the block, otherwise false.
     * When false return, fp of {@link IndexedDISI#slice} is at {@link IndexedDISI#blockEnd} and
     * {@link IndexedDISI#index} is correct but other status vars are undefined. Caller should
     * decode the header of next block by {@link #readBlockHeader()}.
     *
     * <p>Caller need to make sure {@link IndexedDISI#doc} greater than or equals to {@link
     * IndexedDISI#block} and less than {@code upTo} when calling this.
     */
    abstract boolean intoBitSetWithinBlock(
        IndexedDISI disi, int upTo, FixedBitSet bitSet, int offset) throws IOException;

    abstract int docIDRunEnd(IndexedDISI disi) throws IOException;
  }

  /**
   * If the distance between the current position and the target is > 8 words, the rank cache will
   * be used to guarantee a worst-case of 1 rank-lookup and 7 word-read-and-count-bits operations.
   * Note: This does not guarantee a skip up to target, only up to nearest rank boundary. It is the
   * responsibility of the caller to iterate further to reach target.
   *
   * @param disi standard DISI.
   * @param targetInBlock lower 16 bits of the target
   * @throws IOException if a DISI seek failed.
   */
  private static void rankSkip(IndexedDISI disi, int targetInBlock) throws IOException {
    assert disi.denseRankPower >= 0 : disi.denseRankPower;
    // Resolve the rank as close to targetInBlock as possible (maximum distance is 8 longs)
    // Note: rankOrigoOffset is tracked on block open, so it is absolute (e.g. don't add origo)
    final int rankIndex =// 属于哪个rank
        targetInBlock >> disi.denseRankPower; // Default is 9 (8 longs: 2^3 * 2^6 = 512 docIDs)

    final int rank =// 获取的是这个rank
        (disi.denseRankTable[rankIndex << 1] & 0xFF) << 8
            | (disi.denseRankTable[(rankIndex << 1) + 1] & 0xFF);

    // Position the counting logic just after the rank point
    final int rankAlignedWordIndex = rankIndex << disi.denseRankPower >> 6;// 跳过多少个rank，帮助读取具体的word。一个rank 8个long(=disi.denseRankPower >> 6)
    disi.slice.seek(disi.denseBitmapOffset + rankAlignedWordIndex * (long) Long.BYTES); // 跳到存放1024个long，这个rank存放word的起始位置
    long rankWord = disi.slice.readLong();// 获取这个rank的第一个word
    int denseNOO = rank + Long.bitCount(rankWord);// 截止到这个rank，总的文档数

    disi.wordIndex = rankAlignedWordIndex;// 这个文档所在rank在word的起始下标
    disi.word = rankWord;
    disi.numberOfOnes = disi.denseOrigoIndex + denseNOO;
  }
}
