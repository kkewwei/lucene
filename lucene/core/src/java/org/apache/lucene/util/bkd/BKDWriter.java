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
package org.apache.lucene.util.bkd;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.IntFunction;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.MutablePointValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.GrowableByteArrayDataOutput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.store.TrackingDirectoryWrapper;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.FutureArrays;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.PriorityQueue;

// TODO
//   - allow variable length byte[] (across docs and dims), but this is quite a bit more hairy
//   - we could also index "auto-prefix terms" here, and use better compression, and maybe only use for the "fully contained" case so we'd
//     only index docIDs
//   - the index could be efficiently encoded as an FST, so we don't have wasteful
//     (monotonic) long[] leafBlockFPs; or we could use MonotonicLongValues ... but then
//     the index is already plenty small: 60M OSM points --> 1.1 MB with 128 points
//     per leaf, and you can reduce that by putting more points per leaf
//   - we could use threads while building; the higher nodes are very parallelizable

/**
 *  Recursively builds a block KD-tree to assign all incoming points in N-dim space to smaller
 *  and smaller N-dim rectangles (cells) until the number of points in a given
 *  rectangle is &lt;= <code>maxPointsInLeafNode</code>.  The tree is
 *  partially balanced, which means the leaf nodes will have
 *  the requested <code>maxPointsInLeafNode</code> except one that might have less.
 *  Leaf nodes may straddle the two bottom levels of the binary tree.
 *  Values that fall exactly on a cell boundary may be in either cell.
 *
 *  <p>The number of dimensions can be 1 to 8, but every byte[] value is fixed length.
 *
 *  <p>This consumes heap during writing: it allocates a <code>Long[numLeaves]</code>,
 *  a <code>byte[numLeaves*(1+bytesPerDim)]</code> and then uses up to the specified
 *  {@code maxMBSortInHeap} heap space for writing.
 *
 *  <p>
 *  <b>NOTE</b>: This can write at most Integer.MAX_VALUE * <code>maxPointsInLeafNode</code> / bytesPerDim
 *  total points.
 *
 * @lucene.experimental */
  // 每个维度都会建立一个, pint结构
public class BKDWriter implements Closeable {

  public static final String CODEC_NAME = "BKD";
  public static final int VERSION_START = 4; // version used by Lucene 7.0
  //public static final int VERSION_CURRENT = VERSION_START;
  public static final int VERSION_LEAF_STORES_BOUNDS = 5;
  public static final int VERSION_SELECTIVE_INDEXING = 6;
  public static final int VERSION_LOW_CARDINALITY_LEAVES = 7; // version_low_cardinality_leaves = 7
  public static final int VERSION_META_FILE = 9;
  public static final int VERSION_CURRENT = VERSION_META_FILE;

  /** How many bytes each docs takes in the fixed-width offline format */
  private final int bytesPerDoc;

  /** Default maximum number of point in each leaf block */
  public static final int DEFAULT_MAX_POINTS_IN_LEAF_NODE = 512; // 每个叶子block中文档最大个数

  /** Default maximum heap to use, before spilling to (slower) disk */
  public static final float DEFAULT_MAX_MB_SORT_IN_HEAP = 16.0f; // 内存占用最大size

  /** Maximum number of index dimensions (2 * max index dimensions) */
  public static final int MAX_DIMS = 16;

  /** Maximum number of index dimensions */
  public static final int MAX_INDEX_DIMS = 8; // dims

  /** Number of splits before we compute the exact bounding box of an inner node. */
  private static final int SPLITS_BEFORE_EXACT_BOUNDS = 4; // splits_before_exact_bound // 多少次拆分才能计算一个内部节点

  /** How many dimensions we are storing at the leaf (data) nodes */
  protected final int numDataDims; // 在叶子节点，每个元素几个维度

  /** How many dimensions we are indexing in the internal nodes */
  protected final int numIndexDims;// // 在叶子节点，每个元素几个维度

  /** How many bytes each value in each dimension takes. */
  protected final int bytesPerDim; // 单个元素某个维度占用的长度

  /** numDataDims * bytesPerDim */
  protected final int packedBytesLength; // 每个point占用的长度

  /** numIndexDims * bytesPerDim */
  protected final int packedIndexBytesLength;

  final TrackingDirectoryWrapper tempDir;
  final String tempFileNamePrefix;
  final double maxMBSortInHeap; // 内存中最多占用的空间

  final byte[] scratchDiff;
  final byte[] scratch1; // 每个point，就是个临时变量
  final byte[] scratch2;
  final BytesRef scratchBytesRef1 = new BytesRef(); //临时变量，共享使用，避免重复申请
  final BytesRef scratchBytesRef2 = new BytesRef();
  final int[] commonPrefixLengths; // 一个叶子的所有points每个域相同的前缀，分开统计

  protected final FixedBitSet docsSeen; // 涉及到多少文档Id

  private PointWriter pointWriter;
  private boolean finished;

  private IndexOutput tempInput;
  protected final int maxPointsInLeafNode;  // 默认512
  private final int maxPointsSortInHeap;

  /** Minimum per-dim values, packed */
  protected final byte[] minPackedValue;  //  每个维度的最小值。存放的每个维度之间取得不是一是一个point的

  /** Maximum per-dim values, packed */
  protected final byte[] maxPackedValue; //  每个维度的最大值

  protected long pointCount; // 这个segment中，包含几个point

  /** An upper bound on how many points the caller will add (includes deletions) */
  private final long totalPointCount;

  private final int maxDoc; // 最大的文档编号

  public BKDWriter(int maxDoc, Directory tempDir, String tempFileNamePrefix, int numDataDims, int numIndexDims, int bytesPerDim,
                   int maxPointsInLeafNode, double maxMBSortInHeap, long totalPointCount) throws IOException {
    verifyParams(numDataDims, numIndexDims, maxPointsInLeafNode, maxMBSortInHeap, totalPointCount);
    // We use tracking dir to deal with removing files on exception, so each place that
    // creates temp files doesn't need crazy try/finally/sucess logic:
    this.tempDir = new TrackingDirectoryWrapper(tempDir);
    this.tempFileNamePrefix = tempFileNamePrefix; // 临时文件的前缀: _12. segment编号
    this.maxPointsInLeafNode = maxPointsInLeafNode;
    this.numDataDims = numDataDims;
    this.numIndexDims = numIndexDims;
    this.bytesPerDim = bytesPerDim;
    this.totalPointCount = totalPointCount;
    this.maxDoc = maxDoc;
    docsSeen = new FixedBitSet(maxDoc);
    packedBytesLength = numDataDims * bytesPerDim;//  一个point占用的原始空间 = 单个value*维度
    packedIndexBytesLength = numIndexDims * bytesPerDim; //  一个point占用的空间

    scratchDiff = new byte[bytesPerDim];  // 一个value元素占用的空间
    scratch1 = new byte[packedBytesLength]; // 一个point占用的空间
    scratch2 = new byte[packedBytesLength];
    commonPrefixLengths = new int[numDataDims]; // 一个point包含几个元素

    minPackedValue = new byte[packedIndexBytesLength]; // 数字型，有最大值和最小值。到时会统计该批数据每个维度的最大值和最小值
    maxPackedValue = new byte[packedIndexBytesLength];

    // dimensional values (numDims * bytesPerDim) + docID (int)
    bytesPerDoc = packedBytesLength + Integer.BYTES;
    // 最多只能有这么多节点存在于内存中
    // Maximum number of points we hold in memory at any time
    maxPointsSortInHeap = (int) ((maxMBSortInHeap * 1024 * 1024) / (bytesPerDoc)); // 最多在内存中有多少个Point

    // Finally, we must be able to hold at least the leaf node in heap during build:
    if (maxPointsSortInHeap < maxPointsInLeafNode) { // 若超过上限，就直接抛异常
      throw new IllegalArgumentException("maxMBSortInHeap=" + maxMBSortInHeap + " only allows for maxPointsSortInHeap=" + maxPointsSortInHeap + ", but this is less than maxPointsInLeafNode=" + maxPointsInLeafNode + "; either increase maxMBSortInHeap or decrease maxPointsInLeafNode");
    }

    this.maxMBSortInHeap = maxMBSortInHeap;
  }

  public static void verifyParams(int numDims, int numIndexDims, int maxPointsInLeafNode, double maxMBSortInHeap, long totalPointCount) {
    // We encode dim in a single byte in the splitPackedValues, but we only expose 4 bits for it now, in case we want to use
    // remaining 4 bits for another purpose later
    if (numDims < 1 || numDims > MAX_DIMS) {
      throw new IllegalArgumentException("numDims must be 1 .. " + MAX_DIMS + " (got: " + numDims + ")");
    }
    if (numIndexDims < 1 || numIndexDims > MAX_INDEX_DIMS) {
      throw new IllegalArgumentException("numIndexDims must be 1 .. " + MAX_INDEX_DIMS + " (got: " + numIndexDims + ")");
    }
    if (numIndexDims > numDims) {
      throw new IllegalArgumentException("numIndexDims cannot exceed numDims (" + numDims + ") (got: " + numIndexDims + ")");
    }
    if (maxPointsInLeafNode <= 0) {
      throw new IllegalArgumentException("maxPointsInLeafNode must be > 0; got " + maxPointsInLeafNode);
    }
    if (maxPointsInLeafNode > ArrayUtil.MAX_ARRAY_LENGTH) {
      throw new IllegalArgumentException("maxPointsInLeafNode must be <= ArrayUtil.MAX_ARRAY_LENGTH (= " + ArrayUtil.MAX_ARRAY_LENGTH + "); got " + maxPointsInLeafNode);
    }
    if (maxMBSortInHeap < 0.0) {
      throw new IllegalArgumentException("maxMBSortInHeap must be >= 0.0 (got: " + maxMBSortInHeap + ")");
    }
    if (totalPointCount < 0) {
      throw new IllegalArgumentException("totalPointCount must be >=0 (got: " + totalPointCount + ")");
    }
  }

  private void initPointWriter() throws IOException {
    assert pointWriter == null : "Point writer is already initialized";
    //total point count is an estimation but the final point count must be equal or lower to that number.
    if (totalPointCount > maxPointsSortInHeap) {
      pointWriter = new OfflinePointWriter(tempDir, tempFileNamePrefix, packedBytesLength, "spill", 0);
      tempInput = ((OfflinePointWriter)pointWriter).out;
    } else {
      pointWriter = new HeapPointWriter(Math.toIntExact(totalPointCount), packedBytesLength);
    }
  }


  public void add(byte[] packedValue, int docID) throws IOException {
    if (packedValue.length != packedBytesLength) {
      throw new IllegalArgumentException("packedValue should be length=" + packedBytesLength + " (got: " + packedValue.length + ")");
    }
    if (pointCount >= totalPointCount) {
      throw new IllegalStateException("totalPointCount=" + totalPointCount + " was passed when we were created, but we just hit " + (pointCount + 1) + " values");
    }
    if (pointCount == 0) {
      initPointWriter();
      System.arraycopy(packedValue, 0, minPackedValue, 0, packedIndexBytesLength);
      System.arraycopy(packedValue, 0, maxPackedValue, 0, packedIndexBytesLength);
    } else {
      for(int dim=0;dim<numIndexDims;dim++) { // 遍历所有维度
        int offset = dim*bytesPerDim;
        if (FutureArrays.compareUnsigned(packedValue, offset, offset + bytesPerDim, minPackedValue, offset, offset + bytesPerDim) < 0) {
          System.arraycopy(packedValue, offset, minPackedValue, offset, bytesPerDim);
        }
        if (FutureArrays.compareUnsigned(packedValue, offset, offset + bytesPerDim, maxPackedValue, offset, offset + bytesPerDim) > 0) {
          System.arraycopy(packedValue, offset, maxPackedValue, offset, bytesPerDim);
        }
      }
    }
    pointWriter.append(packedValue, docID);
    pointCount++;
    docsSeen.set(docID);
  }

  private static class MergeReader {
    final BKDReader bkd;
    final BKDReader.IntersectState state;
    final MergeState.DocMap docMap;

    /** Current doc ID */
    public int docID;

    /** Which doc in this block we are up to */
    private int docBlockUpto;

    /** How many docs in the current block */
    private int docsInBlock;

    /** Which leaf block we are up to */
    private int blockID;

    private final byte[] packedValues;

    public MergeReader(BKDReader bkd, MergeState.DocMap docMap) throws IOException {
      this.bkd = bkd;
      state = new BKDReader.IntersectState(bkd.in.clone(),
              bkd.numDataDims,
              bkd.packedBytesLength,
              bkd.packedIndexBytesLength,
              bkd.maxPointsInLeafNode,
              null,
              null);
      this.docMap = docMap;
      state.in.seek(bkd.getMinLeafBlockFP());
      this.packedValues = new byte[bkd.maxPointsInLeafNode * bkd.packedBytesLength];
    }

    public boolean next() throws IOException {
      //System.out.println("MR.next this=" + this);
      while (true) {
        if (docBlockUpto == docsInBlock) {
          if (blockID == bkd.leafNodeOffset) {
            //System.out.println("  done!");
            return false;
          }
          //System.out.println("  new block @ fp=" + state.in.getFilePointer());
          docsInBlock = bkd.readDocIDs(state.in, state.in.getFilePointer(), state.scratchIterator);
          assert docsInBlock > 0;
          docBlockUpto = 0;
          bkd.visitDocValues(state.commonPrefixLengths, state.scratchDataPackedValue, state.scratchMinIndexPackedValue, state.scratchMaxIndexPackedValue, state.in, state.scratchIterator, docsInBlock, new IntersectVisitor() {
            int i = 0;

            @Override
            public void visit(int docID) {
              throw new UnsupportedOperationException();
            }

            @Override
            public void visit(int docID, byte[] packedValue) {
              assert docID == state.scratchIterator.docIDs[i];
              System.arraycopy(packedValue, 0, packedValues, i * bkd.packedBytesLength, bkd.packedBytesLength);
              i++;
            }

            @Override
            public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
              return Relation.CELL_CROSSES_QUERY;
            }

          });

          blockID++;
        }

        final int index = docBlockUpto++;
        int oldDocID = state.scratchIterator.docIDs[index];

        int mappedDocID;
        if (docMap == null) {
          mappedDocID = oldDocID;
        } else {
          mappedDocID = docMap.get(oldDocID);
        }

        if (mappedDocID != -1) {
          // Not deleted!
          docID = mappedDocID;
          System.arraycopy(packedValues, index * bkd.packedBytesLength, state.scratchDataPackedValue, 0, bkd.packedBytesLength);
          return true;
        }
      }
    }
  }

  private static class BKDMergeQueue extends PriorityQueue<MergeReader> {
    private final int bytesPerDim;

    public BKDMergeQueue(int bytesPerDim, int maxSize) {
      super(maxSize);
      this.bytesPerDim = bytesPerDim;
    }

    @Override
    public boolean lessThan(MergeReader a, MergeReader b) {
      assert a != b;

      int cmp = FutureArrays.compareUnsigned(a.state.scratchDataPackedValue, 0, bytesPerDim, b.state.scratchDataPackedValue, 0, bytesPerDim);
      if (cmp < 0) {
        return true;
      } else if (cmp > 0) {
        return false;
      }

      // Tie break by sorting smaller docIDs earlier:
      return a.docID < b.docID;
    }
  }

  /** flat representation of a kd-tree */
  private interface BKDTreeLeafNodes {
    /** number of leaf nodes */
    int numLeaves();
    /** pointer to the leaf node previously written. Leaves are order from
     * left to right, so leaf at {@code index} 0 is the leftmost leaf and
     * the the leaf at {@code numleaves()} -1 is the rightmost leaf */
    long getLeafLP(int index);
    /** split value between two leaves. The split value at position n corresponds to the
     *  leaves at (n -1) and n. */
    BytesRef getSplitValue(int index);
    /** split dimension between two leaves. The split dimension at position n corresponds to the
     *  leaves at (n -1) and n.*/
    int getSplitDimension(int index);
  }

  /** Write a field from a {@link MutablePointValues}. This way of writing
   *  points is faster than regular writes with {@link BKDWriter#add} since
   *  there is opportunity for reordering points before writing them to
   *  disk. This method does not use transient disk in order to reorder points.
   */
  public Runnable writeField(IndexOutput metaOut, IndexOutput indexOut, IndexOutput dataOut, String fieldName, MutablePointValues reader) throws IOException {
    if (numDataDims == 1) {// Point只有一个数值
      return writeField1Dim(metaOut, indexOut, dataOut, fieldName, reader);
    } else {//Point有两个数值
      return writeFieldNDims(metaOut, indexOut, dataOut, fieldName, reader);
    }
  }
  // 将每个维度的最大值，最小值，全部保存在minPackedValue和maxPackedValue中。
  private void computePackedValueBounds(MutablePointValues values, int from, int to, byte[] minPackedValue, byte[] maxPackedValue, BytesRef scratch) {
    if (from == to) {
      return;
    }
    values.getValue(from, scratch); // 从values中读取一个pont出来放入scratch
    System.arraycopy(scratch.bytes, scratch.offset, minPackedValue, 0, packedIndexBytesLength);// 每个维度最大值
    System.arraycopy(scratch.bytes, scratch.offset, maxPackedValue, 0, packedIndexBytesLength); // 最小值，
    for (int i = from + 1 ; i < to; ++i) {// 遍历所有节点
      values.getValue(i, scratch); // 读取的一个值
      for(int dim = 0; dim < numIndexDims; dim++) {// 遍历每个节点每个维度最大值，最小值
        final int startOffset = dim * bytesPerDim;// 获取该唯独的最小值
        final int endOffset = startOffset + bytesPerDim;
        if (FutureArrays.compareUnsigned(scratch.bytes, scratch.offset + startOffset, scratch.offset + endOffset, minPackedValue, startOffset, endOffset) < 0) {
          System.arraycopy(scratch.bytes, scratch.offset + startOffset, minPackedValue, startOffset, bytesPerDim);
        } else if (FutureArrays.compareUnsigned(scratch.bytes, scratch.offset + startOffset, scratch.offset + endOffset, maxPackedValue, startOffset, endOffset) > 0) {//获取该维度的最大值
          System.arraycopy(scratch.bytes, scratch.offset + startOffset, maxPackedValue, startOffset, bytesPerDim);
        }
      }
    }
  }

  /* In the 2+D case, we recursively pick the split dimension, compute the
     * median value and partition other values around it. */
    private Runnable writeFieldNDims(IndexOutput metaOut, IndexOutput indexOut, IndexOutput dataOut, String fieldName, MutablePointValues values) throws IOException {
      if (pointCount != 0) {
        throw new IllegalStateException("cannot mix add and writeField");
      }

      // Catch user silliness:
      if (finished == true) {
        throw new IllegalStateException("already finished");
      }

      // Mark that we already finished:
      finished = true;

      pointCount = values.size();//
      // 多少个叶子节点
      final int numLeaves = Math.toIntExact((pointCount + maxPointsInLeafNode - 1) / maxPointsInLeafNode);// 可以有多少个叶子节点，一个数据节点512个元素
      final int numSplits = numLeaves - 1;

      checkMaxLeafNodeCount(numLeaves);

      final byte[] splitPackedValues = new byte[numSplits * bytesPerDim]; // 定义了所有叶子点上，在哪个维度切分时的value
      final byte[] splitDimensionValues = new byte[numSplits]; // 定义了在哪个叶子节点上进行了切分，切分时的维度
      final long[] leafBlockFPs = new long[numLeaves]; // 每个叶子在kdd中开始存放的位置
      // 获取每个维度的最大值与最小值
      // compute the min/max for this slice
      computePackedValueBounds(values, 0, Math.toIntExact(pointCount), minPackedValue, maxPackedValue, scratchBytesRef1);
      for (int i = 0; i < Math.toIntExact(pointCount); ++i) {
        docsSeen.set(values.getDocID(i));// 统计哪些文档中Point
      }
// 开始构造BKD树
      final long dataStartFP = dataOut.getFilePointer(); // kdd文件起始位置
      final int[] parentSplits = new int[numIndexDims]; // 将统计每个维度拆分的次数，若存在某个维度切分次数不足最大的一半，那么本次将选择这个维度切分
      build(0, numLeaves, values, 0, Math.toIntExact(pointCount), dataOut,
              minPackedValue.clone(), maxPackedValue.clone(), parentSplits,
              splitPackedValues, splitDimensionValues, leafBlockFPs,
              new int[maxPointsInLeafNode]);
      assert Arrays.equals(parentSplits, new int[numIndexDims]);

      scratchBytesRef1.length = bytesPerDim;
      scratchBytesRef1.bytes = splitPackedValues;

      BKDTreeLeafNodes leafNodes  = new BKDTreeLeafNodes() {
        @Override
        public long getLeafLP(int index) {
          return leafBlockFPs[index]; // 叶子在kdd文件存储的位置起点
        }

        @Override
        public BytesRef getSplitValue(int index) {
          scratchBytesRef1.offset = index * bytesPerDim;
          return scratchBytesRef1;
        }

        @Override
        public int getSplitDimension(int index) {
          return splitDimensionValues[index] & 0xff;
        }

        @Override
        public int numLeaves() {
          return leafBlockFPs.length;
        }
      };

      return () -> {
        try { // metaOut:kdm文件   indexOut:kdi文件
          writeIndex(metaOut, indexOut, maxPointsInLeafNode, leafNodes, dataStartFP); // 写dim文件
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      };
  }

  /* In the 1D case, we can simply sort points in ascending order and use the
   * same writing logic as we use at merge time. */
  private Runnable writeField1Dim(IndexOutput metaOut, IndexOutput indexOut, IndexOutput dataOut, String fieldName, MutablePointValues reader) throws IOException {
    MutablePointsReaderUtils.sort(maxDoc, packedIndexBytesLength, reader, 0, Math.toIntExact(reader.size()));

    final OneDimensionBKDWriter oneDimWriter = new OneDimensionBKDWriter(metaOut, indexOut, dataOut);

    reader.intersect(new IntersectVisitor() {

      @Override
      public void visit(int docID, byte[] packedValue) throws IOException {
        oneDimWriter.add(packedValue, docID);
      }

      @Override
      public void visit(int docID) throws IOException {
        throw new IllegalStateException();
      }

      @Override
      public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
        return Relation.CELL_CROSSES_QUERY;
      }
    });

    return oneDimWriter.finish();
  }

  /** More efficient bulk-add for incoming {@link BKDReader}s.  This does a merge sort of the already
   *  sorted values and currently only works when numDims==1.  This returns -1 if all documents containing
   *  dimensional values were deleted. */
  public Runnable merge(IndexOutput metaOut, IndexOutput indexOut, IndexOutput dataOut, List<MergeState.DocMap> docMaps, List<BKDReader> readers) throws IOException {
    assert docMaps == null || readers.size() == docMaps.size();

    BKDMergeQueue queue = new BKDMergeQueue(bytesPerDim, readers.size());

    for(int i=0;i<readers.size();i++) {
      BKDReader bkd = readers.get(i);
      MergeState.DocMap docMap;
      if (docMaps == null) {
        docMap = null;
      } else {
        docMap = docMaps.get(i);
      }
      MergeReader reader = new MergeReader(bkd, docMap);
      if (reader.next()) {
        queue.add(reader);
      }
    }

    OneDimensionBKDWriter oneDimWriter = new OneDimensionBKDWriter(metaOut, indexOut, dataOut);

    while (queue.size() != 0) {
      MergeReader reader = queue.top();
      // System.out.println("iter reader=" + reader);

      oneDimWriter.add(reader.state.scratchDataPackedValue, reader.docID);

      if (reader.next()) {
        queue.updateTop();
      } else {
        // This segment was exhausted
        queue.pop();
      }
    }

    return oneDimWriter.finish();
  }

  // reused when writing leaf blocks
  private final GrowableByteArrayDataOutput scratchOut = new GrowableByteArrayDataOutput(32*1024);

  private class OneDimensionBKDWriter {

    final IndexOutput metaOut, indexOut, dataOut;
    final long dataStartFP;
    final List<Long> leafBlockFPs = new ArrayList<>();
    final List<byte[]> leafBlockStartValues = new ArrayList<>();
    final byte[] leafValues = new byte[maxPointsInLeafNode * packedBytesLength];
    final int[] leafDocs = new int[maxPointsInLeafNode];
    private long valueCount;
    private int leafCount;
    private int leafCardinality;

    OneDimensionBKDWriter(IndexOutput metaOut, IndexOutput indexOut, IndexOutput dataOut) {
      if (numIndexDims != 1) {
        throw new UnsupportedOperationException("numIndexDims must be 1 but got " + numIndexDims);
      }
      if (pointCount != 0) {
        throw new IllegalStateException("cannot mix add and merge");
      }

      // Catch user silliness:
      if (finished == true) {
        throw new IllegalStateException("already finished");
      }

      // Mark that we already finished:
      finished = true;

      this.metaOut = metaOut;
      this.indexOut = indexOut;
      this.dataOut = dataOut;
      this.dataStartFP = dataOut.getFilePointer();

      lastPackedValue = new byte[packedBytesLength];
    }

    // for asserts
    final byte[] lastPackedValue;
    private int lastDocID;

    void add(byte[] packedValue, int docID) throws IOException {
      assert valueInOrder(valueCount + leafCount,
              0, lastPackedValue, packedValue, 0, docID, lastDocID);

      if (leafCount == 0 || FutureArrays.mismatch(leafValues, (leafCount - 1) * bytesPerDim, leafCount * bytesPerDim, packedValue, 0, bytesPerDim) != -1) {
        leafCardinality++;
      }
      System.arraycopy(packedValue, 0, leafValues, leafCount * packedBytesLength, packedBytesLength);
      leafDocs[leafCount] = docID;
      docsSeen.set(docID);
      leafCount++;

      if (valueCount + leafCount > totalPointCount) {
        throw new IllegalStateException("totalPointCount=" + totalPointCount + " was passed when we were created, but we just hit " + (valueCount + leafCount) + " values");
      }

      if (leafCount == maxPointsInLeafNode) {
        // We write a block once we hit exactly the max count ... this is different from
        // when we write N > 1 dimensional points where we write between max/2 and max per leaf block
        writeLeafBlock(leafCardinality);
        leafCardinality = 0;
        leafCount = 0;
      }

      assert (lastDocID = docID) >= 0; // only assign when asserts are enabled
    }

    public Runnable finish() throws IOException {
      if (leafCount > 0) {
        writeLeafBlock(leafCardinality);
        leafCardinality = 0;
        leafCount = 0;
      }

      if (valueCount == 0) {
        return null;
      }

      pointCount = valueCount;

      scratchBytesRef1.length = bytesPerDim;
      scratchBytesRef1.offset = 0;
      assert leafBlockStartValues.size() + 1 == leafBlockFPs.size();
      BKDTreeLeafNodes leafNodes = new BKDTreeLeafNodes() {
        @Override
        public long getLeafLP(int index) {
          return leafBlockFPs.get(index);
        }

        @Override
        public BytesRef getSplitValue(int index) {
          scratchBytesRef1.bytes = leafBlockStartValues.get(index);
          return scratchBytesRef1;
        }

        @Override
        public int getSplitDimension(int index) {
          return 0;
        }

        @Override
        public int numLeaves() {
          return leafBlockFPs.size();
        }
      };
      return () -> {
        try {
          writeIndex(metaOut, indexOut, maxPointsInLeafNode, leafNodes, dataStartFP);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      };
    }

    private void writeLeafBlock(int leafCardinality) throws IOException {
      assert leafCount != 0;
      if (valueCount == 0) {
        System.arraycopy(leafValues, 0, minPackedValue, 0, packedIndexBytesLength);
      }
      System.arraycopy(leafValues, (leafCount - 1) * packedBytesLength, maxPackedValue, 0, packedIndexBytesLength);

      valueCount += leafCount;

      if (leafBlockFPs.size() > 0) {
        // Save the first (minimum) value in each leaf block except the first, to build the split value index in the end:
        leafBlockStartValues.add(ArrayUtil.copyOfSubArray(leafValues, 0, packedBytesLength));
      }
      leafBlockFPs.add(dataOut.getFilePointer());
      checkMaxLeafNodeCount(leafBlockFPs.size());

      // Find per-dim common prefix:
      int offset = (leafCount - 1) * packedBytesLength;
      int prefix = FutureArrays.mismatch(leafValues, 0, bytesPerDim, leafValues, offset, offset + bytesPerDim);
      if (prefix == -1) {
        prefix = bytesPerDim;
      }

      commonPrefixLengths[0] = prefix;

      assert scratchOut.getPosition() == 0;
      writeLeafBlockDocs(scratchOut, leafDocs, 0, leafCount);
      writeCommonPrefixes(scratchOut, commonPrefixLengths, leafValues);

      scratchBytesRef1.length = packedBytesLength;
      scratchBytesRef1.bytes = leafValues;

      final IntFunction<BytesRef> packedValues = new IntFunction<BytesRef>() {
        @Override
        public BytesRef apply(int i) {
          scratchBytesRef1.offset = packedBytesLength * i;
          return scratchBytesRef1;
        }
      };
      assert valuesInOrderAndBounds(leafCount, 0, ArrayUtil.copyOfSubArray(leafValues, 0, packedBytesLength),
              ArrayUtil.copyOfSubArray(leafValues, (leafCount - 1) * packedBytesLength, leafCount * packedBytesLength),
              packedValues, leafDocs, 0);
      writeLeafBlockPackedValues(scratchOut, commonPrefixLengths, leafCount, 0, packedValues, leafCardinality);
      dataOut.writeBytes(scratchOut.getBytes(), 0, scratchOut.getPosition());
      scratchOut.reset();
    }
  }
  // 以numLeaves个叶子节点构成的完全二叉树中，左子树上的叶子节点是多少个
  private int getNumLeftLeafNodes(int numLeaves) {
    assert numLeaves > 1: "getNumLeftLeaveNodes() called with " + numLeaves;
    // return the level that can be filled with this number of leaves
    int lastFullLevel = 31 - Integer.numberOfLeadingZeros(numLeaves);//numberOfLeadingZeros：这些叶子能够充满的最大完全二叉树的高度：level1
    // how many leaf nodes are in the full level
    int leavesFullLevel = 1 << lastFullLevel; // 这个完全二叉树的高度level1的叶子节点
    // half of the leaf nodes from the full level goes to the left
    int numLeftLeafNodes = leavesFullLevel / 2; // 完全二叉树的左叶子节点
    // leaf nodes that do not fit in the full level  神来之笔
    int unbalancedLeafNodes = numLeaves - leavesFullLevel;// (核心)假如左子树未满，level1层的节点，在level1+1层的节点（要么直接是叶子节点，要么包含在unbalancedLeafNodes中）
    // distribute unbalanced leaf nodes// 分情况:  假如左子树未满  代表numLeftLeafNodes>=unbalancedLeafNodes,若右子树未满，则unbalancedLeafNodes>numLeftLeafNodes
    numLeftLeafNodes += Math.min(unbalancedLeafNodes, numLeftLeafNodes);
    // we should always place unbalanced leaf nodes on the left
    assert numLeftLeafNodes >= numLeaves - numLeftLeafNodes && numLeftLeafNodes <= 2L * (numLeaves - numLeftLeafNodes);
    return numLeftLeafNodes;
  }

  // TODO: if we fixed each partition step to just record the file offset at the "split point", we could probably handle variable length
  // encoding and not have our own ByteSequencesReader/Writer

  // useful for debugging:
  /*
  private void printPathSlice(String desc, PathSlice slice, int dim) throws IOException {
    System.out.println("    " + desc + " dim=" + dim + " count=" + slice.count + ":");
    try(PointReader r = slice.writer.getReader(slice.start, slice.count)) {
      int count = 0;
      while (r.next()) {
        byte[] v = r.packedValue();
        System.out.println("      " + count + ": " + new BytesRef(v, dim*bytesPerDim, bytesPerDim));
        count++;
        if (count == slice.count) {
          break;
        }
      }
    }
  }
  */
  //  检查是否达到数组的最大长度，
  private void checkMaxLeafNodeCount(int numLeaves) {
    if (bytesPerDim * (long) numLeaves > ArrayUtil.MAX_ARRAY_LENGTH) {
      throw new IllegalStateException("too many nodes; increase maxPointsInLeafNode (currently " + maxPointsInLeafNode + ") and reindex");
    }
  }

  /** Writes the BKD tree to the provided {@link IndexOutput}s and returns a {@link Runnable} that
   *  writes the index of the tree if at least one point has been added, or {@code null} otherwise. */
  public Runnable finish(IndexOutput metaOut, IndexOutput indexOut, IndexOutput dataOut) throws IOException {
    // System.out.println("\nBKDTreeWriter.finish pointCount=" + pointCount + " out=" + out + " heapWriter=" + heapPointWriter);

    // TODO: specialize the 1D case?  it's much faster at indexing time (no partitioning on recurse...)

    // Catch user silliness:
    if (finished == true) {
      throw new IllegalStateException("already finished");
    }

    if (pointCount == 0) {
      return null;
    }

    //mark as finished
    finished = true;

    pointWriter.close();
    BKDRadixSelector.PathSlice points = new BKDRadixSelector.PathSlice(pointWriter, 0, pointCount);
    //clean up pointers
    tempInput = null;
    pointWriter = null;


    final int numLeaves = Math.toIntExact((pointCount + maxPointsInLeafNode - 1) / maxPointsInLeafNode);// 统计叶子节点的数量
    final int numSplits = numLeaves - 1;

    checkMaxLeafNodeCount(numLeaves);

    // NOTE: we could save the 1+ here, to use a bit less heap at search time, but then we'd need a somewhat costly check at each
    // step of the recursion to recompute the split dim:
    // 是一个完全二叉树，每一层如何切分的，都放入了splitPackedValues中
    // Indexed by nodeID, but first (root) nodeID is 1.  We do 1+ because the lead byte at each recursion says which dim we split on.
    byte[] splitPackedValues = new byte[Math.toIntExact(numSplits*bytesPerDim)];
    byte[] splitDimensionValues = new byte[numSplits];

    // +1 because leaf count is power of 2 (e.g. 8), and innerNodeCount is power of 2 minus 1 (e.g. 7)
    long[] leafBlockFPs = new long[numLeaves];

    // Make sure the math above "worked":
    assert pointCount / numLeaves <= maxPointsInLeafNode: "pointCount=" + pointCount + " numLeaves=" + numLeaves + " maxPointsInLeafNode=" + maxPointsInLeafNode;

    //We re-use the selector so we do not need to create an object every time.
    BKDRadixSelector radixSelector = new BKDRadixSelector(numDataDims, numIndexDims, bytesPerDim, maxPointsSortInHeap, tempDir, tempFileNamePrefix);

    final long dataStartFP = dataOut.getFilePointer();
    boolean success = false;
    try {

      final int[] parentSplits = new int[numIndexDims];
      build(0, numLeaves, points,
              dataOut, radixSelector,
              minPackedValue.clone(), maxPackedValue.clone(),
              parentSplits,
              splitPackedValues,
              splitDimensionValues,
              leafBlockFPs,
              new int[maxPointsInLeafNode]);
      assert Arrays.equals(parentSplits, new int[numIndexDims]);

      // If no exception, we should have cleaned everything up:
      assert tempDir.getCreatedFiles().isEmpty();
      //long t2 = System.nanoTime();
      //System.out.println("write time: " + ((t2-t1)/1000000.0) + " msec");

      success = true;
    } finally {
      if (success == false) {
        IOUtils.deleteFilesIgnoringExceptions(tempDir, tempDir.getCreatedFiles());
      }
    }

    scratchBytesRef1.bytes = splitPackedValues;
    scratchBytesRef1.length = bytesPerDim;
    BKDTreeLeafNodes leafNodes  = new BKDTreeLeafNodes() {
      @Override
      public long getLeafLP(int index) {
        return leafBlockFPs[index];
      }

      @Override
      public BytesRef getSplitValue(int index) {
        scratchBytesRef1.offset = index * bytesPerDim;
        return scratchBytesRef1;
      }

      @Override
      public int getSplitDimension(int index) {
        return splitDimensionValues[index] & 0xff;
      }

      @Override
      public int numLeaves() {
        return leafBlockFPs.length;// 叶子节点个数
      }
    };

    return () -> {
      // Write index:
      try {
        writeIndex(metaOut, indexOut, maxPointsInLeafNode, leafNodes, dataStartFP);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    };
  }

  /** Packs the two arrays, representing a semi-balanced binary tree, into a compact byte[] structure. */
  private byte[] packIndex(BKDTreeLeafNodes leafNodes) throws IOException {
    /** Reused while packing the index */
    RAMOutputStream writeBuffer = new RAMOutputStream();// 一直复用的

    // This is the "file" we append the byte[] to:
    List<byte[]> blocks = new ArrayList<>();
    byte[] lastSplitValues = new byte[bytesPerDim * numIndexDims];
    //System.out.println("\npack index");
    int totalSize = recursePackIndex(writeBuffer, leafNodes, 0l, blocks, lastSplitValues, new boolean[numIndexDims], false,
            0, leafNodes.numLeaves());

    // Compact the byte[] blocks into single byte index:
    byte[] index = new byte[totalSize];
    int upto = 0;
    for(byte[] block : blocks) {
      System.arraycopy(block, 0, index, upto, block.length);
      upto += block.length;
    }
    assert upto == totalSize;

    return index;
  }

  /** Appends the current contents of writeBuffer as another block on the growing in-memory file */
  private int appendBlock(RAMOutputStream writeBuffer, List<byte[]> blocks) throws IOException {
    int pos = Math.toIntExact(writeBuffer.getFilePointer());
    byte[] bytes = new byte[pos];
    writeBuffer.writeTo(bytes, 0);
    writeBuffer.reset(); // 只是把指针给修改了。
    blocks.add(bytes);
    return pos;
  }

  /** 往kdi中写的数的结构
   * lastSplitValues is per-dimension split value previously seen; we use this to prefix-code the split byte[] on each inner node
   */  // 递归按照中旬遍历的规则，  splitPackedValues保存了每层是如何切分的，lastSplitValues保存了该维度上次切分的value。negativeDeltas保存了该维度上次切分时是左左子树还是右子树
  private int recursePackIndex(RAMOutputStream writeBuffer, BKDTreeLeafNodes leafNodes, long minBlockFP, List<byte[]> blocks,
                               byte[] lastSplitValues, boolean[] negativeDeltas, boolean isLeft, int leavesOffset, int numLeaves) throws IOException {
    if (numLeaves == 1) { // 到了叶子节点。
      if (isLeft) {
        assert leafNodes.getLeafLP(leavesOffset) - minBlockFP == 0;
        return 0;
      } else {
        long delta = leafNodes.getLeafLP(leavesOffset) - minBlockFP; // 父节点最左侧的的偏移量
        assert leafNodes.numLeaves() == numLeaves || delta > 0 : "expected delta > 0; got numLeaves =" + numLeaves + " and delta=" + delta;
        writeBuffer.writeVLong(delta);
        return appendBlock(writeBuffer, blocks);
      }
    } else { // 不是叶子节点
      long leftBlockFP; // 最左边的
      if (isLeft) {// 父节点的左子树的最左边叶子起始
        // The left tree's left most leaf block FP is always the minimal FP:
        assert leafNodes.getLeafLP(leavesOffset) == minBlockFP;
        leftBlockFP = minBlockFP;
      } else { // 右子树节点
        leftBlockFP = leafNodes.getLeafLP(leavesOffset); // 该子树左边开始第一个叶子也是父亲左边第一个叶子节点
        long delta = leftBlockFP - minBlockFP;// 右子树第一个叶子（左边）的起始位置-父子树第一个叶子（左边）的起始位置。
        assert leafNodes.numLeaves() == numLeaves || delta > 0 : "expected delta > 0; got numLeaves =" + numLeaves + " and delta=" + delta;
        writeBuffer.writeVLong(delta); // writeBuffer写到kdi中。
      }

      int numLeftLeafNodes = getNumLeftLeafNodes(numLeaves);
      final int rightOffset = leavesOffset + numLeftLeafNodes;// 右子树第一个叶子的下标
      final int splitOffset = rightOffset - 1; // 切分的偏移量
      // 和构建时一样
      int splitDim = leafNodes.getSplitDimension(splitOffset);// 是以哪个维度切分的，然后address指向下一个位置（value值）
      BytesRef splitValue = leafNodes.getSplitValue(splitOffset);// 这个维度切分时这个维度的值
      int address = splitValue.offset;

      //System.out.println("recursePack inner nodeID=" + nodeID + " splitDim=" + splitDim + " splitValue=" + new BytesRef(splitPackedValues, address, bytesPerDim));
       // 查找切分的那个值和之前切分那个维度相同的前缀
      // find common prefix with last split value in this dim:
      int prefix = FutureArrays.mismatch(splitValue.bytes, address, address + bytesPerDim, lastSplitValues,
              splitDim * bytesPerDim, splitDim * bytesPerDim + bytesPerDim);
      if (prefix == -1) {
        prefix = bytesPerDim;
      }

      //System.out.println("writeNodeData nodeID=" + nodeID + " splitDim=" + splitDim + " numDims=" + numDims + " bytesPerDim=" + bytesPerDim + " prefix=" + prefix);

      int firstDiffByteDelta;
      if (prefix < bytesPerDim) { // 两次切分的值是不同的
        //System.out.println("  delta byte cur=" + Integer.toHexString(splitPackedValues[address+prefix]&0xFF) + " prev=" + Integer.toHexString(lastSplitValues[splitDim * bytesPerDim + prefix]&0xFF) + " negated?=" + negativeDeltas[splitDim]);
        firstDiffByteDelta = (splitValue.bytes[address+prefix]&0xFF) - (lastSplitValues[splitDim * bytesPerDim + prefix]&0xFF);
        if (negativeDeltas[splitDim]) {// 保留该维度上次切分时，当前分支左子树还是右子树
          firstDiffByteDelta = -firstDiffByteDelta; // 因为该该维度已经排好序了，若当前是左子树，那么肯定currentValue<lastValue
        }
        //System.out.println("  delta=" + firstDiffByteDelta);
        assert firstDiffByteDelta > 0; // 保证该值是正数
      } else {
        firstDiffByteDelta = 0;
      }
      // 将prefix、splitDim和firstDiffByteDelta打包编码到同一个vint中，也很容易解码出来。见BKDReader.readNodeData()中287行编码
      // pack the prefix, splitDim and delta first diff byte into a single vInt:
      int code = (firstDiffByteDelta * (1+bytesPerDim) + prefix) * numIndexDims + splitDim;
      //
      //System.out.println("  code=" + code);
      //System.out.println("  splitValue=" + new BytesRef(splitPackedValues, address, bytesPerDim));

      writeBuffer.writeVInt(code);

      // write the split value, prefix coded vs. our parent's split value:
      int suffix = bytesPerDim - prefix;
      byte[] savSplitValue = new byte[suffix];// 临时变量，主要是为了记录上一次的切分值
      if (suffix > 1) {// 不完全一样
        writeBuffer.writeBytes(splitValue.bytes, address+prefix+1, suffix-1);// 把这个split分词的那个词后半段存储起来
      }

      byte[] cmp = lastSplitValues.clone(); // 不再是同一个对象
      // 把lastSplitValues中的不相同的后缀全部copy到savSplitValue中
      System.arraycopy(lastSplitValues, splitDim * bytesPerDim + prefix, savSplitValue, 0, suffix); // 临时保存，后面会还原
      // 将splitPackedValues中该不相同的后缀copy，放到lastSplitValues对应的位置，以便遍历使用
      // copy our split value into lastSplitValues for our children to prefix-code against
      System.arraycopy(splitValue.bytes, address+prefix, lastSplitValues, splitDim * bytesPerDim + prefix, suffix);
      // 将writeBuffer存储的值，以[]byte方式放入blocks中
      int numBytes = appendBlock(writeBuffer, blocks); // 放的code，词的后半缀

      // placeholder for left-tree numBytes; we need this so that at search time if we only need to recurse into the right sub-tree we can
      // quickly seek to its starting point
      int idxSav = blocks.size();//已经写了几个
      blocks.add(null); // 占位符，后面会赋值

      boolean savNegativeDelta = negativeDeltas[splitDim];
      negativeDeltas[splitDim] = true;


      int leftNumBytes = recursePackIndex(writeBuffer, leafNodes, leftBlockFP, blocks, lastSplitValues, negativeDeltas, true,
              leavesOffset, numLeftLeafNodes);

      if (numLeftLeafNodes != 1) { // 仅仅是1位，那么bytes2.length=1（左叶子节点不存储leftNumBytes，直接返回0了）
        writeBuffer.writeVInt(leftNumBytes);
      } else { // 最左边的那个叶子节点，那么那么bytes2.length=0
        assert leftNumBytes == 0: "leftNumBytes=" + leftNumBytes;
      }

      int numBytes2 = Math.toIntExact(writeBuffer.getFilePointer()); // 存储的leftNumBytes的长度
      byte[] bytes2 = new byte[numBytes2]; //长度只能为0，或者1
      writeBuffer.writeTo(bytes2, 0);
      writeBuffer.reset();
      // replace our placeholder:
      blocks.set(idxSav, bytes2);

      negativeDeltas[splitDim] = false;// 置位
      int rightNumBytes = recursePackIndex(writeBuffer,  leafNodes, leftBlockFP, blocks, lastSplitValues, negativeDeltas, false,
              rightOffset, numLeaves - numLeftLeafNodes);

      negativeDeltas[splitDim] = savNegativeDelta; // 这里会复位

      // restore lastSplitValues to what caller originally passed us:
      System.arraycopy(savSplitValue, 0, lastSplitValues, splitDim * bytesPerDim + prefix, suffix); // 再放回去

      assert Arrays.equals(lastSplitValues, cmp);
      // 当前父节点信息+左子树长度+左子树byte+右子树byte
      return numBytes + bytes2.length + leftNumBytes + rightNumBytes;// 当前非叶子节点存储使用的空间，分中+左右占用
    }// bytes2.length长度占1位，存储的是值是leftNumBytes的长度
  }

  private void writeIndex(IndexOutput metaOut, IndexOutput indexOut, int countPerLeaf, BKDTreeLeafNodes leafNodes, long dataStartFP) throws IOException {
    byte[] packedIndex = packIndex(leafNodes);
    writeIndex(metaOut, indexOut, countPerLeaf, leafNodes.numLeaves(), packedIndex, dataStartFP);
  }
  // splitPackedValues保存了每层是如何切分的  // 与BKDReader构造函数中读取的编码顺序一致
  private void writeIndex(IndexOutput metaOut, IndexOutput indexOut, int countPerLeaf, int numLeaves, byte[] packedIndex, long dataStartFP) throws IOException {
    CodecUtil.writeHeader(metaOut, CODEC_NAME, VERSION_CURRENT); // kdm文件写入
    metaOut.writeVInt(numDataDims);
    metaOut.writeVInt(numIndexDims);
    metaOut.writeVInt(countPerLeaf); // 每个页节点的元素个数
    metaOut.writeVInt(bytesPerDim);

    assert numLeaves > 0;
    metaOut.writeVInt(numLeaves);
    metaOut.writeBytes(minPackedValue, 0, packedIndexBytesLength);
    metaOut.writeBytes(maxPackedValue, 0, packedIndexBytesLength);

    metaOut.writeVLong(pointCount);
    metaOut.writeVInt(docsSeen.cardinality());
    metaOut.writeVInt(packedIndex.length);// BKD树转存结构的长度
    metaOut.writeLong(dataStartFP); // kdd存放数据的起始位置
    // If metaOut and indexOut are the same file, we account for the fact that
    // writing a long makes the index start 8 bytes later.
    metaOut.writeLong(indexOut.getFilePointer() + (metaOut == indexOut ? Long.BYTES : 0));

    indexOut.writeBytes(packedIndex, 0, packedIndex.length);
  }

  private void writeLeafBlockDocs(DataOutput out, int[] docIDs, int start, int count) throws IOException {
    assert count > 0: "maxPointsInLeafNode=" + maxPointsInLeafNode;
    out.writeVInt(count);
    DocIdsWriter.writeDocIds(docIDs, start, count, out);
  }
  // 叶子不同的个数
  private void writeLeafBlockPackedValues(DataOutput out, int[] commonPrefixLengths, int count, int sortedDim, IntFunction<BytesRef> packedValues, int leafCardinality) throws IOException {
    int prefixLenSum = Arrays.stream(commonPrefixLengths).sum(); //前缀相加，全部相等的话
    if (prefixLenSum == packedBytesLength) {
      // all values in this block are equal
      out.writeByte((byte) -1); // 完全相同
    } else {
      assert commonPrefixLengths[sortedDim] < bytesPerDim;
      // estimate if storing the values with cardinality is cheaper than storing all values.
      int compressedByteOffset = sortedDim * bytesPerDim + commonPrefixLengths[sortedDim]; // 排序的这一维度，从这个点开始，就开始不一样的了。
      int highCardinalityCost; // 衡量相同的
      int lowCardinalityCost;
      if (count == leafCardinality) { // 所有值全部不相同
        // all values in this block are different
        highCardinalityCost = 0;   // 希望的得分，很高的相同性
        lowCardinalityCost = 1; // 不希望得分，很低的相同性
      } else { // 部分值是相同的
        // compute cost of runLen compression
        int numRunLens = 0; // compressedByteOffset共有多少个字符
        for (int i = 0; i < count; ) { //
          // do run-length compression on the byte at compressedByteOffset// 为啥设置255一波，就不止255个相同的，下一波可以继续被处理
          int runLen = runLen(packedValues, i, Math.min(i + 0xff, count), compressedByteOffset);
          assert runLen <= 0xff;
          numRunLens++;
          i += runLen; // 再继续找下轮
        }
        // Add cost of runLen compression
        highCardinalityCost = count * (packedBytesLength - prefixLenSum - 1) + 2 * numRunLens;  // 统计的是存储时需要多少byte,叶子个数*需要存储的位数*
        // +1 is the byte needed for storing the cardinality
        lowCardinalityCost = leafCardinality * (packedBytesLength - prefixLenSum + 1);
      }
      if (lowCardinalityCost <= highCardinalityCost) {
        out.writeByte((byte) -2); // 大部分是一样的
        writeLowCardinalityLeafBlockPackedValues(out, commonPrefixLengths, count, packedValues);
      } else { // 大部分是不一样的
        out.writeByte((byte) sortedDim); // 以哪个阶进行的排序
        writeHighCardinalityLeafBlockPackedValues(out, commonPrefixLengths, count, sortedDim, packedValues, compressedByteOffset);
      } // 两个函数，都是先保存的最大最小值
    }
  }
   // 大部分数据是一样的，则联系相同的元素一组，存放一次
  private void writeLowCardinalityLeafBlockPackedValues(DataOutput out, int[] commonPrefixLengths, int count, IntFunction<BytesRef> packedValues) throws IOException {
    if (numIndexDims != 1) { // 存储最大值，最小值
      writeActualBounds(out, commonPrefixLengths, count, packedValues);
    }
    BytesRef value = packedValues.apply(0);
    System.arraycopy(value.bytes, value.offset, scratch1, 0, packedBytesLength);
    int cardinality = 1; // 连续相同的个数
    for (int i = 1; i < count; i++) { //
      value = packedValues.apply(i);
      for(int dim = 0; dim < numDataDims; dim++) {
        final int start = dim * bytesPerDim + commonPrefixLengths[dim];
        final int end = dim * bytesPerDim + bytesPerDim;
        if (FutureArrays.mismatch(value.bytes, value.offset + start, value.offset + end, scratch1, start, end) != -1) { // 不一样的话
          out.writeVInt(cardinality); // 如果不一样的话，则统计相同的个数
          for (int j = 0; j < numDataDims; j++) { // 然后将此元素存入文件中
            out.writeBytes(scratch1, j * bytesPerDim + commonPrefixLengths[j], bytesPerDim - commonPrefixLengths[j]);
          }
          System.arraycopy(value.bytes, value.offset, scratch1, 0, packedBytesLength);
          cardinality = 1;
          break;
        } else if (dim == numDataDims - 1){ // 若这个维度一样，并且是最后一个维度，则统计相同的格式
          cardinality++;
        }
      }
    }
    out.writeVInt(cardinality); // 统计最后一次的元素
    for (int i = 0; i < numDataDims; i++) {
      out.writeBytes(scratch1, i * bytesPerDim + commonPrefixLengths[i], bytesPerDim - commonPrefixLengths[i]);
    }
  }
  // 大部分数据都是不一样的，那么就依次排开存放。针对compressedByteOffset位检测，连续相同的就存储这位，不相同的位再存储。再找下一个连续相同的
  private void writeHighCardinalityLeafBlockPackedValues(DataOutput out, int[] commonPrefixLengths, int count, int sortedDim, IntFunction<BytesRef> packedValues, int compressedByteOffset) throws IOException {
    if (numIndexDims != 1) { // 进来，把每个维度的最大最小值都存储起来
      writeActualBounds(out, commonPrefixLengths, count, packedValues);
    } // +1 使用了小技巧，sortedDim少存储一位
    commonPrefixLengths[sortedDim]++; // 这里加+1，是为了sortedDim阶少存储一位。在接下来的for中，找出相同位的个数，然后先保存这一位，在存放后面的。（这个维度还会继续检查下前后相同长度）
    for (int i = 0; i < count; ) { // 遍历所有point数据
      // do run-length compression on the byte at compressedByteOffset
      int runLen = runLen(packedValues, i, Math.min(i + 0xff, count), compressedByteOffset); // 一次只让比较255个数据，前后point不相同位相同相邻的长度
      assert runLen <= 0xff;
      BytesRef first = packedValues.apply(i);
      byte prefixByte = first.bytes[first.offset + compressedByteOffset];// 压缩的这一维度
      out.writeByte(prefixByte); // 先把相同的前缀写起来
      out.writeByte((byte) runLen); // 在写多少字符一样
      writeLeafBlockPackedValuesRange(out, commonPrefixLengths, i, i + runLen, packedValues); // 就是按照顺序写一批数据进去
      i += runLen;
      assert i <= count;
    }
  }

  private void writeActualBounds(DataOutput out, int[] commonPrefixLengths, int count, IntFunction<BytesRef> packedValues) throws IOException {
    for (int dim = 0; dim < numIndexDims; ++dim) { // 遍历每个dim，找到每个维度最大值，最小值，存储起来
      int commonPrefixLength = commonPrefixLengths[dim];
      int suffixLength = bytesPerDim - commonPrefixLength;
      if (suffixLength > 0) {
        BytesRef[] minMax = computeMinMax(count, packedValues, dim * bytesPerDim + commonPrefixLength, suffixLength); // 统计这批数据的最大最小值
        BytesRef min = minMax[0];
        BytesRef max = minMax[1];
        out.writeBytes(min.bytes, min.offset, min.length);
        out.writeBytes(max.bytes, max.offset, max.length);
      }
    }
  }

  /** Return an array that contains the min and max values for the [offset, offset+length] interval
   *  of the given {@link BytesRef}s. */
  private static BytesRef[] computeMinMax(int count, IntFunction<BytesRef> packedValues, int offset, int length) {
    assert length > 0;
    BytesRefBuilder min = new BytesRefBuilder();
    BytesRefBuilder max = new BytesRefBuilder();
    BytesRef first = packedValues.apply(0); // 先读取
    min.copyBytes(first.bytes, first.offset + offset, length);
    max.copyBytes(first.bytes, first.offset + offset, length);
    for (int i = 1; i < count; ++i) {
      BytesRef candidate = packedValues.apply(i);
      if (FutureArrays.compareUnsigned(min.bytes(), 0, length, candidate.bytes, candidate.offset + offset, candidate.offset + offset + length) > 0) {
        min.copyBytes(candidate.bytes, candidate.offset + offset, length);
      } else if (FutureArrays.compareUnsigned(max.bytes(), 0, length, candidate.bytes, candidate.offset + offset, candidate.offset + offset + length) < 0) {
        max.copyBytes(candidate.bytes, candidate.offset + offset, length);
      }
    }
    return new BytesRef[]{min.get(), max.get()};
  }
  // 按照顺序，将剩余250个元素，每一阶阶写入
  private void writeLeafBlockPackedValuesRange(DataOutput out, int[] commonPrefixLengths, int start, int end, IntFunction<BytesRef> packedValues) throws IOException {
    for (int i = start; i < end; ++i) {
      BytesRef ref = packedValues.apply(i);
      assert ref.length == packedBytesLength;

      for(int dim=0;dim<numDataDims;dim++) {
        int prefix = commonPrefixLengths[dim];
        out.writeBytes(ref.bytes, ref.offset + dim*bytesPerDim + prefix, bytesPerDim-prefix);
      }
    }
  }
  //返回与第一个数在byteOffset位相同字符的节点个数, byteOffset不为公共字符的第一位
  private static int runLen(IntFunction<BytesRef> packedValues, int start, int end, int byteOffset) {
    BytesRef first = packedValues.apply(start);
    byte b = first.bytes[first.offset + byteOffset]; // 比较的是排序这一维度，的不同值这一列，有多少字符是不同的
    for (int i = start + 1; i < end; ++i) {
      BytesRef ref = packedValues.apply(i); // 读取这个词
      byte b2 = ref.bytes[ref.offset + byteOffset];
      assert Byte.toUnsignedInt(b2) >= Byte.toUnsignedInt(b);
      if (b != b2) { // 不一样就返回
        return i - start;
      }
    }
    return end - start; // 全部一样，那么这阶的这一位，全部一样
  }

  private void writeCommonPrefixes(DataOutput out, int[] commonPrefixes, byte[] packedValue) throws IOException {
    for(int dim=0;dim<numDataDims;dim++) { // 把每个维度相同前缀长度&前缀存储起来
      out.writeVInt(commonPrefixes[dim]);
      //System.out.println(commonPrefixes[dim] + " of " + bytesPerDim);
      out.writeBytes(packedValue, dim*bytesPerDim, commonPrefixes[dim]);
    }
  }

  @Override
  public void close() throws IOException {
    finished = true;
    if (tempInput != null) {
      // NOTE: this should only happen on exception, e.g. caller calls close w/o calling finish:
      try {
        tempInput.close();
      } finally {
        tempDir.deleteFile(tempInput.getName());
        tempInput = null;
      }
    }
  }

  /** Called on exception, to check whether the checksum is also corrupt in this source, and add that
   *  information (checksum matched or didn't) as a suppressed exception. */
  private Error verifyChecksum(Throwable priorException, PointWriter writer) throws IOException {
    assert priorException != null;

    // TODO: we could improve this, to always validate checksum as we recurse, if we shared left and
    // right reader after recursing to children, and possibly within recursed children,
    // since all together they make a single pass through the file.  But this is a sizable re-org,
    // and would mean leaving readers (IndexInputs) open for longer:
    if (writer instanceof OfflinePointWriter) {
      // We are reading from a temp file; go verify the checksum:
      String tempFileName = ((OfflinePointWriter) writer).name;
      if (tempDir.getCreatedFiles().contains(tempFileName)) {
        try (ChecksumIndexInput in = tempDir.openChecksumInput(tempFileName, IOContext.READONCE)) {
          CodecUtil.checkFooter(in, priorException);
        }
      }
    }

    // We are reading from heap; nothing to add:
    throw IOUtils.rethrowAlways(priorException);
  }

  /** Called only in assert */
  private boolean valueInBounds(BytesRef packedValue, byte[] minPackedValue, byte[] maxPackedValue) {
    for(int dim=0;dim<numIndexDims;dim++) {
      int offset = bytesPerDim*dim;
      if (FutureArrays.compareUnsigned(packedValue.bytes, packedValue.offset + offset, packedValue.offset + offset + bytesPerDim, minPackedValue, offset, offset + bytesPerDim) < 0) {
        return false;
      }
      if (FutureArrays.compareUnsigned(packedValue.bytes, packedValue.offset + offset, packedValue.offset + offset + bytesPerDim, maxPackedValue, offset, offset + bytesPerDim) > 0) {
        return false;
      }
    }

    return true;
  }

  /**
   * Pick the next dimension to split.
   * @param minPackedValue the min values for all dimensions
   * @param maxPackedValue the max values for all dimensions
   * @param parentSplits how many times each dim has been split on the parent levels
   * @return the dimension to split
   */
  protected int split(byte[] minPackedValue, byte[] maxPackedValue, int[] parentSplits) {
    // First look at whether there is a dimension that has split less than 2x less than
    // the dim that has most splits, and return it if there is such a dimension and it
    // does not only have equals values. This helps ensure all dimensions are indexed.
    int maxNumSplits = 0;
    for (int numSplits : parentSplits) { // 找父节点最大的那个值
      maxNumSplits = Math.max(maxNumSplits, numSplits);
    }// 如果在一个维度上切分次数太少（切分次数不到最多的一半，那么就直接选该维度来切分）
    for (int dim = 0; dim < numIndexDims; ++dim) { //依次遍历每一个维度
      final int offset = dim * bytesPerDim;
      if (parentSplits[dim] < maxNumSplits / 2 &&// 当前维度切分次数不足某个维度的一半，为了保证每个维度都被切分到, 那么选择这个维度切分，
              FutureArrays.compareUnsigned(minPackedValue, offset, offset + bytesPerDim, maxPackedValue, offset, offset + bytesPerDim) != 0) {
        return dim;
      }
    }

    // Find which dim has the largest span so we can split on it:
    int splitDim = -1;
    for(int dim=0;dim<numIndexDims;dim++) { // 遍历每一维度，获取每个维度的最大值和最小值只差
      NumericUtils.subtract(bytesPerDim, dim, maxPackedValue, minPackedValue, scratchDiff); // 这一位减法之后的结果，放在scratchDiff
      if (splitDim == -1 || FutureArrays.compareUnsigned(scratchDiff, 0, bytesPerDim, scratch1, 0, bytesPerDim) > 0) {
        System.arraycopy(scratchDiff, 0, scratch1, 0, bytesPerDim);
        splitDim = dim; // 找的是每个维度最大值和最小值差值的最大值的那个维度
      }
    }

    //System.out.println("SPLIT: " + splitDim);
    return splitDim;
  }

  /** Pull a partition back into heap once the point count is low enough while recursing. */
  private HeapPointWriter switchToHeap(PointWriter source) throws IOException {
    int count = Math.toIntExact(source.count());
    try (PointReader reader = source.getReader(0, source.count());
         HeapPointWriter writer = new HeapPointWriter(count, packedBytesLength)) {
      for(int i=0;i<count;i++) {
        boolean hasNext = reader.next();
        assert hasNext;
        writer.append(reader.pointValue());
      }
      source.destroy();
      return writer;
    } catch (Throwable t) {
      throw verifyChecksum(t, source);
    }
  }
  // 构造BKD树, leavesOffset: 当前构建时是从第几个叶子节点开始构建的（从0开始）
  /* Recursively reorders the provided reader and writes the bkd-tree on the fly; this method is used
   * when we are writing a new segment directly from IndexWriter's indexing buffer (MutablePointsReader). */
  private void build(int leavesOffset, int numLeaves,
                     MutablePointValues reader, int from, int to, // from和to都是point下标，不是叶子下标，不包含to,to是个数下标，
                     IndexOutput out, // kdd文件
                     byte[] minPackedValue, byte[] maxPackedValue,
                     int[] parentSplits,
                     byte[] splitPackedValues,// 定义了所有叶子点上，在哪个维度切分时的value
                     byte[] splitDimensionValues,// 定义了在哪个叶子节点上进行了切分，切分时的维度
                     long[] leafBlockFPs, // 当前节点下，所有叶子节点存放在kdd文件起始位置
                     int[] spareDocIds) throws IOException {

    if (numLeaves == 1) { // 当前是叶子节点了
      // leaf node
      final int count = to - from; // 叶子节点的起始位置
      assert count <= maxPointsInLeafNode;
      //计算这批数据里面每个域相同的前缀
      // Compute common prefixes
      Arrays.fill(commonPrefixLengths, bytesPerDim);
      reader.getValue(from, scratchBytesRef1); // 读取第from个Point整个的所有点
      for (int i = from + 1; i < to; ++i) { //从下个开始，找相同的前缀
        reader.getValue(i, scratchBytesRef2);
        for (int dim=0;dim<numDataDims;dim++) { // 比较每个维度从from->to中相同的前缀
          final int offset = dim * bytesPerDim;
          int dimensionPrefixLength = commonPrefixLengths[dim];
          commonPrefixLengths[dim] = FutureArrays.mismatch(scratchBytesRef1.bytes, scratchBytesRef1.offset + offset, // 不同数据的起点
                  scratchBytesRef1.offset + offset + dimensionPrefixLength,
                  scratchBytesRef2.bytes, scratchBytesRef2.offset + offset,
                  scratchBytesRef2.offset + offset + dimensionPrefixLength);
          if (commonPrefixLengths[dim] == -1) {// 两个字符串一模一样
            commonPrefixLengths[dim] = dimensionPrefixLength; //
          }
        }
      }

      // Find the dimension that has the least number of unique bytes at commonPrefixLengths[dim]
      FixedBitSet[] usedBytes = new FixedBitSet[numDataDims];
      for (int dim = 0; dim < numDataDims; ++dim) {
        if (commonPrefixLengths[dim] < bytesPerDim) { // 当前维度所有字符不一样长
          usedBytes[dim] = new FixedBitSet(256); //因为最多有128个字符，这里用256位就满足了.只有不一样的才会被赋值
        }
      } // 统计不一样的那个维度，去重之后可以分为多少个字符
      for (int i = from + 1; i < to; ++i) {
        for (int dim=0;dim<numDataDims;dim++) {
          if (usedBytes[dim] != null) { // 该维度值不一样
            byte b = reader.getByteAt(i, dim * bytesPerDim + commonPrefixLengths[dim]);//
            usedBytes[dim].set(Byte.toUnsignedInt(b));
          }
        }
      }
      int sortedDim = 0; // 统计两个维度中distinct字母最少的那个维度
      int sortedDimCardinality = Integer.MAX_VALUE; // distinct后的值个数
      for (int dim = 0; dim < numDataDims; ++dim) {
        if (usedBytes[dim] != null) {
          final int cardinality = usedBytes[dim].cardinality(); //
          if (cardinality < sortedDimCardinality) {
            sortedDim = dim;
            sortedDimCardinality = cardinality;
          }
        }
      } // 将数据以最集中的那个维度排序（但不是完全一致）
      // 每个维度都有一系列数据，这系列数据在某位开始不同，统计开始不同这位有多少个distinct个数据，找到这几个维度中，distinct最小的那个维度，进行排序
      // sort by sortedDim
      MutablePointsReaderUtils.sortByDim(numDataDims, numIndexDims, sortedDim, bytesPerDim, commonPrefixLengths,
              reader, from, to, scratchBytesRef1, scratchBytesRef2);

      BytesRef comparator = scratchBytesRef1;
      BytesRef collector = scratchBytesRef2;
      reader.getValue(from, comparator); // 读取排序后的第一个元素，被比较的数
      int leafCardinality = 1; // 获取的是元素（全维度）与后面一个元素不相同的个数
      for (int i = from + 1; i < to; ++i) {
        reader.getValue(i, collector); // 读取下一个元素, collector是最新的数
        for (int dim =0; dim < numDataDims; dim++) { // 几个维度，只有前面一个和后面有一个不相同，就leafCardinality+1
          final int start = dim * bytesPerDim + commonPrefixLengths[dim]; // 从不同之处开始比较，只要有一个维度不一样，救认为不一样
          final int end = dim * bytesPerDim + bytesPerDim;
          if (FutureArrays.mismatch(collector.bytes, collector.offset + start, collector.offset + end,
                  comparator.bytes, comparator.offset + start, comparator.offset + end) != -1) {// 如果不是完全一样
            leafCardinality++; // 每个value都不同
            BytesRef scratch = collector;// 在交换collector和comparator的值，是想前后比较是否一致
            collector = comparator;
            comparator = scratch;
            break; // 直接退出了, 所以交换没啥用
          }
        }
      }
      // Save the block file pointer:
      leafBlockFPs[leavesOffset] = out.getFilePointer(); // kdd

      assert scratchOut.getPosition() == 0;

      // Write doc IDs
      int[] docIDs = spareDocIds;
      for (int i = from; i < to; ++i) {
        docIDs[i - from] = reader.getDocID(i); // 获取from->to之间的文档Id
      }
      //System.out.println("writeLeafBlock pos=" + out.getFilePointer());
      writeLeafBlockDocs(scratchOut, docIDs, 0, count);  // 把排好序的文档Id给存储起来了
      // 存储相同的前缀
      // Write the common prefixes:
      reader.getValue(from, scratchBytesRef1); // copy第一个词
      System.arraycopy(scratchBytesRef1.bytes, scratchBytesRef1.offset, scratch1, 0, packedBytesLength);
      writeCommonPrefixes(scratchOut, commonPrefixLengths, scratch1); // 存储每个维度的前缀

      // Write the full values:
      IntFunction<BytesRef> packedValues = new IntFunction<BytesRef>() {
        @Override
        public BytesRef apply(int i) {
          reader.getValue(from + i, scratchBytesRef1);
          return scratchBytesRef1;
        }
      };
      assert valuesInOrderAndBounds(count, sortedDim, minPackedValue, maxPackedValue, packedValues,
              docIDs, 0);  // 再写入叶子剩余数据
      writeLeafBlockPackedValues(scratchOut, commonPrefixLengths, count, sortedDim, packedValues, leafCardinality);
      // 写入kdd文件
      out.writeBytes(scratchOut.getBytes(), 0, scratchOut.getPosition());
      scratchOut.reset();

    } else { // 是内部节点
      // inner node

      final int splitDim;
      // compute the split dimension and partition around it
      if (numIndexDims == 1) { // 只有一个维度
        splitDim = 0;
      } else { // 至少两个维度的数据
        // for dimensions > 2 we recompute the bounds for the current inner node to help the algorithm choose best
        // split dimensions. Because it is an expensive operation, the frequency we recompute the bounds is given
        // by SPLITS_BEFORE_EXACT_BOUNDS. // 大于2个维度的话，为了最好的拆分，需要重新找下最大值和最小值
        if (numLeaves != leafBlockFPs.length && numIndexDims > 2 && Arrays.stream(parentSplits).sum() % SPLITS_BEFORE_EXACT_BOUNDS == 0) {
          computePackedValueBounds(reader, from, to, minPackedValue, maxPackedValue, scratchBytesRef1);
        }
        splitDim = split(minPackedValue, maxPackedValue, parentSplits);// 每个维度最大值和最小值差值的最大的那个维度。决定以这个维度开始拆分
      }
      // 左子树上的叶子节点
      // How many leaves will be in the left tree:
      int numLeftLeafNodes = getNumLeftLeafNodes(numLeaves);
      // How many points will be in the left tree:
      final int mid = from + numLeftLeafNodes * maxPointsInLeafNode; // 右子树，第一个point值
      // 确定最大值和最小值相同的前缀长度
      int commonPrefixLen = FutureArrays.mismatch(minPackedValue, splitDim * bytesPerDim,
              splitDim * bytesPerDim + bytesPerDim, maxPackedValue, splitDim * bytesPerDim,
              splitDim * bytesPerDim + bytesPerDim);
      if (commonPrefixLen == -1) {
        commonPrefixLen = bytesPerDim;
      }
      // 通过基数排序+快排实现了排序，保证中间数左右有序（最中间的那个桶也保证有序）
      MutablePointsReaderUtils.partition(numDataDims, numIndexDims, maxDoc, splitDim, bytesPerDim, commonPrefixLen,
              reader, from, to, mid, scratchBytesRef1, scratchBytesRef2);

      final int rightOffset = leavesOffset + numLeftLeafNodes; // 右边叶子节点下标
      final int splitOffset = rightOffset - 1; // 拆分时那个节点实际下标（从0开始）
      // set the split value
      final int address = splitOffset * bytesPerDim;// 存储值的绝对位置
      splitDimensionValues[splitOffset] = (byte) splitDim;// 以哪个节点哪个维度开始切分
      reader.getValue(mid, scratchBytesRef1);// 把中间那个值给读取出来，放在scratchBytesRef1中
      System.arraycopy(scratchBytesRef1.bytes, scratchBytesRef1.offset + splitDim * bytesPerDim, splitPackedValues, address, bytesPerDim);// 把哪个节点的切分维度的值放入splitPackedValues中

      byte[] minSplitPackedValue = ArrayUtil.copyOfSubArray(minPackedValue, 0, packedIndexBytesLength); //从minPackedValue中copy一份最小值
      byte[] maxSplitPackedValue = ArrayUtil.copyOfSubArray(maxPackedValue, 0, packedIndexBytesLength); //从maxPackedValue中copy一份最大值
      System.arraycopy(scratchBytesRef1.bytes, scratchBytesRef1.offset + splitDim * bytesPerDim,
              minSplitPackedValue, splitDim * bytesPerDim, bytesPerDim); //把中间值放在放在minSplitPackedValue中
      System.arraycopy(scratchBytesRef1.bytes, scratchBytesRef1.offset + splitDim * bytesPerDim,
              maxSplitPackedValue, splitDim * bytesPerDim, bytesPerDim);// 把中间值，放在maxSplitPackedValue中

      // recurse
      parentSplits[splitDim]++; // 统计哪个维度被切分了
      build(leavesOffset, numLeftLeafNodes, reader, from, mid, out,// 左中
              minPackedValue, maxSplitPackedValue, parentSplits,
              splitPackedValues, splitDimensionValues, leafBlockFPs, spareDocIds);
      build(rightOffset, numLeaves - numLeftLeafNodes, reader, mid, to, out,// 中又
              minSplitPackedValue, maxPackedValue, parentSplits,
              splitPackedValues, splitDimensionValues, leafBlockFPs, spareDocIds);
      parentSplits[splitDim]--;// 对于递归上一层父母来说，本节点还没有被拆分
    }
  }

  private void computePackedValueBounds(BKDRadixSelector.PathSlice slice, byte[] minPackedValue, byte[] maxPackedValue) throws IOException {
    try (PointReader reader = slice.writer.getReader(slice.start, slice.count)) {
      if (reader.next() == false) {
        return;
      }
      BytesRef value = reader.pointValue().packedValue();
      System.arraycopy(value.bytes, value.offset, minPackedValue, 0, packedIndexBytesLength);
      System.arraycopy(value.bytes, value.offset, maxPackedValue, 0, packedIndexBytesLength);
      while (reader.next()) {
        value = reader.pointValue().packedValue();
        for(int dim = 0; dim < numIndexDims; dim++) {
          final int startOffset = dim * bytesPerDim;
          final int endOffset = startOffset + bytesPerDim;
          if (FutureArrays.compareUnsigned(value.bytes, value.offset + startOffset, value.offset + endOffset, minPackedValue, startOffset, endOffset) < 0) {
            System.arraycopy(value.bytes, value.offset + startOffset, minPackedValue, startOffset, bytesPerDim);
          } else if (FutureArrays.compareUnsigned(value.bytes, value.offset + startOffset, value.offset + endOffset, maxPackedValue, startOffset, endOffset) > 0) {
            System.arraycopy(value.bytes, value.offset + startOffset, maxPackedValue, startOffset, bytesPerDim);
          }
        }
      }
    }
  }

  /** The point writer contains the data that is going to be splitted using radix selection.
   /*  This method is used when we are merging previously written segments, in the numDims > 1 case. */
  private void build(int leavesOffset, int numLeaves,// leafNodeOffset:叶子节点的个数
                     BKDRadixSelector.PathSlice points,
                     IndexOutput out,
                     BKDRadixSelector radixSelector,
                     byte[] minPackedValue, byte[] maxPackedValue,
                     int[] parentSplits,
                     byte[] splitPackedValues,
                     byte[] splitDimensionValues,
                     long[] leafBlockFPs,
                     int[] spareDocIds) throws IOException {

    if (numLeaves == 1) {

      // Leaf node: write block
      // We can write the block in any order so by default we write it sorted by the dimension that has the
      // least number of unique bytes at commonPrefixLengths[dim], which makes compression more efficient
      HeapPointWriter heapSource;
      if (points.writer instanceof HeapPointWriter == false) {
        // Adversarial cases can cause this, e.g. merging big segments with most of the points deleted
        heapSource = switchToHeap(points.writer);
      } else {
        heapSource = (HeapPointWriter) points.writer;
      }

      int from = Math.toIntExact(points.start);
      int to = Math.toIntExact(points.start + points.count);
      //we store common prefix on scratch1
      computeCommonPrefixLength(heapSource, scratch1, from, to);

      int sortedDim = 0;
      int sortedDimCardinality = Integer.MAX_VALUE;
      FixedBitSet[] usedBytes = new FixedBitSet[numDataDims];
      for (int dim = 0; dim < numDataDims; ++dim) {
        if (commonPrefixLengths[dim] < bytesPerDim) {
          usedBytes[dim] = new FixedBitSet(256);
        }
      }
      //Find the dimension to compress
      for (int dim = 0; dim < numDataDims; dim++) {
        int prefix = commonPrefixLengths[dim];
        if (prefix < bytesPerDim) {
          int offset = dim * bytesPerDim;
          for (int i = from; i < to; ++i) {
            PointValue value = heapSource.getPackedValueSlice(i);
            BytesRef packedValue = value.packedValue();
            int bucket = packedValue.bytes[packedValue.offset + offset + prefix] & 0xff;
            usedBytes[dim].set(bucket);
          }
          int cardinality =usedBytes[dim].cardinality();
          if (cardinality < sortedDimCardinality) {
            sortedDim = dim;
            sortedDimCardinality = cardinality;
          }
        }
      }

      // sort the chosen dimension
      radixSelector.heapRadixSort(heapSource, from, to, sortedDim, commonPrefixLengths[sortedDim]);
      // compute cardinality
      int leafCardinality = heapSource.computeCardinality(from ,to, numDataDims, bytesPerDim, commonPrefixLengths);

      // Save the block file pointer:
      leafBlockFPs[leavesOffset] = out.getFilePointer();
      //System.out.println("  write leaf block @ fp=" + out.getFilePointer());

      // Write docIDs first, as their own chunk, so that at intersect time we can add all docIDs w/o
      // loading the values:
      int count = to - from;
      assert count > 0: "numLeaves=" + numLeaves + " leavesOffset=" + leavesOffset;
      assert count <= spareDocIds.length : "count=" + count + " > length=" + spareDocIds.length;
      // Write doc IDs
      int[] docIDs = spareDocIds;
      for (int i = 0; i < count; i++) {
        docIDs[i] = heapSource.getPackedValueSlice(from + i).docID();
      }
      writeLeafBlockDocs(out, docIDs, 0, count);

      // TODO: minor opto: we don't really have to write the actual common prefixes, because BKDReader on recursing can regenerate it for us
      // from the index, much like how terms dict does so from the FST:

      // Write the common prefixes:
      writeCommonPrefixes(out, commonPrefixLengths, scratch1);

      // Write the full values:
      IntFunction<BytesRef> packedValues = new IntFunction<BytesRef>() {
        final BytesRef scratch = new BytesRef();

        {
          scratch.length = packedBytesLength;
        }

        @Override
        public BytesRef apply(int i) {
          PointValue value = heapSource.getPackedValueSlice(from + i);
          return value.packedValue();
        }
      };
      assert valuesInOrderAndBounds(count, sortedDim, minPackedValue, maxPackedValue, packedValues,
              docIDs, 0);
      writeLeafBlockPackedValues(out, commonPrefixLengths, count, sortedDim, packedValues, leafCardinality);

    } else {
      // Inner node: partition/recurse

      final int splitDim;
      if (numIndexDims == 1) {
        splitDim = 0;
      } else {
        // for dimensions > 2 we recompute the bounds for the current inner node to help the algorithm choose best
        // split dimensions. Because it is an expensive operation, the frequency we recompute the bounds is given
        // by SPLITS_BEFORE_EXACT_BOUNDS.
        if (numLeaves != leafBlockFPs.length && numIndexDims > 2 && Arrays.stream(parentSplits).sum() % SPLITS_BEFORE_EXACT_BOUNDS == 0) {
          computePackedValueBounds(points, minPackedValue, maxPackedValue);
        }
        splitDim = split(minPackedValue, maxPackedValue, parentSplits);
      }

      assert numLeaves <= leafBlockFPs.length : "numLeaves=" + numLeaves + " leafBlockFPs.length=" + leafBlockFPs.length;

      // How many leaves will be in the left tree:
      int numLeftLeafNodes = getNumLeftLeafNodes(numLeaves);
      // How many points will be in the left tree:
      final long leftCount = numLeftLeafNodes * maxPointsInLeafNode;

      BKDRadixSelector.PathSlice[] slices = new BKDRadixSelector.PathSlice[2];

      int commonPrefixLen = FutureArrays.mismatch(minPackedValue, splitDim * bytesPerDim,
              splitDim * bytesPerDim + bytesPerDim, maxPackedValue, splitDim * bytesPerDim,
              splitDim * bytesPerDim + bytesPerDim);
      if (commonPrefixLen == -1) {
        commonPrefixLen = bytesPerDim;
      }

      byte[] splitValue = radixSelector.select(points, slices, points.start, points.start + points.count,  points.start + leftCount, splitDim, commonPrefixLen);

      final int rightOffset = leavesOffset + numLeftLeafNodes;
      final int splitValueOffset = rightOffset - 1;

      splitDimensionValues[splitValueOffset] = (byte) splitDim;
      int address = splitValueOffset * bytesPerDim;
      System.arraycopy(splitValue, 0, splitPackedValues, address, bytesPerDim);

      byte[] minSplitPackedValue = new byte[packedIndexBytesLength];
      System.arraycopy(minPackedValue, 0, minSplitPackedValue, 0, packedIndexBytesLength);

      byte[] maxSplitPackedValue = new byte[packedIndexBytesLength];
      System.arraycopy(maxPackedValue, 0, maxSplitPackedValue, 0, packedIndexBytesLength);

      System.arraycopy(splitValue, 0, minSplitPackedValue, splitDim * bytesPerDim, bytesPerDim);
      System.arraycopy(splitValue, 0, maxSplitPackedValue, splitDim * bytesPerDim, bytesPerDim);

      parentSplits[splitDim]++;
      // Recurse on left tree:
      build(leavesOffset, numLeftLeafNodes, slices[0],
              out, radixSelector, minPackedValue, maxSplitPackedValue,
              parentSplits, splitPackedValues, splitDimensionValues, leafBlockFPs, spareDocIds);

      // Recurse on right tree:
      build(rightOffset, numLeaves - numLeftLeafNodes, slices[1],
              out, radixSelector, minSplitPackedValue, maxPackedValue,
              parentSplits, splitPackedValues, splitDimensionValues, leafBlockFPs, spareDocIds);

      parentSplits[splitDim]--;
    }
  }

  private void computeCommonPrefixLength(HeapPointWriter heapPointWriter, byte[] commonPrefix, int from, int to) {
    Arrays.fill(commonPrefixLengths, bytesPerDim);
    PointValue value = heapPointWriter.getPackedValueSlice(from);
    BytesRef packedValue = value.packedValue();
    for (int dim = 0; dim < numDataDims; dim++) {
      System.arraycopy(packedValue.bytes, packedValue.offset + dim * bytesPerDim, commonPrefix, dim * bytesPerDim, bytesPerDim);
    }
    for (int i = from + 1; i < to; i++) {
      value =  heapPointWriter.getPackedValueSlice(i);
      packedValue = value.packedValue();
      for (int dim = 0; dim < numDataDims; dim++) {
        if (commonPrefixLengths[dim] != 0) {
          int j = FutureArrays.mismatch(commonPrefix, dim * bytesPerDim, dim * bytesPerDim + commonPrefixLengths[dim], packedValue.bytes, packedValue.offset + dim * bytesPerDim, packedValue.offset + dim * bytesPerDim + commonPrefixLengths[dim]);
          if (j != -1) {
            commonPrefixLengths[dim] = j;
          }
        }
      }
    }
  }

  // only called from assert
  private boolean valuesInOrderAndBounds(int count, int sortedDim, byte[] minPackedValue, byte[] maxPackedValue,
                                         IntFunction<BytesRef> values, int[] docs, int docsOffset) throws IOException {
    byte[] lastPackedValue = new byte[packedBytesLength];
    int lastDoc = -1;
    for (int i=0;i<count;i++) {
      BytesRef packedValue = values.apply(i);
      assert packedValue.length == packedBytesLength;
      assert valueInOrder(i, sortedDim, lastPackedValue, packedValue.bytes, packedValue.offset,
              docs[docsOffset + i], lastDoc);
      lastDoc = docs[docsOffset + i];

      // Make sure this value does in fact fall within this leaf cell:
      assert valueInBounds(packedValue, minPackedValue, maxPackedValue);
    }
    return true;
  }

  // only called from assert
  private boolean valueInOrder(long ord, int sortedDim, byte[] lastPackedValue, byte[] packedValue, int packedValueOffset,
                               int doc, int lastDoc) {
    int dimOffset = sortedDim * bytesPerDim;
    if (ord > 0) {
      int cmp = FutureArrays.compareUnsigned(lastPackedValue, dimOffset, dimOffset + bytesPerDim, packedValue, packedValueOffset + dimOffset, packedValueOffset + dimOffset + bytesPerDim);
      if (cmp > 0) {
        throw new AssertionError("values out of order: last value=" + new BytesRef(lastPackedValue) + " current value=" + new BytesRef(packedValue, packedValueOffset, packedBytesLength) + " ord=" + ord);
      }
      if (cmp == 0  && numDataDims > numIndexDims) {
        int dataOffset = numIndexDims * bytesPerDim;
        cmp = FutureArrays.compareUnsigned(lastPackedValue, dataOffset, packedBytesLength, packedValue, packedValueOffset + dataOffset, packedValueOffset + packedBytesLength);
        if (cmp > 0) {
          throw new AssertionError("data values out of order: last value=" + new BytesRef(lastPackedValue) + " current value=" + new BytesRef(packedValue, packedValueOffset, packedBytesLength) + " ord=" + ord);
        }
      }
      if (cmp == 0 && doc < lastDoc) {
        throw new AssertionError("docs out of order: last doc=" + lastDoc + " current doc=" + doc + " ord=" + ord);
      }
    }
    System.arraycopy(packedValue, packedValueOffset, lastPackedValue, 0, packedBytesLength);
    return true;
  }
}
