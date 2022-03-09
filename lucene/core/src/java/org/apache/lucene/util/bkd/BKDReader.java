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

import java.io.IOException;
import java.io.UncheckedIOException;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FutureArrays;
import org.apache.lucene.util.MathUtil;

/** Handles intersection of an multi-dimensional shape in byte[] space with a block KD-tree previously written with {@link BKDWriter}.
 *
 * @lucene.experimental */
// 引入针对数值类型的新索引数据结构：BKD-Tree，用于优化Lucene中范围查询的性能。由于这一索引结构最初用于地理坐标场景，因此被命名为Point索引。
public final class BKDReader extends PointValues {

  // Packed array of byte[] holding all split values in the full binary tree:
  final int leafNodeOffset; //  多少叶子节点
  final int numDataDims; // 一个元素几个维度
  final int numIndexDims; // 一个元素几个维度
  final int bytesPerDim;
  final int numLeaves;//  多少叶子节点
  final IndexInput in; // kdd文件
  final int maxPointsInLeafNode; // 一个叶子节点多少个Point
  final byte[] minPackedValue; // 该segment中最大/最小的那个值
  final byte[] maxPackedValue;
  final long pointCount; //
  final int docCount; // 这俩兄弟
  final int version;
  protected final int packedBytesLength;
  protected final int packedIndexBytesLength; // 一个元素占用的空间
  final long minLeafBlockFP;

  final IndexInput packedIndex;// 就是kdi文件读取
// 读取的是kdm文件，写入过程详见 BKDWriter.writeIndex()函数
  /** Caller must pre-seek the provided {@link IndexInput} to the index location that {@link BKDWriter#finish} returned.
   * BKD tree is always stored off-heap. */
  public BKDReader(IndexInput metaIn, IndexInput indexIn, IndexInput dataIn) throws IOException {
    version = CodecUtil.checkHeader(metaIn, BKDWriter.CODEC_NAME, BKDWriter.VERSION_START, BKDWriter.VERSION_CURRENT);// 读取dim文件
    numDataDims = metaIn.readVInt(); // 从kdm中读取数据
    if (version >= BKDWriter.VERSION_SELECTIVE_INDEXING) {
      numIndexDims = metaIn.readVInt();
    } else {
      numIndexDims = numDataDims;
    }
    maxPointsInLeafNode = metaIn.readVInt();// 每个页节点的元素个数
    bytesPerDim = metaIn.readVInt();// 每一个元素中每以阶的大小
    packedBytesLength = numDataDims * bytesPerDim;
    packedIndexBytesLength = numIndexDims * bytesPerDim;

    // Read index:
    numLeaves = metaIn.readVInt(); // 多少个叶子节点
    assert numLeaves > 0;
    leafNodeOffset = numLeaves;

    minPackedValue = new byte[packedIndexBytesLength];
    maxPackedValue = new byte[packedIndexBytesLength];
// 每个维度的最大值、最小值
    metaIn.readBytes(minPackedValue, 0, packedIndexBytesLength);
    metaIn.readBytes(maxPackedValue, 0, packedIndexBytesLength);

    for(int dim=0;dim<numIndexDims;dim++) { // 比较每个维度最大值和最小值
      if (FutureArrays.compareUnsigned(minPackedValue, dim * bytesPerDim, dim * bytesPerDim + bytesPerDim, maxPackedValue, dim * bytesPerDim, dim * bytesPerDim + bytesPerDim) > 0) {
        throw new CorruptIndexException("minPackedValue " + new BytesRef(minPackedValue) + " is > maxPackedValue " + new BytesRef(maxPackedValue) + " for dim=" + dim, metaIn);
      }
    }
    
    pointCount = metaIn.readVLong();// 该segment总共的元素个数
    docCount = metaIn.readVInt();// 总共存在多少个文档中

    int numIndexBytes = metaIn.readVInt();// BKD树转存结构
    long indexStartPointer;
    if (version >= BKDWriter.VERSION_META_FILE) {
      minLeafBlockFP = metaIn.readLong(); // kdd文件存储的起始位置
      indexStartPointer = metaIn.readLong(); // kdi中读取索引结构的起始位置
    } else {
      indexStartPointer = indexIn.getFilePointer();
      minLeafBlockFP = indexIn.readVLong();
      indexIn.seek(indexStartPointer);// 跳转到dki中，可以直接读取叶子信息
    }
    this.packedIndex = indexIn.slice("packedIndex", indexStartPointer, numIndexBytes);// 定位到kdi中，方便读取该
    this.in = dataIn;
  }

  long getMinLeafBlockFP() {
    return minLeafBlockFP;
  }
  // 这个结构是为了遍历树的,仅从根节点开始，开始一个子节点
  /** Used to walk the off-heap index. The format takes advantage of the limited
   *  access pattern to the BKD tree at search time, i.e. starting at the root
   *  node and recursing downwards one child at a time.
   *  @lucene.internal */
  public class IndexTree implements Cloneable {
    private int nodeID; //当前节点编号,从1开始
    // level is 1-based so that we can do level-1 w/o checking each time:
    private int level; // 树高从1开始的
    private int splitDim;// 拆分出来的当前节点的切分边
    private final byte[][] splitPackedValueStack; // 临时变量，免得一直申请
    // used to read the packed tree off-heap
    private final IndexInput in; // 映射kdi文件
    // holds the minimum (left most) leaf block file pointer for each level we've recursed to:
    private final long[] leafBlockFPStack;//从kdi文件读取每个level第一个叶子在kdd文件中开始存放位置。
    // holds the address, in the off-heap index, of the right-node of each level:
    private final int[] rightNodePositions; // 这层的右子树所有bytes在kdi中的起始位置
    // holds the splitDim for each level:
    private final int[] splitDims; // 记录的深度链路每层当前切分的值
    // true if the per-dim delta we read for the node at this level is a negative offset vs. the last split on this dim; this is a packed
    // 2D array, i.e. to access array[level][dim] you read from negativeDeltas[level*numDims+dim].  this will be true if the last time we
    // split on this dimension, we next pushed to the left sub-tree:
    private final boolean[] negativeDeltas; // negativeDeltas
    // holds the packed per-level split values; the intersect method uses this to save the cell min/max as it recurses:
    private final byte[][] splitValuesStack; // 记录的某个维度上一次切分的值
    // scratch value to return from getPackedValue:
    private final BytesRef scratch; //
    // 为了遍历树
    IndexTree() {
      this(packedIndex.clone(), 1, 1);
      // read root node
      readNodeData(false); // 第一次从kdi文件中读取根节点
    }
    // 遍历尝试读取kdi树的转存结构
    private IndexTree(IndexInput in, int nodeID, int level) {
      int treeDepth = getTreeDepth();// 获取树的深度
      splitPackedValueStack = new byte[treeDepth+1][];
      this.nodeID = nodeID;
      this.level = level;
      splitPackedValueStack[level] = new byte[packedIndexBytesLength];// 一个元素所有维度的空间
      leafBlockFPStack = new long[treeDepth+1];
      rightNodePositions = new int[treeDepth+1];
      splitValuesStack = new byte[treeDepth+1][];
      splitDims = new int[treeDepth+1];
      negativeDeltas = new boolean[numIndexDims*(treeDepth+1)]; // 树的每个高度都有一个
      this.in = in; // kdi文件
      splitValuesStack[0] = new byte[packedIndexBytesLength];
      scratch = new BytesRef();
      scratch.length = bytesPerDim;
    }      
    //indexTree向左移动一步
    public void pushLeft() {
      nodeID *= 2;
      level++;
      readNodeData(true);
    }
    
    /** Clone, but you are not allowed to pop up past the point where the clone happened. */
    @Override
    public IndexTree clone() {
      IndexTree index = new IndexTree(in.clone(), nodeID, level);
      // copy node data
      index.splitDim = splitDim;
      index.leafBlockFPStack[level] = leafBlockFPStack[level];
      index.rightNodePositions[level] = rightNodePositions[level];
      index.splitValuesStack[index.level] = splitValuesStack[index.level].clone();
      System.arraycopy(negativeDeltas, level*numIndexDims, index.negativeDeltas, level*numIndexDims, numIndexDims);
      index.splitDims[level] = splitDims[level];
      return index;
    }
    
    public void pushRight() {
      final int nodePosition = rightNodePositions[level];
      assert nodePosition >= in.getFilePointer() : "nodePosition = " + nodePosition + " < currentPosition=" + in.getFilePointer();
      nodeID = nodeID * 2 + 1;
      level++;
      try {
        in.seek(nodePosition); //kdi文件 直接定位到rightNode
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      readNodeData(false);
    }

    public void pop() {
      nodeID /= 2;
      level--;
      splitDim = splitDims[level];
      //System.out.println("  pop nodeID=" + nodeID);
    }

    public boolean isLeafNode() {// 对于最小的叶子节点，2*nodeID-1>=(nodeID-1)+叶子个数，nodeID=leafNodeOffset
      return nodeID >= leafNodeOffset;// 那么叶子点全部满足nodeID >= leafNodeOffset
    }
    //
    public boolean nodeExists() { // 若最后一个叶子节点为nodeID，那么最后一个非叶子节点：nodeID/2，那么nodeID/2+叶子个数=nodeID，也就是nodeID=2*叶子个数
      return nodeID - leafNodeOffset < leafNodeOffset; // 若最后没有右子树，nodeID/2+叶子个数=nodeID，若有右子树，（nodeID-1)/2+叶子个数=nodeID
    } // 不清楚这里到底判断的啥？最大一个叶子节点nodeID:nodeID-nodeID/2=叶子个数   =》  nodeID/2=叶子个数。

    public int getNodeID() {
      return nodeID;
    }

    public byte[] getSplitPackedValue() {
      assert isLeafNode() == false;
      assert splitPackedValueStack[level] != null: "level=" + level;
      return splitPackedValueStack[level];
    }
                                                       
    /** Only valid after pushLeft or pushRight, not pop! */
    public int getSplitDim() {
      assert isLeafNode() == false;
      return splitDim;
    }

    /** Only valid after pushLeft or pushRight, not pop! */
    public BytesRef getSplitDimValue() {
      assert isLeafNode() == false;
      scratch.bytes = splitValuesStack[level];
      scratch.offset = splitDim * bytesPerDim;
      return scratch;
    }
    
    /** Only valid after pushLeft or pushRight, not pop! */
    public long getLeafBlockFP() {
      assert isLeafNode(): "nodeID=" + nodeID + " is not a leaf";
      return leafBlockFPStack[level];
    }

    /** Return the number of leaves below the current node. */
    public int getNumLeaves() {
      int leftMostLeafNode = nodeID;
      while (leftMostLeafNode < leafNodeOffset) {
        leftMostLeafNode = leftMostLeafNode * 2;
      }
      int rightMostLeafNode = nodeID;
      while (rightMostLeafNode < leafNodeOffset) {
        rightMostLeafNode = rightMostLeafNode * 2 + 1;
      }
      final int numLeaves;
      if (rightMostLeafNode >= leftMostLeafNode) {
        // both are on the same level
        numLeaves = rightMostLeafNode - leftMostLeafNode + 1;
      } else {
        // left is one level deeper than right
        numLeaves = rightMostLeafNode - leftMostLeafNode + 1 + leafNodeOffset;
      }
      assert numLeaves == getNumLeavesSlow(nodeID) : numLeaves + " " + getNumLeavesSlow(nodeID);
      return numLeaves;
    }

    // for assertions
    private int getNumLeavesSlow(int node) {
      if (node >= 2 * leafNodeOffset) {
        return 0;
      } else if (node >= leafNodeOffset) {
        return 1;
      } else {
        final int leftCount = getNumLeavesSlow(node * 2);
        final int rightCount = getNumLeavesSlow(node * 2 + 1);
        return leftCount + rightCount;
      }
    }
    // 开始读取当前节点，当前节点以level和nodeId标识。和 BKDWriter.recursePackIndex() 写入过程文件一一对应
    private void readNodeData(boolean isLeft) {
      if (splitPackedValueStack[level] == null) { // 此时level已经是当前level
        splitPackedValueStack[level] = new byte[packedIndexBytesLength];
      }// 上次的拿过来的来更新本地
      System.arraycopy(negativeDeltas, (level-1)*numIndexDims, negativeDeltas, level*numIndexDims, numIndexDims); // 记录了上次每个维度的最后一次的切分情况
      assert splitDim != -1;
      negativeDeltas[level*numIndexDims+splitDim] = isLeft; // 修正这个维度最近一次的是左子树还是右子树切分
      // level和splitDim都还是上层的
      try {
        leafBlockFPStack[level] = leafBlockFPStack[level - 1];//

        // read leaf block FP delta
        if (isLeft == false) { // 右边的话
          leafBlockFPStack[level] += in.readVLong(); // 先从kdi文件中开始读取的，读取kdd中存放的第一个叶子起始位置。存放的delta
        }

        if (isLeafNode()) { // 若是叶子节点，则本次读取完成
          splitDim = -1;
        } else {
            // 见BKDWriter中982行编码过程
          // read split dim, prefix, firstDiffByteDelta encoded as int:
          int code = in.readVInt(); // 读kdi文件
          splitDim = code % numIndexDims;
          splitDims[level] = splitDim;
          code /= numIndexDims;
          int prefix = code % (1 + bytesPerDim);// 前缀
          int suffix = bytesPerDim - prefix; // 后半段

          if (splitValuesStack[level] == null) {// 临时变量
            splitValuesStack[level] = new byte[packedIndexBytesLength];
          }// 把上一个切分的值拿来，以便读取当前切分的原值
          System.arraycopy(splitValuesStack[level - 1], 0, splitValuesStack[level], 0, packedIndexBytesLength);
          if (suffix > 0) {
            int firstDiffByteDelta = code / (1 + bytesPerDim);
            if (negativeDeltas[level * numIndexDims + splitDim]) { // 本节点是左边节点的话
              firstDiffByteDelta = -firstDiffByteDelta;
            }
            int oldByte = splitValuesStack[level][splitDim * bytesPerDim + prefix] & 0xFF; // 读取上一个节点不一样原始值
            splitValuesStack[level][splitDim * bytesPerDim + prefix] = (byte) (oldByte + firstDiffByteDelta); // 修改这次切分值和上次切分值得差值部分
            in.readBytes(splitValuesStack[level], splitDim * bytesPerDim + prefix + 1, suffix - 1); // 读取这次切分的不相同的部分
          } else {
            // our split value is == last split value in this dim, which can happen when there are many duplicate values
          }
          // kdi存储：delte,code,diffChar,suffix;leftLength;leftBlock;rightBlock
          int leftNumBytes;                    // readNow
          if (nodeID * 2 < leafNodeOffset) {
            leftNumBytes = in.readVInt(); // 读取左子树的存储空间，等于BKDWriter.recursePackIndex()中返回值中的bytes2.length
          } else {// （左叶子节点不存储leftNumBytes，直接返回0了）
            leftNumBytes = 0;
          } // 比较妙，就是右子树在kdd存储的起始位置
          rightNodePositions[level] = Math.toIntExact(in.getFilePointer()) + leftNumBytes; // rightBlock
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }
  // 从1层开始算起的话，若完全二叉树的话，第h层，可以有2^(h-1)个叶子节点.若不是完全二叉树的话，第h最少有2^(h-2)个叶子节点)，则2^(h-1)>=x>=2^(h-2)
  private int getTreeDepth() {
    // First +1 because all the non-leave nodes makes another power
    // of 2; e.g. to have a fully balanced tree with 4 leaves you
    // need a depth=3 tree:

    // Second +1 because MathUtil.log computes floor of the logarithm; e.g.
    // with 5 leaves you need a depth=4 tree:
    return MathUtil.log(numLeaves, 2) + 2; // lnx+1<=x<lnx+2,因为lnx取值必须为整数，则舍弃了小数，则x=lnx+2
  }

  /** Used to track all state for a single call to {@link #intersect}. */
  public static final class IntersectState {
    final IndexInput in; // 读取的是kdd文件
    final BKDReaderDocIDSetIterator scratchIterator;
    final byte[] scratchDataPackedValue, scratchMinIndexPackedValue, scratchMaxIndexPackedValue; // 临时使用的
    final int[] commonPrefixLengths;

    final IntersectVisitor visitor;
    public final IndexTree index;

    public IntersectState(IndexInput in, int numDims,
                          int packedBytesLength,
                          int packedIndexBytesLength,
                          int maxPointsInLeafNode,
                          IntersectVisitor visitor,
                          IndexTree indexVisitor) {
      this.in = in;
      this.visitor = visitor;
      this.commonPrefixLengths = new int[numDims]; // 一个元素几个阶
      this.scratchIterator = new BKDReaderDocIDSetIterator(maxPointsInLeafNode); // 每个叶子节点多少元素
      this.scratchDataPackedValue = new byte[packedBytesLength]; // 一个元素多少位存储
      this.scratchMinIndexPackedValue = new byte[packedIndexBytesLength];
      this.scratchMaxIndexPackedValue = new byte[packedIndexBytesLength];
      this.index = indexVisitor;
    }
  }

  @Override
  public void intersect(IntersectVisitor visitor) throws IOException {
    intersect(getIntersectState(visitor), minPackedValue, maxPackedValue); // 开始正式交互
  }

  @Override
  public long estimatePointCount(IntersectVisitor visitor) {
    return estimatePointCount(getIntersectState(visitor), minPackedValue, maxPackedValue);
  }

  /** Fast path: this is called when the query box fully encompasses all cells under this node. */
  private void addAll(IntersectState state, boolean grown) throws IOException {
    //System.out.println("R: addAll nodeID=" + nodeID);

    if (grown == false) {
      final long maxPointCount = (long) maxPointsInLeafNode * state.index.getNumLeaves();
      if (maxPointCount <= Integer.MAX_VALUE) { // could be >MAX_VALUE if there are more than 2B points in total
        state.visitor.grow((int) maxPointCount);
        grown = true;
      }
    }

    if (state.index.isLeafNode()) {
      assert grown;
      //System.out.println("ADDALL");
      if (state.index.nodeExists()) {
        visitDocIDs(state.in, state.index.getLeafBlockFP(), state.visitor);
      }
      // TODO: we can assert that the first value here in fact matches what the index claimed?
    } else {
      state.index.pushLeft();
      addAll(state, grown);
      state.index.pop();

      state.index.pushRight();
      addAll(state, grown);
      state.index.pop();
    }
  }
  // point和terms公共部分
  /** Create a new {@link IntersectState} */
  public IntersectState getIntersectState(IntersectVisitor visitor) {
    IndexTree index = new IndexTree();
    return new IntersectState(in.clone(), numDataDims, // 映射kdd文件
                              packedBytesLength,
                              packedIndexBytesLength,
                              maxPointsInLeafNode,
                              visitor,
                              index);
  }

  /** Visits all docIDs and packed values in a single leaf block */
  public void visitLeafBlockValues(IndexTree index, IntersectState state) throws IOException {

    // Leaf node; scan and filter all points in this block:
    int count = readDocIDs(state.in, index.getLeafBlockFP(), state.scratchIterator);

    // Again, this time reading values and checking with the visitor
    visitDocValues(state.commonPrefixLengths, state.scratchDataPackedValue, state.scratchMinIndexPackedValue, state.scratchMaxIndexPackedValue, state.in, state.scratchIterator, count, state.visitor);
  }

  private void visitDocIDs(IndexInput in, long blockFP, IntersectVisitor visitor) throws IOException {
    // Leaf node
    in.seek(blockFP);

    // How many points are stored in this leaf cell:
    int count = in.readVInt();
    // No need to call grow(), it has been called up-front

    DocIdsWriter.readInts(in, count, visitor);
  }
   // 读取叶子节点数据,参考 BKDWriter.build()中叶子存储过程：writeLeafBlockDocs函数,先存储的docCount
  int readDocIDs(IndexInput in, long blockFP, BKDReaderDocIDSetIterator iterator) throws IOException {
    in.seek(blockFP); // 跑到KDD文件中的某个叶子节点上了

    // How many points are stored in this leaf cell:
    int count = in.readVInt();
    //再读取的具体Doc内容，保存在iterator.docIDs中
    DocIdsWriter.readInts(in, count, iterator.docIDs);

    return count;
  }

  void visitDocValues(int[] commonPrefixLengths, byte[] scratchDataPackedValue, byte[] scratchMinIndexPackedValue, byte[] scratchMaxIndexPackedValue,
                      IndexInput in, BKDReaderDocIDSetIterator scratchIterator, int count, IntersectVisitor visitor) throws IOException {
    if (version >= BKDWriter.VERSION_LOW_CARDINALITY_LEAVES) { // 默认跑到这里
      visitDocValuesWithCardinality(commonPrefixLengths, scratchDataPackedValue, scratchMinIndexPackedValue, scratchMaxIndexPackedValue, in, scratchIterator, count, visitor);
    } else {
      visitDocValuesNoCardinality(commonPrefixLengths, scratchDataPackedValue, scratchMinIndexPackedValue, scratchMaxIndexPackedValue, in, scratchIterator, count, visitor);
    }
  }

  void visitDocValuesNoCardinality(int[] commonPrefixLengths, byte[] scratchDataPackedValue, byte[] scratchMinIndexPackedValue, byte[] scratchMaxIndexPackedValue,
                      IndexInput in, BKDReaderDocIDSetIterator scratchIterator, int count, IntersectVisitor visitor) throws IOException {
    readCommonPrefixes(commonPrefixLengths, scratchDataPackedValue, in);

    if (numIndexDims != 1 && version >= BKDWriter.VERSION_LEAF_STORES_BOUNDS) {
      byte[] minPackedValue = scratchMinIndexPackedValue;
      System.arraycopy(scratchDataPackedValue, 0, minPackedValue, 0, packedIndexBytesLength);
      byte[] maxPackedValue = scratchMaxIndexPackedValue;
      // Copy common prefixes before reading adjusted box
      System.arraycopy(minPackedValue, 0, maxPackedValue, 0, packedIndexBytesLength);
      readMinMax(commonPrefixLengths, minPackedValue, maxPackedValue, in);

      // The index gives us range of values for each dimension, but the actual range of values
      // might be much more narrow than what the index told us, so we double check the relation
      // here, which is cheap yet might help figure out that the block either entirely matches
      // or does not match at all. This is especially more likely in the case that there are
      // multiple dimensions that have correlation, ie. splitting on one dimension also
      // significantly changes the range of values in another dimension.
      Relation r = visitor.compare(minPackedValue, maxPackedValue);
      if (r == Relation.CELL_OUTSIDE_QUERY) {
        return;
      }
      visitor.grow(count);

      if (r == Relation.CELL_INSIDE_QUERY) {
        for (int i = 0; i < count; ++i) {
          visitor.visit(scratchIterator.docIDs[i]);
        }
        return;
      }
    } else {
      visitor.grow(count);
    }


    int compressedDim = readCompressedDim(in);

    if (compressedDim == -1) {
      visitUniqueRawDocValues(scratchDataPackedValue, scratchIterator, count, visitor);
    } else {
      visitCompressedDocValues(commonPrefixLengths, scratchDataPackedValue, in, scratchIterator, count, visitor, compressedDim);
    }
  }
  // 获取每个point的原始的值
  void visitDocValuesWithCardinality(int[] commonPrefixLengths, byte[] scratchDataPackedValue, byte[] scratchMinIndexPackedValue, byte[] scratchMaxIndexPackedValue,
                                     IndexInput in, BKDReaderDocIDSetIterator scratchIterator, int count, IntersectVisitor visitor) throws IOException {
    // 读取每个维度的前缀，保存在commonPrefixLengths中
    readCommonPrefixes(commonPrefixLengths, scratchDataPackedValue, in);
    int compressedDim = readCompressedDim(in); // 可以看下BKDWriter.writeLeafBlockPackedValues,若为-1，则相等
    if (compressedDim == -1) { // 若读取-1，则说明所有元素相同
      // all values are the same
      visitor.grow(count);
      visitUniqueRawDocValues(scratchDataPackedValue, scratchIterator, count, visitor);
    } else {
      if (numIndexDims != 1) { //
        byte[] minPackedValue = scratchMinIndexPackedValue;
        System.arraycopy(scratchDataPackedValue, 0, minPackedValue, 0, packedIndexBytesLength); // 相同前缀，先给最小最大值赋值
        byte[] maxPackedValue = scratchMaxIndexPackedValue;
        // Copy common prefixes before reading adjusted box
        System.arraycopy(minPackedValue, 0, maxPackedValue, 0, packedIndexBytesLength); // 最小最大值相同前缀
        readMinMax(commonPrefixLengths, minPackedValue, maxPackedValue, in);

        // The index gives us range of values for each dimension, but the actual range of values
        // might be much more narrow than what the index told us, so we double check the relation
        // here, which is cheap yet might help figure out that the block either entirely matches
        // or does not match at all. This is especially more likely in the case that there are
        // multiple dimensions that have correlation, ie. splitting on one dimension also
        // significantly changes the range of values in another dimension.
        Relation r = visitor.compare(minPackedValue, maxPackedValue);
        if (r == Relation.CELL_OUTSIDE_QUERY) { //无关,则不用继续了
          return;
        }
        visitor.grow(count);// 可以跑到ExitableDirectoryReader$ExitableIntersectVisitor

        if (r == Relation.CELL_INSIDE_QUERY) { // 数据完全在查询范围之类
          for (int i = 0; i < count; ++i) { // 遍历每个数据
            visitor.visit(scratchIterator.docIDs[i]);
          }
          return;
        }
      } else { // 只有一维的话，则读取也只是先定义下
        visitor.grow(count);
      }
      if (compressedDim == -2) { //大部分一样，会读取每个文档的value进行读取，并缓存docId
        // low cardinality values  会check value是否满足条件，并记录下docId编号
        visitSparseRawDocValues(commonPrefixLengths, scratchDataPackedValue, in, scratchIterator, count, visitor);
      } else { // 大部分不一样。会有原值范围比较
        // high cardinality
        visitCompressedDocValues(commonPrefixLengths, scratchDataPackedValue, in, scratchIterator, count, visitor, compressedDim);
      }
    }
  }

  private void readMinMax(int[] commonPrefixLengths, byte[] minPackedValue, byte[] maxPackedValue, IndexInput in) throws IOException {
    for (int dim = 0; dim < numIndexDims; dim++) {
      int prefix = commonPrefixLengths[dim];
      in.readBytes(minPackedValue, dim * bytesPerDim + prefix, bytesPerDim - prefix);
      in.readBytes(maxPackedValue, dim * bytesPerDim + prefix, bytesPerDim - prefix);
    }
  }
  // 大部分相同，会读取每个docId的value，并进行比较，将匹配的docId给暂存起来
  // read cardinality and point
  private void visitSparseRawDocValues(int[] commonPrefixLengths, byte[] scratchPackedValue, IndexInput in, BKDReaderDocIDSetIterator scratchIterator, int count, IntersectVisitor visitor) throws IOException {
    int i;
    for (i = 0; i < count;) { // 遍历每个值
      int length = in.readVInt(); // 读取相同元素的个数
      for(int dim = 0; dim < numDataDims; dim++) {
        int prefix = commonPrefixLengths[dim];
        in.readBytes(scratchPackedValue, dim*bytesPerDim + prefix, bytesPerDim - prefix); // 将后缀值读取出来
      } // scratchPackedValue放的读取出来的文档id的value
      scratchIterator.reset(i, length); // BKDReader$BKDReaderDocIDSetIterator， 制定了docId的长度及docId起始位置
      visitor.visit(scratchIterator, scratchPackedValue); // 读取每个docId的value值，对比。
      i += length;
    }
    if (i != count) {
      throw new CorruptIndexException("Sub blocks do not add up to the expected count: " + count + " != " + i, in);
    }
  }

  // point is under commonPrefix
  private void visitUniqueRawDocValues(byte[] scratchPackedValue, BKDReaderDocIDSetIterator scratchIterator, int count, IntersectVisitor visitor) throws IOException {
    scratchIterator.reset(0, count);
    visitor.visit(scratchIterator, scratchPackedValue);
  }

  private void visitCompressedDocValues(int[] commonPrefixLengths, byte[] scratchPackedValue, IndexInput in, BKDReaderDocIDSetIterator scratchIterator, int count, IntersectVisitor visitor, int compressedDim) throws IOException {
    // the byte at `compressedByteOffset` is compressed using run-length compression,
    // other suffix bytes are stored verbatim
    final int compressedByteOffset = compressedDim * bytesPerDim + commonPrefixLengths[compressedDim];
    commonPrefixLengths[compressedDim]++;
    int i;
    for (i = 0; i < count; ) {
      scratchPackedValue[compressedByteOffset] = in.readByte(); // 不相同的第一位
      final int runLen = Byte.toUnsignedInt(in.readByte()); // 不相同的第一位个数
      for (int j = 0; j < runLen; ++j) { // 读取剩余每位的value
        for(int dim = 0; dim < numDataDims; dim++) {
          int prefix = commonPrefixLengths[dim];
          in.readBytes(scratchPackedValue, dim*bytesPerDim + prefix, bytesPerDim - prefix);//读取原始值
        }//读取一个具体的point值，和查询范围作对比，符合的话，会将docId存放在visitor中
        visitor.visit(scratchIterator.docIDs[i+j], scratchPackedValue); //
      }
      i += runLen;
    }
    if (i != count) {
      throw new CorruptIndexException("Sub blocks do not add up to the expected count: " + count + " != " + i, in);
    }
  }

  private int readCompressedDim(IndexInput in) throws IOException {
    int compressedDim = in.readByte();// 相同前缀
    if (compressedDim < -2 || compressedDim >= numDataDims || (version < BKDWriter.VERSION_LOW_CARDINALITY_LEAVES && compressedDim == -2)) {
      throw new CorruptIndexException("Got compressedDim="+compressedDim, in);
    }
    return compressedDim;
  }
  // 在BKDWriter.writeCommonPrefixes()有写入过程
  private void readCommonPrefixes(int[] commonPrefixLengths, byte[] scratchPackedValue, IndexInput in) throws IOException {
    for(int dim=0;dim<numDataDims;dim++) {
      int prefix = in.readVInt();
      commonPrefixLengths[dim] = prefix;
      if (prefix > 0) {
        in.readBytes(scratchPackedValue, dim*bytesPerDim, prefix);
      }
      //System.out.println("R: " + dim + " of " + numDims + " prefix=" + prefix);
    }
  }
  // 交互,和estimatePointCount()大致相似，不同的是，intersect会进入叶子节点具体判断哪些符合
  private void intersect(IntersectState state, byte[] cellMinPacked, byte[] cellMaxPacked) throws IOException {

    /*
    System.out.println("\nR: intersect nodeID=" + state.index.getNodeID());
    for(int dim=0;dim<numDims;dim++) {
      System.out.println("  dim=" + dim + "\n    cellMin=" + new BytesRef(cellMinPacked, dim*bytesPerDim, bytesPerDim) + "\n    cellMax=" + new BytesRef(cellMaxPacked, dim*bytesPerDim, bytesPerDim));
    }
    */

    Relation r = state.visitor.compare(cellMinPacked, cellMaxPacked);

    if (r == Relation.CELL_OUTSIDE_QUERY) {// 不在范围内
      // This cell is fully outside of the query shape: stop recursing
    } else if (r == Relation.CELL_INSIDE_QUERY) {// 完全包含
      // This cell is fully inside of the query shape: recursively add all points in this cell without filtering
      addAll(state, false);
      // The cell crosses the shape boundary, or the cell fully contains the query, so we fall through and do full filtering:
    } else if (state.index.isLeafNode()) { // 当前遍历节点是叶子节点
      
      // TODO: we can assert that the first value here in fact matches what the index claimed?
      
      // In the unbalanced case it's possible the left most node only has one child:
      if (state.index.nodeExists()) { // 可能没有右子节点
        // Leaf node; scan and filter all points in this block:
        int count = readDocIDs(state.in, state.index.getLeafBlockFP(), state.scratchIterator);// 读取具体的文档id
         // 将读取具体的value，比较，缓存docId
        // Again, this time reading values and checking with the visitor
        visitDocValues(state.commonPrefixLengths, state.scratchDataPackedValue, state.scratchMinIndexPackedValue, state.scratchMaxIndexPackedValue, state.in, state.scratchIterator, count, state.visitor);
      }

    } else { // 非叶子节点
      
      // Non-leaf node: recurse on the split left and right nodes
      int splitDim = state.index.getSplitDim(); // 当前节点切分维度
      assert splitDim >= 0: "splitDim=" + splitDim + ", numIndexDims=" + numIndexDims;
      assert splitDim < numIndexDims: "splitDim=" + splitDim + ", numIndexDims=" + numIndexDims;

      byte[] splitPackedValue = state.index.getSplitPackedValue();
      BytesRef splitDimValue = state.index.getSplitDimValue();
      assert splitDimValue.length == bytesPerDim;
      //System.out.println("  splitDimValue=" + splitDimValue + " splitDim=" + splitDim);

      // make sure cellMin <= splitValue <= cellMax:
      assert FutureArrays.compareUnsigned(cellMinPacked, splitDim * bytesPerDim, splitDim * bytesPerDim + bytesPerDim, splitDimValue.bytes, splitDimValue.offset, splitDimValue.offset + bytesPerDim) <= 0: "bytesPerDim=" + bytesPerDim + " splitDim=" + splitDim + " numIndexDims=" + numIndexDims + " numDataDims=" + numDataDims;
      assert FutureArrays.compareUnsigned(cellMaxPacked, splitDim * bytesPerDim, splitDim * bytesPerDim + bytesPerDim, splitDimValue.bytes, splitDimValue.offset, splitDimValue.offset + bytesPerDim) >= 0: "bytesPerDim=" + bytesPerDim + " splitDim=" + splitDim + " numIndexDims=" + numIndexDims + " numDataDims=" + numDataDims;
      // 连续两次给splitPackedValue赋值，存在覆盖的情况
      // Recurse on left sub-tree:
      System.arraycopy(cellMaxPacked, 0, splitPackedValue, 0, packedIndexBytesLength); // 当前最大值
      System.arraycopy(splitDimValue.bytes, splitDimValue.offset, splitPackedValue, splitDim*bytesPerDim, bytesPerDim);
      state.index.pushLeft(); // 遍历左边,是将文件指针指向了kdi、
      intersect(state, cellMinPacked, splitPackedValue);
      state.index.pop();
      // 将splitPackedValue中保存的当前切分维度的值给取出来复原给splitDimValue，免得被修改
      // Restore the split dim value since it may have been overwritten while recursing:
      System.arraycopy(splitPackedValue, splitDim*bytesPerDim, splitDimValue.bytes, splitDimValue.offset, bytesPerDim);

      // Recurse on right sub-tree: // 构建最小值
      System.arraycopy(cellMinPacked, 0, splitPackedValue, 0, packedIndexBytesLength);
      System.arraycopy(splitDimValue.bytes, splitDimValue.offset, splitPackedValue, splitDim*bytesPerDim, bytesPerDim);
      state.index.pushRight();
      intersect(state, splitPackedValue, cellMaxPacked);
      state.index.pop();
    }
  }
  // 预估匹配个数，代表着匹配cost。交互,和intersect()大致相似，不同的是，estimatePointCount只会大致评估多少符合
  private long estimatePointCount(IntersectState state, byte[] cellMinPacked, byte[] cellMaxPacked) {

    /*
    System.out.println("\nR: intersect nodeID=" + state.index.getNodeID());
    for(int dim=0;dim<numDims;dim++) {
      System.out.println("  dim=" + dim + "\n    cellMin=" + new BytesRef(cellMinPacked, dim*bytesPerDim, bytesPerDim) + "\n    cellMax=" + new BytesRef(cellMaxPacked, dim*bytesPerDim, bytesPerDim));
    }
    */
    // 查询与数据范围的关系，该字段最大最小范围值cellMinPacked、cellMaxPacked
    Relation r = state.visitor.compare(cellMinPacked, cellMaxPacked); // 查看关系
    // 完全不相关
    if (r == Relation.CELL_OUTSIDE_QUERY) {
      // This cell is fully outside of the query shape: stop recursing
      return 0L;
    } else if (r == Relation.CELL_INSIDE_QUERY) { // 在范围之内
      return (long) maxPointsInLeafNode * state.index.getNumLeaves();
    } else if (state.index.isLeafNode()) { // 也是叶子节点
      // Assume half the points matched
      return (maxPointsInLeafNode + 1) / 2;// 预估是与一半的叶子节点相交
    } else {
      // 若不是叶子节点，会进行递归遍历
      // Non-leaf node: recurse on the split left and right nodes
      int splitDim = state.index.getSplitDim();
      assert splitDim >= 0: "splitDim=" + splitDim + ", numIndexDims=" + numIndexDims;
      assert splitDim < numIndexDims: "splitDim=" + splitDim + ", numIndexDims=" + numIndexDims;

      byte[] splitPackedValue = state.index.getSplitPackedValue();  // 临时变量
      BytesRef splitDimValue = state.index.getSplitDimValue(); //当前level拆分的值
      assert splitDimValue.length == bytesPerDim;
      //System.out.println("  splitDimValue=" + splitDimValue + " splitDim=" + splitDim);
      // 比较切分点
      // make sure cellMin <= splitValue <= cellMax:
      assert FutureArrays.compareUnsigned(cellMinPacked, splitDim * bytesPerDim, splitDim * bytesPerDim + bytesPerDim, splitDimValue.bytes, splitDimValue.offset, splitDimValue.offset + bytesPerDim) <= 0: "bytesPerDim=" + bytesPerDim + " splitDim=" + splitDim + " numIndexDims=" + numIndexDims + " numDataDims=" + numDataDims;
      assert FutureArrays.compareUnsigned(cellMaxPacked, splitDim * bytesPerDim, splitDim * bytesPerDim + bytesPerDim, splitDimValue.bytes, splitDimValue.offset, splitDimValue.offset + bytesPerDim) >= 0: "bytesPerDim=" + bytesPerDim + " splitDim=" + splitDim + " numIndexDims=" + numIndexDims + " numDataDims=" + numDataDims;

      // Recurse on left sub-tree:检索左边的边，替换该数据范围的最大值， 将拆分那阶的最大值给换成拆分的值
      System.arraycopy(cellMaxPacked, 0, splitPackedValue, 0, packedIndexBytesLength); // 赋值给临时变量
      System.arraycopy(splitDimValue.bytes, splitDimValue.offset, splitPackedValue, splitDim*bytesPerDim, bytesPerDim); // 修改临时变量里面该值最大值
      state.index.pushLeft(); // 到左孩子分支
      final long leftCost = estimatePointCount(state, cellMinPacked, splitPackedValue); // 这里会一直循环
      state.index.pop();// 后退一层
       // 从splitPackedValue中获取切分的值,保存到splitDimValue中
      // Restore the split dim value since it may have been overwritten while recursing:
      System.arraycopy(splitPackedValue, splitDim*bytesPerDim, splitDimValue.bytes, splitDimValue.offset, bytesPerDim);
      // 遍历右边
      // Recurse on right sub-tree: // 产生右边子树的下限边界
      System.arraycopy(cellMinPacked, 0, splitPackedValue, 0, packedIndexBytesLength); // 直接将边界拿来
      System.arraycopy(splitDimValue.bytes, splitDimValue.offset, splitPackedValue, splitDim*bytesPerDim, bytesPerDim); // 修改边界该域
      state.index.pushRight();
      final long rightCost = estimatePointCount(state, splitPackedValue, cellMaxPacked); // 计算右子树的cost
      state.index.pop();
      return leftCost + rightCost;
    }
  }

  @Override
  public byte[] getMinPackedValue() {
    return minPackedValue.clone();
  }

  @Override
  public byte[] getMaxPackedValue() {
    return maxPackedValue.clone();
  }

  @Override
  public int getNumDimensions() {
    return numDataDims;
  }

  @Override
  public int getNumIndexDimensions() {
    return numIndexDims;
  }

  @Override
  public int getBytesPerDimension() {
    return bytesPerDim;
  }

  @Override
  public long size() {
    return pointCount;
  }

  @Override
  public int getDocCount() {
    return docCount; //
  }

  public boolean isLeafNode(int nodeID) {
    return nodeID >= leafNodeOffset;
  }

  /**
   * Reusable {@link DocIdSetIterator} to handle low cardinality leaves. */
  protected static class BKDReaderDocIDSetIterator extends DocIdSetIterator {

    private int idx;
    private int length;
    private int offset;
    private int docID;
    final int[] docIDs; //读取的一个叶子节点的所有DocId内容

    public BKDReaderDocIDSetIterator(int maxPointsInLeafNode) {
      this.docIDs = new int[maxPointsInLeafNode];
    }

    @Override
    public int docID() {
     return docID;
    }

    private void  reset(int offset, int length) {
      this.offset = offset;
      this.length = length;
      assert offset + length <= docIDs.length;
      this.docID = -1;
      this.idx = 0;
    }

    @Override
    public int nextDoc() throws IOException {
      if (idx == length) { // 这次读取的个数是有已达到
        docID = DocIdSetIterator.NO_MORE_DOCS;
      } else {
        docID = docIDs[offset + idx];
        idx++;
      }
      return docID;
    }

    @Override
    public int advance(int target) throws IOException {
      return slowAdvance(target);
    }

    @Override
    public long cost() {
      return length;
    }
  }
}