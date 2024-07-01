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
import java.util.Arrays;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.AbstractDocIdSetIterator;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.MathUtil;

/**
 * Handles reading a block KD-tree in byte[] space previously written with {@link BKDWriter}.
 *
 * @lucene.experimental
 */// 引入针对数值类型的新索引数据结构：BKD-Tree  节点启动的时候，会来初始化该类
public class BKDReader extends PointValues {
  final BKDConfig config;
  final int numLeaves;//  多少叶子节点
  final IndexInput in;// kdd文件
  final byte[] minPackedValue;// 该segment中最大/最小的那个值
  final byte[] maxPackedValue;
  final long pointCount; // 可能一个文档有两个point，一般来说，docCount和pointCount是相等的
  final int docCount; // 涉及到多少个文档
  final int version;
  final long minLeafBlockFP;

  private final long indexStartPointer;// kdi中读取索引结构的起始位置
  private final int numIndexBytes;// 节点启动的时候，会来初始化该类。读取的是kdm文件，写入过程详见 BKDWriter.writeIndex()函数
  private final IndexInput indexIn;// 就是kdi文件读取，在节点重启时候会去映射。（kdm在接电启动时候会去全量读取）
  // if true, the tree is a legacy balanced tree
  private final boolean isTreeBalanced;

  /**
   * Caller must pre-seek the provided {@link IndexInput} to the index location that {@link
   * BKDWriter#finish} returned. BKD tree is always stored off-heap.
   */
  public BKDReader(IndexInput metaIn, IndexInput indexIn, IndexInput dataIn) throws IOException {
    version =
        CodecUtil.checkHeader(
            metaIn, BKDWriter.CODEC_NAME, BKDWriter.VERSION_START, BKDWriter.VERSION_CURRENT);// 读取dim文件
    final int numDims = metaIn.readVInt();// 从kdm中读取数据
    final int numIndexDims;
    if (version >= BKDWriter.VERSION_SELECTIVE_INDEXING) {
      numIndexDims = metaIn.readVInt();
    } else {
      numIndexDims = numDims;
    }
    final int maxPointsInLeafNode = metaIn.readVInt();// 每个页节点的元素个数
    final int bytesPerDim = metaIn.readVInt();// 单个元素某个维度占用的长度
    config = BKDConfig.of(numDims, numIndexDims, bytesPerDim, maxPointsInLeafNode);

    // Read index:
    numLeaves = metaIn.readVInt(); // 多少个叶子节点
    assert numLeaves > 0;

    byte[] minPackedValue = new byte[config.packedIndexBytesLength()];
    byte[] maxPackedValue = new byte[config.packedIndexBytesLength()];
// 每个维度的最大值、最小值
    metaIn.readBytes(minPackedValue, 0, config.packedIndexBytesLength());
    metaIn.readBytes(maxPackedValue, 0, config.packedIndexBytesLength());
    final ArrayUtil.ByteArrayComparator comparator =
        ArrayUtil.getUnsignedComparator(config.bytesPerDim());
    for (int dim = 0; dim < config.numIndexDims(); dim++) {// 比较每个维度最大值和最小值
      if (comparator.compare(
              minPackedValue,
              dim * config.bytesPerDim(),
              maxPackedValue,
              dim * config.bytesPerDim())
          > 0) {
        throw new CorruptIndexException(
            "minPackedValue "
                + new BytesRef(minPackedValue)
                + " is > maxPackedValue "
                + new BytesRef(maxPackedValue)
                + " for dim="
                + dim,
            metaIn);
      }
    }
    this.minPackedValue = minPackedValue;
    if (Arrays.equals(maxPackedValue, minPackedValue)) {
      // save heap for edge case of only a single value
      this.maxPackedValue = minPackedValue;
    } else {
      this.maxPackedValue = maxPackedValue;
    }

    pointCount = metaIn.readVLong();// 该segment总共的元素个数
    docCount = metaIn.readVInt();// 总共存在多少个文档中

    numIndexBytes = metaIn.readVInt();// BKD树转存结构总长度
    if (version >= BKDWriter.VERSION_META_FILE) {
      minLeafBlockFP = metaIn.readLong(); // 在kdd文件存储数据的起始位置
      indexStartPointer = metaIn.readLong(); // kdi中读取索引结构的起始位置
    } else {
      indexStartPointer = indexIn.getFilePointer();
      minLeafBlockFP = indexIn.readVLong();
      indexIn.seek(indexStartPointer);
    }
    this.indexIn = indexIn;// 定位到kdi中，方便读取该
    this.in = dataIn;
    // for only one leaf, balanced and unbalanced trees can be handled the same way
    // we set it to unbalanced.
    this.isTreeBalanced = numLeaves != 1 && isTreeBalanced();
  }

  private boolean isTreeBalanced() throws IOException {
    if (version >= BKDWriter.VERSION_META_FILE) {
      // since lucene 8.6 all trees are unbalanced.
      return false;
    }
    if (config.numDims() > 1) {
      // high dimensional tree in pre-8.6 indices are balanced.
      assert 1 << MathUtil.log(numLeaves, 2) == numLeaves;
      return true;
    }
    if (1 << MathUtil.log(numLeaves, 2) != numLeaves) {
      // if we don't have enough leaves to fill the last level then it is unbalanced
      return false;
    }
    // count of the last node for unbalanced trees
    final int lastLeafNodePointCount = Math.toIntExact(pointCount % config.maxPointsInLeafNode());
    // navigate to last node
    PointTree pointTree = getPointTree();
    do {
      while (pointTree.moveToSibling()) {}
    } while (pointTree.moveToChild());
    // count number of docs in the node
    final int[] count = new int[] {0};
    pointTree.visitDocIDs(
        new IntersectVisitor() {
          @Override
          public void visit(int docID) {
            count[0]++;
          }

          @Override
          public void visit(DocIdSetIterator iterator) throws IOException {
            int docID;
            while ((docID = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
              visit(docID);
            }
          }

          @Override
          public void visit(IntsRef ref) {
            count[0] += ref.length;
          }

          @Override
          public void visit(int docID, byte[] packedValue) {
            throw new AssertionError();
          }

          @Override
          public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
            throw new AssertionError();
          }
        });
    return count[0] != lastLeafNodePointCount;
  }
  // 就是这个bkd树全新的遍历方式
  @Override
  public PointTree getPointTree() throws IOException {
    return new BKDPointTree(
        indexIn.slice("packedIndex", indexStartPointer, numIndexBytes),
        this.in.clone(),
        config,
        numLeaves,
        version,
        pointCount,
        minPackedValue,
        maxPackedValue,
        isTreeBalanced);
  }
  // 这个结构是为了遍历树的,仅从根节点开始，开始一个子节点
  private static class BKDPointTree implements PointTree {
    private int nodeID;//当前节点编号,从1开始
    // during clone, the node root can be different to 1
    private final int nodeRoot;
    // level is 1-based so that we can do level-1 w/o checking each time:
    private int level;// 树高从1开始的
    // used to read the packed tree off-heap
    private final IndexInput innerNodes;// 映射kdi文件
    // used to read the packed leaves off-heap
    private final IndexInput leafNodes;
    // holds the minimum (left most) leaf block file pointer for each level we've recursed to:
    private final long[] leafBlockFPStack;//从kdi文件读取每个level第一个叶子在kdd文件中开始存放位置。
    // holds the address, in the off-heap index, after reading the node data of each level:
    private final int[] readNodeDataPositions;
    // holds the address, in the off-heap index, of the right-node of each level:
    private final int[] rightNodePositions; // 这层的右子树所有bytes在kdi中的起始位置
    // holds the splitDim position for each level:
    private final int[] splitDimsPos;// 拆分出来的当前节点的切分边
    // true if the per-dim delta we read for the node at this level is a negative offset vs. the
    // last split on this dim; this is a packed
    // 2D array, i.e. to access array[level][dim] you read from negativeDeltas[level*numDims+dim].
    // this will be true if the last time we
    // split on this dimension, we next pushed to the left sub-tree:
    private final boolean[] negativeDeltas;// 数高*维度, 当前维度是数的左子树还是右子树
    // holds the packed per-level split values
    private final byte[][] splitValuesStack;// 记录的某个维度上一次切分的值（这里比较粗暴，直接将上个level所有的维度都copy了，没有必要）
    // holds the min / max value of the current node.
    private final byte[] minPackedValue, maxPackedValue;
    // holds the previous value of the split dimension
    private final byte[][] splitDimValueStack;// 临时变量，免得一直申请
    // tree parameters
    private final BKDConfig config;
    // number of leaves
    private final int leafNodeOffset;
    // version of the index
    private final int version;
    // total number of points
    final long pointCount;
    // last node might not be fully populated
    private final int lastLeafNodePointCount;
    // right most leaf node ID
    private final int rightMostLeafNode;
    // helper objects for reading doc values
    private final byte[] scratchDataPackedValue,
        scratchMinIndexPackedValue,
        scratchMaxIndexPackedValue;
    private final int[] commonPrefixLengths;
    private final BKDReaderDocIDSetIterator scratchIterator;
    private final DocIdsWriter docIdsWriter;
    // if true the tree is balanced, otherwise unbalanced
    private final boolean isTreeBalanced;
    private final IntsRef scratchIntsRef = new IntsRef();

    {
      assert scratchIntsRef.offset == 0;
    }

    private BKDPointTree(
        IndexInput innerNodes,
        IndexInput leafNodes,
        BKDConfig config,
        int numLeaves,
        int version,
        long pointCount,
        byte[] minPackedValue,
        byte[] maxPackedValue,
        boolean isTreeBalanced)
        throws IOException {
      this(
          innerNodes,
          leafNodes,
          config,
          numLeaves,
          version,
          pointCount,
          1,
          1,
          minPackedValue,
          maxPackedValue,
          new BKDReaderDocIDSetIterator(config.maxPointsInLeafNode(), version),
          new byte[config.packedBytesLength()],
          new byte[config.packedIndexBytesLength()],
          new byte[config.packedIndexBytesLength()],
          new int[config.numDims()],
          isTreeBalanced);
      // read root node
      readNodeData(false);
    }
    // 遍历尝试读取kdi树的转存结构
    private BKDPointTree(
        IndexInput innerNodes,
        IndexInput leafNodes,
        BKDConfig config,
        int numLeaves,
        int version,
        long pointCount,
        int nodeID,
        int level,
        byte[] minPackedValue,
        byte[] maxPackedValue,
        BKDReaderDocIDSetIterator scratchIterator,
        byte[] scratchDataPackedValue,
        byte[] scratchMinIndexPackedValue,
        byte[] scratchMaxIndexPackedValue,
        int[] commonPrefixLengths,
        boolean isTreeBalanced) {
      this.config = config;
      this.version = version;
      this.nodeID = nodeID;
      this.nodeRoot = nodeID;
      this.level = level;
      this.isTreeBalanced = isTreeBalanced;
      leafNodeOffset = numLeaves;
      this.innerNodes = innerNodes;
      this.leafNodes = leafNodes;
      this.minPackedValue = minPackedValue.clone();
      this.maxPackedValue = maxPackedValue.clone();
      // stack arrays that keep information at different levels
      int treeDepth = getTreeDepth(numLeaves);// 获取树的深度
      splitDimValueStack = new byte[treeDepth][];
      splitValuesStack = new byte[treeDepth][];
      splitValuesStack[0] = new byte[config.packedIndexBytesLength()];
      leafBlockFPStack = new long[treeDepth + 1];
      readNodeDataPositions = new int[treeDepth + 1];
      rightNodePositions = new int[treeDepth];
      splitDimsPos = new int[treeDepth];
      negativeDeltas = new boolean[config.numIndexDims() * treeDepth];// 树的每个高度都有一个
      // information about the unbalance of the tree so we can report the exact size below a node
      this.pointCount = pointCount;
      rightMostLeafNode = (1 << treeDepth - 1) - 1;
      int lastLeafNodePointCount = Math.toIntExact(pointCount % config.maxPointsInLeafNode());
      this.lastLeafNodePointCount =
          lastLeafNodePointCount == 0 ? config.maxPointsInLeafNode() : lastLeafNodePointCount;
      // scratch objects, reused between clones so NN search are not creating those objects
      // in every clone.
      this.scratchIterator = scratchIterator;
      this.commonPrefixLengths = commonPrefixLengths;
      this.scratchDataPackedValue = scratchDataPackedValue;
      this.scratchMinIndexPackedValue = scratchMinIndexPackedValue;
      this.scratchMaxIndexPackedValue = scratchMaxIndexPackedValue;
      this.docIdsWriter = scratchIterator.docIdsWriter;
    }

    @Override
    public PointTree clone() {
      BKDPointTree index =
          new BKDPointTree(
              innerNodes.clone(),
              leafNodes.clone(),
              config,
              leafNodeOffset,
              version,
              pointCount,
              nodeID,
              level,
              minPackedValue,
              maxPackedValue,
              scratchIterator,
              scratchDataPackedValue,
              scratchMinIndexPackedValue,
              scratchMaxIndexPackedValue,
              commonPrefixLengths,
              isTreeBalanced);
      index.leafBlockFPStack[index.level] = leafBlockFPStack[level];
      if (isLeafNode() == false) {
        // copy node data
        index.rightNodePositions[index.level] = rightNodePositions[level];
        index.readNodeDataPositions[index.level] = readNodeDataPositions[level];
        index.splitValuesStack[index.level] = splitValuesStack[level].clone();
        System.arraycopy(
            negativeDeltas,
            level * config.numIndexDims(),
            index.negativeDeltas,
            level * config.numIndexDims(),
            config.numIndexDims());
        index.splitDimsPos[level] = splitDimsPos[level];
      }
      return index;
    }

    @Override
    public byte[] getMinPackedValue() {
      return minPackedValue;
    }

    @Override
    public byte[] getMaxPackedValue() {
      return maxPackedValue;
    }

    @Override
    public boolean moveToChild() throws IOException {
      if (isLeafNode()) {
        return false;
      }
      resetNodeDataPosition();
      pushBoundsLeft();
      pushLeft();
      return true;
    }

    private void resetNodeDataPosition() throws IOException {
      // move position of the inner nodes index to visit the first child
      assert readNodeDataPositions[level] <= innerNodes.getFilePointer();
      innerNodes.seek(readNodeDataPositions[level]);
    }
    //indexTree向左移动一步
    private void pushBoundsLeft() {
      final int splitDimPos = splitDimsPos[level];
      if (splitDimValueStack[level] == null) {
        splitDimValueStack[level] = new byte[config.bytesPerDim()];
      }
      // save the dimension we are going to change
      System.arraycopy(
          maxPackedValue, splitDimPos, splitDimValueStack[level], 0, config.bytesPerDim());
      assert ArrayUtil.getUnsignedComparator(config.bytesPerDim())
                  .compare(maxPackedValue, splitDimPos, splitValuesStack[level], splitDimPos)
              >= 0
          : "config.bytesPerDim()="
              + config.bytesPerDim()
              + " splitDimPos="
              + splitDimsPos[level]
              + " config.numIndexDims()="
              + config.numIndexDims()
              + " config.numDims()="
              + config.numDims();
      // add the split dim value:
      System.arraycopy(
          splitValuesStack[level], splitDimPos, maxPackedValue, splitDimPos, config.bytesPerDim());
    }

    private void pushLeft() throws IOException {
      nodeID *= 2;
      level++;
      readNodeData(true);
    }

    private void pushBoundsRight() {
      final int splitDimPos = splitDimsPos[level];
      // we should have already visited the left node
      assert splitDimValueStack[level] != null;
      // save the dimension we are going to change
      System.arraycopy(
          minPackedValue, splitDimPos, splitDimValueStack[level], 0, config.bytesPerDim());
      assert ArrayUtil.getUnsignedComparator(config.bytesPerDim())
                  .compare(minPackedValue, splitDimPos, splitValuesStack[level], splitDimPos)
              <= 0
          : "config.bytesPerDim()="
              + config.bytesPerDim()
              + " splitDimPos="
              + splitDimsPos[level]
              + " config.numIndexDims()="
              + config.numIndexDims()
              + " config.numDims()="
              + config.numDims();
      // add the split dim value:
      System.arraycopy(
          splitValuesStack[level], splitDimPos, minPackedValue, splitDimPos, config.bytesPerDim());
    }

    private void pushRight() throws IOException {
      final int nodePosition = rightNodePositions[level];
      assert nodePosition >= innerNodes.getFilePointer()
          : "nodePosition = " + nodePosition + " < currentPosition=" + innerNodes.getFilePointer();
      innerNodes.seek(nodePosition);
      nodeID = 2 * nodeID + 1;
      level++;
      readNodeData(false);
    }

    @Override
    public boolean moveToSibling() throws IOException {
      if (isLeftNode() == false || isRootNode()) {
        return false;
      }
      pop();
      popBounds(maxPackedValue);
      pushBoundsRight();
      pushRight();
      assert nodeExists();
      return true;
    }

    private void pop() {
      nodeID /= 2;
      level--;
    }

    private void popBounds(byte[] packedValue) {
      // restore the split dimension
      System.arraycopy(
          splitDimValueStack[level], 0, packedValue, splitDimsPos[level], config.bytesPerDim());
    }

    @Override
    public boolean moveToParent() {
      if (isRootNode()) {
        return false;
      }
      final byte[] packedValue = isLeftNode() ? maxPackedValue : minPackedValue;
      pop();
      popBounds(packedValue);
      return true;
    }

    private boolean isRootNode() {
      return nodeID == nodeRoot;
    }

    private boolean isLeftNode() {
      return (nodeID & 1) == 0;
    }

    private boolean isLeafNode() {
      return nodeID >= leafNodeOffset;
    }

    private boolean nodeExists() {
      return nodeID - leafNodeOffset < leafNodeOffset;
    }

    /** Only valid after pushLeft or pushRight, not pop! */
    private long getLeafBlockFP() {
      assert isLeafNode() : "nodeID=" + nodeID + " is not a leaf";
      return leafBlockFPStack[level];
    }
    // 计算多少叶子节点在该节点之下
    @Override
    public long size() {
      int leftMostLeafNode = nodeID;// 计算出该节点最左边的那个叶子编号
      while (leftMostLeafNode < leafNodeOffset) {
        leftMostLeafNode = leftMostLeafNode * 2;
      }
      int rightMostLeafNode = nodeID;// 计算左右边的那个叶子编号
      while (rightMostLeafNode < leafNodeOffset) {
        rightMostLeafNode = rightMostLeafNode * 2 + 1;
      }
      final int numLeaves;
      if (rightMostLeafNode >= leftMostLeafNode) { // 同一级别
        // both are on the same level
        numLeaves = rightMostLeafNode - leftMostLeafNode + 1;
      } else { // 不同级别
        // left is one level deeper than right
        numLeaves = rightMostLeafNode - leftMostLeafNode + 1 + leafNodeOffset;
      }// getNumLeavesSlow为了校验
      assert numLeaves == getNumLeavesSlow(nodeID) : numLeaves + " " + getNumLeavesSlow(nodeID);
      if (isTreeBalanced) {
        // before lucene 8.6, trees might have been constructed as fully balanced trees.
        return sizeFromBalancedTree(leftMostLeafNode, rightMostLeafNode);
      }
      // size for an unbalanced tree.
      return rightMostLeafNode == this.rightMostLeafNode
          ? (long) (numLeaves - 1) * config.maxPointsInLeafNode() + lastLeafNodePointCount
          : (long) numLeaves * config.maxPointsInLeafNode();
    }

    private long sizeFromBalancedTree(int leftMostLeafNode, int rightMostLeafNode) {
      // number of points that need to be distributed between leaves, one per leaf
      final int extraPoints =
          Math.toIntExact(((long) config.maxPointsInLeafNode() * this.leafNodeOffset) - pointCount);
      assert extraPoints < leafNodeOffset : "point excess should be lower than leafNodeOffset";
      // offset where we stop adding one point to the leaves
      final int nodeOffset = leafNodeOffset - extraPoints;
      long count = 0;
      for (int node = leftMostLeafNode; node <= rightMostLeafNode; node++) {
        // offsetPosition provides which extra point will be added to this node
        if (balanceTreeNodePosition(0, leafNodeOffset, node - leafNodeOffset, 0, 0) < nodeOffset) {
          count += config.maxPointsInLeafNode();
        } else {
          count += config.maxPointsInLeafNode() - 1;
        }
      }
      return count;
    }

    private int balanceTreeNodePosition(
        int minNode, int maxNode, int node, int position, int level) {
      if (maxNode - minNode == 1) {
        return position;
      }
      final int mid = (minNode + maxNode + 1) >>> 1;
      if (mid > node) {
        return balanceTreeNodePosition(minNode, mid, node, position, level + 1);
      } else {
        return balanceTreeNodePosition(mid, maxNode, node, position + (1 << level), level + 1);
      }
    }
    // 读取该叶子全量的docIds
    @Override
    public void visitDocIDs(PointValues.IntersectVisitor visitor) throws IOException {
      resetNodeDataPosition();
      addAll(visitor, false);
    }

    public void addAll(PointValues.IntersectVisitor visitor, boolean grown) throws IOException {
      if (grown == false) {
        final long size = size();
        if (size <= Integer.MAX_VALUE) {
          visitor.grow((int) size);
          grown = true;
        }
      }
      if (isLeafNode()) {
        // Leaf node
        leafNodes.seek(getLeafBlockFP());
        // How many points are stored in this leaf cell:
        int count = leafNodes.readVInt();
        // No need to call grow(), it has been called up-front
        // Borrow scratchIterator.docIds as decoding buffer
        docIdsWriter.readInts(leafNodes, count, visitor, scratchIterator.docIDs);
      } else {// 非叶子节点
        pushLeft();
        addAll(visitor, grown);
        pop();
        pushRight();
        addAll(visitor, grown);
        pop();
      }
    }

    @Override
    public void visitDocValues(PointValues.IntersectVisitor visitor) throws IOException {
      resetNodeDataPosition();
      visitLeavesOneByOne(visitor);
    }

    private void visitLeavesOneByOne(PointValues.IntersectVisitor visitor) throws IOException {
      if (isLeafNode()) {
        // Leaf node
        visitDocValues(visitor, getLeafBlockFP());
      } else {
        pushLeft();
        visitLeavesOneByOne(visitor);
        pop();
        pushRight();
        visitLeavesOneByOne(visitor);
        pop();
      }
    }
    // 会读取每个point的value,找到符合范围的docId
    private void visitDocValues(PointValues.IntersectVisitor visitor, long fp) throws IOException {
      // Leaf node; scan and filter all points in this block:
      int count = readDocIDs(leafNodes, fp, scratchIterator);
      if (version >= BKDWriter.VERSION_LOW_CARDINALITY_LEAVES) {// 默认跑到这里，会读取每个point的value,找到符合范围的docId
        visitDocValuesWithCardinality(
            commonPrefixLengths,
            scratchDataPackedValue,
            scratchMinIndexPackedValue,
            scratchMaxIndexPackedValue,
            leafNodes,
            scratchIterator,
            count,
            visitor);
      } else {
        visitDocValuesNoCardinality(
            commonPrefixLengths,
            scratchDataPackedValue,
            scratchMinIndexPackedValue,
            scratchMaxIndexPackedValue,
            leafNodes,
            scratchIterator,
            count,
            visitor);
      }
    }
    // 读取叶子节点上的文档个数,参考 BKDWriter.build()中叶子存储过程：writeLeafBlockDocs 函数,先存储的docCount
    private int readDocIDs(IndexInput in, long blockFP, BKDReaderDocIDSetIterator iterator)
        throws IOException {
      in.seek(blockFP);// 跑到KDD文件中的某个叶子节点上了
      // How many points are stored in this leaf cell:
      int count = in.readVInt();
      //读取这个叶子上所有文档id，将文档id存在iterator.docIDs中
      docIdsWriter.readInts(in, count, iterator.docIDs);

      return count;
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
    private void readNodeData(boolean isLeft) throws IOException {
      leafBlockFPStack[level] = leafBlockFPStack[level - 1];
      if (isLeft == false) {// 右边的话
        // read leaf block FP delta
        leafBlockFPStack[level] += innerNodes.readVLong();// 先从kdi文件中开始读取的，读取kdd中存放的第一个叶子起始位置。存放的delta
      }

      if (isLeafNode() == false) {// 若是叶子节点，则本次读取完成
        System.arraycopy(
            negativeDeltas,
            (level - 1) * config.numIndexDims(),
            negativeDeltas,
            level * config.numIndexDims(),
            config.numIndexDims());
        negativeDeltas[
                level * config.numIndexDims() + (splitDimsPos[level - 1] / config.bytesPerDim())] =
            isLeft;

        if (splitValuesStack[level] == null) {// 临时变量
          splitValuesStack[level] = splitValuesStack[level - 1].clone();
        } else {// 见BKDWriter中9xx行编码过程
          System.arraycopy(// 把上一个level所有维度都拿过来覆盖掉，而不是只选择一个维度复原。
              splitValuesStack[level - 1],
              0,
              splitValuesStack[level],
              0,
              config.packedIndexBytesLength());
        }

        // read split dim, prefix, firstDiffByteDelta encoded as int:
        int code = innerNodes.readVInt();
        final int splitDim = code % config.numIndexDims();
        splitDimsPos[level] = splitDim * config.bytesPerDim();
        code /= config.numIndexDims();
        final int prefix = code % (1 + config.bytesPerDim());// 前缀
        final int suffix = config.bytesPerDim() - prefix;// 后半段

        if (suffix > 0) {
          int firstDiffByteDelta = code / (1 + config.bytesPerDim());
          if (negativeDeltas[level * config.numIndexDims() + splitDim]) {// 本节点是左边节点的话
            firstDiffByteDelta = -firstDiffByteDelta;
          }
          final int startPos = splitDimsPos[level] + prefix;
          final int oldByte = splitValuesStack[level][startPos] & 0xFF;// 读取上一个节点不一样原始值
          splitValuesStack[level][startPos] = (byte) (oldByte + firstDiffByteDelta);// 修改这次切分值和上次切分值得差值部分
          innerNodes.readBytes(splitValuesStack[level], startPos + 1, suffix - 1);// 读取这次切分的不相同的部分
        } else {
          // our split value is == last split value in this dim, which can happen when there are
          // many duplicate values
        }

        final int leftNumBytes;
        if (nodeID * 2 < leafNodeOffset) {//提前判断了左子节点是不是叶子节点。
          leftNumBytes = innerNodes.readVInt();// 读取左子树的存储空间，等于BKDWriter.recursePackIndex()中返回值中的bytes2.length
        } else {
          leftNumBytes = 0;
        }// 比较妙，就是右子树在kdd存储的起始位置
        rightNodePositions[level] = Math.toIntExact(innerNodes.getFilePointer()) + leftNumBytes;
        readNodeDataPositions[level] = Math.toIntExact(innerNodes.getFilePointer());
      }
    }
// 从1层开始算起的话，若完全二叉树的话，第h层，可以有2^(h-1)个叶子节点.若不是完全二叉树的话，第h最少有2^(h-2)个叶子节点)，则2^(h-1)>=x>=2^(h-2)
    private int getTreeDepth(int numLeaves) {
      // First +1 because all the non-leave nodes makes another power
      // of 2; e.g. to have a fully balanced tree with 4 leaves you
      // need a depth=3 tree:

      // Second +1 because MathUtil.log computes floor of the logarithm; e.g.
      // with 5 leaves you need a depth=4 tree:
      return MathUtil.log(numLeaves, 2) + 2; // lnx+1<=x<lnx+2,因为lnx取值必须为整数，则舍弃了小数，则x=lnx+2
    }
// 获取每个point的原始的值
    private void visitDocValuesNoCardinality(
        int[] commonPrefixLengths,
        byte[] scratchDataPackedValue,
        byte[] scratchMinIndexPackedValue,
        byte[] scratchMaxIndexPackedValue,
        IndexInput in,
        BKDReaderDocIDSetIterator scratchIterator,
        int count,
        PointValues.IntersectVisitor visitor)
        throws IOException {
      readCommonPrefixes(commonPrefixLengths, scratchDataPackedValue, in);// 读取每个维度的前缀，保存在commonPrefixLengths中
      if (config.numIndexDims() != 1 && version >= BKDWriter.VERSION_LEAF_STORES_BOUNDS) { // 若读取-1，则说明所有元素相同
        byte[] minPackedValue = scratchMinIndexPackedValue;
        System.arraycopy(
            scratchDataPackedValue, 0, minPackedValue, 0, config.packedIndexBytesLength());
        byte[] maxPackedValue = scratchMaxIndexPackedValue;
        // Copy common prefixes before reading adjusted box
        System.arraycopy(minPackedValue, 0, maxPackedValue, 0, config.packedIndexBytesLength());// 最小最大值相同前缀
        readMinMax(commonPrefixLengths, minPackedValue, maxPackedValue, in);// 读最大最小值

        // The index gives us range of values for each dimension, but the actual range of values
        // might be much more narrow than what the index told us, so we double check the relation
        // here, which is cheap yet might help figure out that the block either entirely matches
        // or does not match at all. This is especially more likely in the case that there are
        // multiple dimensions that have correlation, ie. splitting on one dimension also
        // significantly changes the range of values in another dimension.
        PointValues.Relation r = visitor.compare(minPackedValue, maxPackedValue);
        if (r == PointValues.Relation.CELL_OUTSIDE_QUERY) {//无关,则不用继续了
          return;
        }
        visitor.grow(count);// 可以跑到ExitableDirectoryReader$ExitableIntersectVisitor

        if (r == PointValues.Relation.CELL_INSIDE_QUERY) {// 数据完全在查询范围之类
          scratchIntsRef.ints = scratchIterator.docIDs;
          scratchIntsRef.length = count;
          visitor.visit(scratchIntsRef);// 遍历每个数据
          return;
        }
      } else { // 只有一维的话，则读取也只是先定义下
        visitor.grow(count);
      }

      int compressedDim = readCompressedDim(in);
          // 稀疏矩阵
      if (compressedDim == -1) {//大部分一样，会读取每个文档的value进行读取，并缓存docId
        visitUniqueRawDocValues(scratchDataPackedValue, scratchIterator, count, visitor); //  会check value是否满足条件，并记录下docId编号
      } else {// 大部分不一样。会有原值范围比较
        visitCompressedDocValues(
            commonPrefixLengths,
            scratchDataPackedValue,
            in,
            scratchIterator,
            count,
            visitor,
            compressedDim);
      }
    }

    private void visitDocValuesWithCardinality(
        int[] commonPrefixLengths,
        byte[] scratchDataPackedValue,
        byte[] scratchMinIndexPackedValue,
        byte[] scratchMaxIndexPackedValue,
        IndexInput in,
        BKDReaderDocIDSetIterator scratchIterator,
        int count,
        PointValues.IntersectVisitor visitor)
        throws IOException {

      readCommonPrefixes(commonPrefixLengths, scratchDataPackedValue, in);
      int compressedDim = readCompressedDim(in);// 可以看下BKDWriter.writeLeafBlockPackedValues,若为-1，则相等
      if (compressedDim == -1) {// 所有value都是相同大小的
        // all values are the same
        visitor.grow(count);
        visitUniqueRawDocValues(scratchDataPackedValue, scratchIterator, count, visitor);
      } else {
        if (config.numIndexDims() != 1) {// 多维的
          byte[] minPackedValue = scratchMinIndexPackedValue;
          System.arraycopy(
              scratchDataPackedValue, 0, minPackedValue, 0, config.packedIndexBytesLength());
          byte[] maxPackedValue = scratchMaxIndexPackedValue;
          // Copy common prefixes before reading adjusted box
          System.arraycopy(minPackedValue, 0, maxPackedValue, 0, config.packedIndexBytesLength());
          readMinMax(commonPrefixLengths, minPackedValue, maxPackedValue, in);// 首先读取最大最小值

          // The index gives us range of values for each dimension, but the actual range of values
          // might be much more narrow than what the index told us, so we double check the relation
          // here, which is cheap yet might help figure out that the block either entirely matches
          // or does not match at all. This is especially more likely in the case that there are
          // multiple dimensions that have correlation, ie. splitting on one dimension also
          // significantly changes the range of values in another dimension.
          PointValues.Relation r = visitor.compare(minPackedValue, maxPackedValue);
          if (r == PointValues.Relation.CELL_OUTSIDE_QUERY) {
            return;
          }
          visitor.grow(count);

          if (r == PointValues.Relation.CELL_INSIDE_QUERY) {
            scratchIntsRef.ints = scratchIterator.docIDs;
            scratchIntsRef.length = count;
            visitor.visit(scratchIntsRef);
            return;
          }
        } else {
          visitor.grow(count);
        }

        if (compressedDim == -2) {// 代表低基数
          // low cardinality values  会check value是否满足条件，并记录下所有匹配的docId编号（这个成本有点高呀）
          visitSparseRawDocValues(
              commonPrefixLengths, scratchDataPackedValue, in, scratchIterator, count, visitor);
        } else {// 仅仅代表高基数，以哪个字段排序的
          // high cardinality
          visitCompressedDocValues(
              commonPrefixLengths,
              scratchDataPackedValue,
              in,
              scratchIterator,
              count,
              visitor,
              compressedDim);
        }
      }
    }

    private void readMinMax(
        int[] commonPrefixLengths, byte[] minPackedValue, byte[] maxPackedValue, IndexInput in)
        throws IOException {
      for (int dim = 0; dim < config.numIndexDims(); dim++) {
        int prefix = commonPrefixLengths[dim];
        in.readBytes(
            minPackedValue, dim * config.bytesPerDim() + prefix, config.bytesPerDim() - prefix);
        in.readBytes(
            maxPackedValue, dim * config.bytesPerDim() + prefix, config.bytesPerDim() - prefix);
      }
    }
    // 大部分相同，会读取每个docId的value，并进行比较，将匹配的docId给暂存起来
    // read cardinality and point可看 BKDWriter.writeLowCardinalityLeafBlockPackedValues()
    private void visitSparseRawDocValues(
        int[] commonPrefixLengths,
        byte[] scratchPackedValue,
        IndexInput in,
        BKDReaderDocIDSetIterator scratchIterator,
        int count,
        PointValues.IntersectVisitor visitor)
        throws IOException {
      int i;
      for (i = 0; i < count; ) { // 遍历这个叶子每个文档对应的pint
        int length = in.readVInt();// 读取相同元素的个数（一个叶子可以分多次读取）
        for (int dim = 0; dim < config.numDims(); dim++) {
          int prefix = commonPrefixLengths[dim];
          in.readBytes(// 将后缀值读取出来
              scratchPackedValue,
              dim * config.bytesPerDim() + prefix,
              config.bytesPerDim() - prefix);
        }// scratchPackedValue放的读取出来的文档id的value
        scratchIterator.reset(i, length);// BKDReader$BKDReaderDocIDSetIterator， 制定需要读取的docId起始位置，及docId长度
        visitor.visit(scratchIterator, scratchPackedValue);// 会去匹配当前这个词，会跑到 ExitableDirectoryReader$ExitableIntersectVisitor
        i += length;
      }
      if (i != count) {
        throw new CorruptIndexException(
            "Sub blocks do not add up to the expected count: " + count + " != " + i, in);
      }
    }

    // point is under commonPrefix
    private void visitUniqueRawDocValues(
        byte[] scratchPackedValue,
        BKDReaderDocIDSetIterator scratchIterator,
        int count,
        PointValues.IntersectVisitor visitor)
        throws IOException {
      scratchIterator.reset(0, count);
      visitor.visit(scratchIterator, scratchPackedValue);
    }
    // 大部分不同，可看 BKDWriter.writeHighCardinalityLeafBlockPackedValues
    private void visitCompressedDocValues(
        int[] commonPrefixLengths,
        byte[] scratchPackedValue,
        IndexInput in,
        BKDReaderDocIDSetIterator scratchIterator,
        int count,
        PointValues.IntersectVisitor visitor,
        int compressedDim)
        throws IOException {
      // the byte at `compressedByteOffset` is compressed using run-length compression,
      // other suffix bytes are stored verbatim
      final int compressedByteOffset =
          compressedDim * config.bytesPerDim() + commonPrefixLengths[compressedDim];
      commonPrefixLengths[compressedDim]++;
      int i;
      for (i = 0; i < count; ) {
        scratchPackedValue[compressedByteOffset] = in.readByte(); // 不相同的第一位
        final int runLen = Byte.toUnsignedInt(in.readByte());// 不相同的第一位个数
        for (int j = 0; j < runLen; ++j) {// 读取剩余每位的value
          for (int dim = 0; dim < config.numDims(); dim++) {// 读取每个具体的value,每个value都是不相通的
            int prefix = commonPrefixLengths[dim];
            in.readBytes(//读取原始值
                scratchPackedValue,
                dim * config.bytesPerDim() + prefix,
                config.bytesPerDim() - prefix);
          }//读取一个具体的point值，和查询范围作对比，符合的话，会将docId存放在visitor中
          visitor.visit(scratchIterator.docIDs[i + j], scratchPackedValue);// 会去匹配当前这个词，会跑到ExitableDirectoryReader$ExitableIntersectVisitor
        }
        i += runLen;
      }
      if (i != count) {
        throw new CorruptIndexException(
            "Sub blocks do not add up to the expected count: " + count + " != " + i, in);
      }
    }
    // 相同前缀
    private int readCompressedDim(IndexInput in) throws IOException {
      int compressedDim = in.readByte();
      if (compressedDim < -2
          || compressedDim >= config.numDims()
          || (version < BKDWriter.VERSION_LOW_CARDINALITY_LEAVES && compressedDim == -2)) {
        throw new CorruptIndexException("Got compressedDim=" + compressedDim, in);
      }
      return compressedDim;
    }
    // 在BKDWriter.writeCommonPrefixes()有写入过程
    private void readCommonPrefixes(
        int[] commonPrefixLengths, byte[] scratchPackedValue, IndexInput in) throws IOException {
      for (int dim = 0; dim < config.numDims(); dim++) {
        int prefix = in.readVInt();
        commonPrefixLengths[dim] = prefix;
        if (prefix > 0) {
          in.readBytes(scratchPackedValue, dim * config.bytesPerDim(), prefix);
        }
        // System.out.println("R: " + dim + " of " + numDims + " prefix=" + prefix);
      }
    }

    @Override
    public String toString() {
      return "nodeID=" + nodeID;
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
  public int getNumDimensions() throws IOException {
    return config.numDims();
  }

  @Override
  public int getNumIndexDimensions() throws IOException {
    return config.numIndexDims();
  }

  @Override
  public int getBytesPerDimension() throws IOException {
    return config.bytesPerDim();
  }

  @Override
  public long size() {
    return pointCount;
  }

  @Override
  public int getDocCount() {
    return docCount; //
  }

  /** Reusable {@link DocIdSetIterator} to handle low cardinality leaves. */
  private static class BKDReaderDocIDSetIterator extends AbstractDocIdSetIterator {

    private int idx;
    private int length;// 这次可读取文档的长度
    private int offset; // 这次可读取文档的起始位置(每次读取一个区间)
    final int[] docIDs;//读取的一个叶子节点的所有DocId内容
    private final DocIdsWriter docIdsWriter;

    public BKDReaderDocIDSetIterator(int maxPointsInLeafNode, int version) {
      this.docIDs = new int[maxPointsInLeafNode];
      this.docIdsWriter = new DocIdsWriter(maxPointsInLeafNode, version);
    }

    private void reset(int offset, int length) {
      this.offset = offset;
      this.length = length;
      assert offset + length <= docIDs.length;
      this.doc = -1;
      this.idx = 0;
    }

    @Override
    public int nextDoc() throws IOException {
      if (idx == length) {// 这次读取的个数是有已达到
        doc = DocIdSetIterator.NO_MORE_DOCS;
      } else {
        doc = docIDs[offset + idx];
        idx++;
      }
      return doc;
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