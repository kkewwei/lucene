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
package org.apache.lucene.util.fst;

import static org.apache.lucene.util.fst.FST.ARCS_FOR_BINARY_SEARCH;
import static org.apache.lucene.util.fst.FST.ARCS_FOR_CONTINUOUS;
import static org.apache.lucene.util.fst.FST.ARCS_FOR_DIRECT_ADDRESSING;
import static org.apache.lucene.util.fst.FST.BIT_ARC_HAS_FINAL_OUTPUT;
import static org.apache.lucene.util.fst.FST.BIT_ARC_HAS_OUTPUT;
import static org.apache.lucene.util.fst.FST.BIT_FINAL_ARC;
import static org.apache.lucene.util.fst.FST.BIT_LAST_ARC;
import static org.apache.lucene.util.fst.FST.BIT_STOP_NODE;
import static org.apache.lucene.util.fst.FST.BIT_TARGET_NEXT;
import static org.apache.lucene.util.fst.FST.FINAL_END_NODE;
import static org.apache.lucene.util.fst.FST.NON_FINAL_END_NODE;
import static org.apache.lucene.util.fst.FST.getNumPresenceBytes;

import java.io.IOException;
import java.util.Objects;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.FST.INPUT_TYPE; // javadoc

/**
 * Builds a minimal FST (maps an IntsRef term to an arbitrary output) from pre-sorted terms with
 * outputs. The FST becomes an FSA if you use NoOutputs. The FST is written on-the-fly into a
 * compact serialized format byte array, which can be saved to / loaded from a Directory or used
 * directly for traversal. The FST is always finite (no cycles).
 *
 * <p>NOTE: The algorithm is described at
 * http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.24.3698
 *
 * <p>The parameterized type T is the output type. See the subclasses of {@link Outputs}.
 *
 * <p>FSTs larger than 2.1GB are now possible (as of Lucene 4.2). FSTs containing more than 2.1B
 * nodes are also now possible, however they cannot be packed.
 *
 * <p>It now supports 3 different workflows:
 *
 * <p>- Build FST and use it immediately entirely in RAM and then discard it
 *
 * <p>- Build FST and use it immediately entirely in RAM and also save it to other DataOutput, and
 * load it later and use it
 *
 * <p>- Build FST but stream it immediately to disk (except the FSTMetaData, to be saved at the
 * end). In order to use it, you need to construct the corresponding DataInput and use the FST
 * constructor to read it.
 *
 * @lucene.experimental
 */
public class FSTCompiler<T> {

  static final float DIRECT_ADDRESSING_MAX_OVERSIZING_FACTOR = 1f;

  /**
   * @see #shouldExpandNodeWithFixedLengthArcs
   */
  static final int FIXED_LENGTH_ARC_SHALLOW_DEPTH = 3; // 0 => only root node.

  /**
   * @see #shouldExpandNodeWithFixedLengthArcs
   */
  static final int FIXED_LENGTH_ARC_SHALLOW_NUM_ARCS = 5;

  /**
   * @see #shouldExpandNodeWithFixedLengthArcs
   */
  static final int FIXED_LENGTH_ARC_DEEP_NUM_ARCS = 10;

  /**
   * Maximum oversizing factor allowed for direct addressing compared to binary search when
   * expansion credits allow the oversizing. This factor prevents expansions that are obviously too
   * costly even if there are sufficient credits.
   *
   * @see #shouldExpandNodeWithDirectAddressing
   */
  private static final float DIRECT_ADDRESSING_MAX_OVERSIZE_WITH_CREDIT_FACTOR = 1.66f;

  // a FSTReader used when a non-FSTReader DataOutput is configured.
  // it will throw exceptions if attempt to call getReverseBytesReader() or writeTo(DataOutput)
  private static final FSTReader NULL_FST_READER = new NullFSTReader();

  private final NodeHash<T> dedupHash;
  // a temporary FST used during building for NodeHash cache
  final FST<T> fst;
  private final T NO_OUTPUT;

  // private static final boolean DEBUG = true;

  private final IntsRefBuilder lastInput = new IntsRefBuilder();// 上一个相同的前缀

  // indicates whether we are not yet to write the padding byte
  private boolean paddingBytePending;

  // NOTE: cutting this over to ArrayList instead loses ~6%
  // in build performance on 9.8M Wikipedia terms; so we
  // left this as an array:
  // current "frontier"
  private UnCompiledNode<T>[] frontier; // 是个UnCompiledNode数组，主要是存公共前缀Node，Node的inputCount是指共享这个Node的term的数量(后文用Node的共享数代替)，根节点被所有的term共享。已知了公共前缀的长度，之后通过freezeTail将上个term的后缀Node全部写入到FST中或者删除掉。
  // 在FST的构造过程中，它维护整棵FST树，其中里面直接保存的是UnCompiledNode，是当前添加的字符串所形成的状态节点，而前面添加的字符串形成的状态节点通过指针相互引用。
  // Used for the BIT_TARGET_NEXT optimization (whereby
  // instead of storing the address of the target node for
  // a given arc, we mark a single bit noting that the next
  // node in the byte[] is the target node):
  long lastFrozenNode;// 最后冷冻的那个节点存储位置

  // Reused temporarily while building the FST:
  int[] numBytesPerArc = new int[4];
  int[] numLabelBytesPerArc = new int[numBytesPerArc.length];
  final FixedLengthArcsBuffer fixedLengthArcsBuffer = new FixedLengthArcsBuffer();

  long arcCount;// 该fst中总共边的个数
  long nodeCount;// 该fst中节点的个数
  long binarySearchNodeCount;
  long directAddressingNodeCount;
  long continuousNodeCount;

  final boolean allowFixedLengthArcs;// 默认为true
  final float directAddressingMaxOversizingFactor;
  final int version;
  long directAddressingExpansionCredit;

  // the DataOutput to stream the FST bytes to
  final DataOutput dataOutput; //每个arc序列化后，都堆积放这里：放的序列化的fst（节点是从结尾向开头存储的）
  // 临时变量，两个节点之间的所有边编码放这里
  // buffer to store bytes for the one node we are currently writing
  final GrowableByteArrayDataOutput scratchBytes = new GrowableByteArrayDataOutput();

  private long numBytesWritten;// fst byte写入的大小

  /**
   * Get an on-heap DataOutput that allows the FST to be read immediately after writing, and also
   * optionally saved to an external DataOutput.
   *
   * @param blockBits how many bits wide to make each block of the DataOutput
   * @return the DataOutput
   */
  public static DataOutput getOnHeapReaderWriter(int blockBits) {
    return new ReadWriteDataOutput(blockBits);
  }

  private FSTCompiler(
      FST.INPUT_TYPE inputType,
      double suffixRAMLimitMB,
      Outputs<T> outputs,
      boolean allowFixedLengthArcs,
      DataOutput dataOutput,
      float directAddressingMaxOversizingFactor,
      int version) {
    this.allowFixedLengthArcs = allowFixedLengthArcs;
    this.directAddressingMaxOversizingFactor = directAddressingMaxOversizingFactor;
    this.version = version;
    // pad: ensure no node gets address 0 which is reserved to mean
    // the stop state w/ no arcs. the actual byte will be written lazily
    numBytesWritten++; // 0作为保留stop状态
    paddingBytePending = true;
    this.dataOutput = dataOutput;
    fst =// inputType=INPUT_TYPE.BYTE1， 也是新创建的
        new FST<>(new FST.FSTMetadata<>(inputType, outputs, null, -1, version, 0), NULL_FST_READER);
    if (suffixRAMLimitMB < 0) {
      throw new IllegalArgumentException("ramLimitMB must be >= 0; got: " + suffixRAMLimitMB);
    } else if (suffixRAMLimitMB > 0) {
      dedupHash = new NodeHash<>(this, suffixRAMLimitMB);
    } else {
      dedupHash = null;
    }
    NO_OUTPUT = outputs.getNoOutput();

    @SuppressWarnings({"rawtypes", "unchecked"})
    final UnCompiledNode<T>[] f = (UnCompiledNode<T>[]) new UnCompiledNode[10];
    frontier = f;
    for (int idx = 0; idx < frontier.length; idx++) {
      frontier[idx] = new UnCompiledNode<>(this, idx);
    }
  }

  /**
   * This class is used for FST backed by non-FSTReader DataOutput. It does not allow getting the
   * reverse BytesReader nor writing to a DataOutput.
   */
  private static final class NullFSTReader implements FSTReader {

    @Override
    public long ramBytesUsed() {
      return 0;
    }

    @Override
    public FST.BytesReader getReverseBytesReader() {
      throw new UnsupportedOperationException(
          "FST was not constructed with getOnHeapReaderWriter()");
    }

    @Override
    public void writeTo(DataOutput out) {
      throw new UnsupportedOperationException(
          "FST was not constructed with getOnHeapReaderWriter()");
    }
  }

  /**
   * Get the respective {@link FSTReader} of the {@link DataOutput}. To call this method, you need
   * to use the default DataOutput or {@link #getOnHeapReaderWriter(int)}, otherwise we will throw
   * an exception.
   *
   * @return the DataOutput as FSTReader
   * @throws IllegalStateException if the DataOutput does not implement FSTReader
   */
  public FSTReader getFSTReader() {
    if (dataOutput instanceof FSTReader) {
      return (FSTReader) dataOutput;
    }
    throw new IllegalStateException(
        "The DataOutput must implement FSTReader, but got " + dataOutput);
  }

  /**
   * Fluent-style constructor for FST {@link FSTCompiler}.
   *
   * <p>Creates an FST/FSA builder with all the possible tuning and construction tweaks. Read
   * parameter documentation carefully.
   */
  public static class Builder<T> {

    private final INPUT_TYPE inputType;
    private final Outputs<T> outputs;
    private double suffixRAMLimitMB = 32.0;
    private boolean allowFixedLengthArcs = true;
    private DataOutput dataOutput;
    private float directAddressingMaxOversizingFactor = DIRECT_ADDRESSING_MAX_OVERSIZING_FACTOR;
    private int version = FST.VERSION_CURRENT;

    /**
     * @param inputType The input type (transition labels). Can be anything from {@link INPUT_TYPE}
     *     enumeration. Shorter types will consume less memory. Strings (character sequences) are
     *     represented as {@link INPUT_TYPE#BYTE4} (full unicode codepoints).
     * @param outputs The output type for each input sequence. Applies only if building an FST. For
     *     FSA, use {@link NoOutputs#getSingleton()} and {@link NoOutputs#getNoOutput()} as the
     *     singleton output object.
     */
    public Builder(FST.INPUT_TYPE inputType, Outputs<T> outputs) {
      this.inputType = inputType;
      this.outputs = outputs;
    }

    /**
     * The approximate maximum amount of RAM (in MB) to use holding the suffix cache, which enables
     * the FST to share common suffixes. Pass {@link Double#POSITIVE_INFINITY} to keep all suffixes
     * and create an exactly minimal FST. In this case, the amount of RAM actually used will be
     * bounded by the number of unique suffixes. If you pass a value smaller than the builder would
     * use, the least recently used suffixes will be discarded, thus reducing suffix sharing and
     * creating a non-minimal FST. In this case, the larger the limit, the closer the FST will be to
     * its true minimal size, with diminishing returns as you increase the limit. Pass {@code 0} to
     * disable suffix sharing entirely, but note that the resulting FST can be substantially larger
     * than the minimal FST.
     *
     * <p>Note that this is not a precise limit. The current implementation uses hash tables to map
     * the suffixes, and approximates the rough overhead (unused slots) in the hash table.
     *
     * <p>Default = {@code 32.0} MB.
     */
    public Builder<T> suffixRAMLimitMB(double mb) {
      if (mb < 0) {
        throw new IllegalArgumentException("suffixRAMLimitMB must be >= 0; got: " + mb);
      }
      this.suffixRAMLimitMB = mb;
      return this;
    }

    /**
     * Pass {@code false} to disable the fixed length arc optimization (binary search or direct
     * addressing) while building the FST; this will make the resulting FST smaller but slower to
     * traverse.
     *
     * <p>Default = {@code true}.
     */
    public Builder<T> allowFixedLengthArcs(boolean allowFixedLengthArcs) {
      this.allowFixedLengthArcs = allowFixedLengthArcs;
      return this;
    }

    /**
     * Set the {@link DataOutput} which is used for low-level writing of FST. If you want the FST to
     * be immediately readable, you need to use {@link FSTCompiler#getOnHeapReaderWriter(int)}.
     *
     * <p>Otherwise you need to construct the corresponding {@link
     * org.apache.lucene.store.DataInput} and use the FST constructor to read it.
     *
     * @param dataOutput the DataOutput
     * @return this builder
     * @see FSTCompiler#getOnHeapReaderWriter(int)
     */
    public Builder<T> dataOutput(DataOutput dataOutput) {
      this.dataOutput = Objects.requireNonNull(dataOutput, "DataOutput cannot be null");
      return this;
    }

    /**
     * Overrides the default the maximum oversizing of fixed array allowed to enable direct
     * addressing of arcs instead of binary search.
     *
     * <p>Setting this factor to a negative value (e.g. -1) effectively disables direct addressing,
     * only binary search nodes will be created.
     *
     * <p>This factor does not determine whether to encode a node with a list of variable length
     * arcs or with fixed length arcs. It only determines the effective encoding of a node that is
     * already known to be encoded with fixed length arcs.
     *
     * <p>Default = 1.
     */
    public Builder<T> directAddressingMaxOversizingFactor(float factor) {
      this.directAddressingMaxOversizingFactor = factor;
      return this;
    }

    /** Expert: Set the codec version. * */
    public Builder<T> setVersion(int version) {
      if (version < FST.VERSION_90 || version > FST.VERSION_CURRENT) {
        throw new IllegalArgumentException(
            "Expected version in range ["
                + FST.VERSION_90
                + ", "
                + FST.VERSION_CURRENT
                + "], got "
                + version);
      }
      this.version = version;
      return this;
    }

    /** Creates a new {@link FSTCompiler}. */
    public FSTCompiler<T> build() {
      // create a default DataOutput if not specified
      if (dataOutput == null) {
        dataOutput = getOnHeapReaderWriter(15);
      }
      return new FSTCompiler<>(
          inputType,
          suffixRAMLimitMB,
          outputs,
          allowFixedLengthArcs,
          dataOutput,
          directAddressingMaxOversizingFactor,
          version);
    }
  }

  public float getDirectAddressingMaxOversizingFactor() {
    return directAddressingMaxOversizingFactor;
  }

  public long getNodeCount() {
    // 1+ in order to count the -1 implicit final node
    return 1 + nodeCount;
  }

  public long getArcCount() {
    return arcCount;
  }
  // compileNode可能返回的是一个已经写入 FST的 Node的 ID，这样就达到了共享 Node 的目的。tailLength是可能共享的长度
  private CompiledNode compileNode(UnCompiledNode<T> nodeIn) throws IOException {
    final long node;
    long bytesPosStart = numBytesWritten;// 当前已经写入的起始位置
    if (dedupHash != null) {// 一直为null
      if (nodeIn.numArcs == 0) {// 说明该节点是结尾。
        node = addNode(nodeIn);// Node的Arc数量为0，fst.addNode 会返回 -1。已经将nodeId的value存放了fst中
        lastFrozenNode = node;// 每次重新输入一条边，又开始置位-1
      } else {// root节点也会进来
        node = dedupHash.add(nodeIn);// 这个就是node，是在fst中的存储位置
      }
    } else {
      node = addNode(nodeIn);// 把node写入fst中。有几个边就编译几个边
    }

    assert node != -2;

    long bytesPosEnd = numBytesWritten; // 看看是否有写入
    if (bytesPosEnd != bytesPosStart) { // fst.bytes中有写入
      // The FST added a new node:
      assert bytesPosEnd > bytesPosStart;
      lastFrozenNode = node;
    }

    nodeIn.clear();// 这里将这个nodeIn给清空了，意味着该边被compile了。后面还会继续复用

    final CompiledNode fn = new CompiledNode();
    fn.node = node;
    return fn;// 返回一个CompiledNode
  }

  // serializes new node by appending its bytes to the end
  // of the current byte[]
  long addNode(FSTCompiler.UnCompiledNode<T> nodeIn) throws IOException {
    // System.out.println("FST.addNode pos=" + bytes.getPosition() + " numArcs=" + nodeIn.numArcs);
    if (nodeIn.numArcs == 0) {
      if (nodeIn.isFinal) {// 的确是个final节点
        return FINAL_END_NODE;
      } else {
        return NON_FINAL_END_NODE;
      }
    }
    // reset the scratch writer to prepare for new write
    scratchBytes.setPosition(0);// 重制写入

    final boolean doFixedLengthArcs = shouldExpandNodeWithFixedLengthArcs(nodeIn);
    if (doFixedLengthArcs) {
      // System.out.println("  fixed length arcs");
      if (numBytesPerArc.length < nodeIn.numArcs) {
        numBytesPerArc = new int[ArrayUtil.oversize(nodeIn.numArcs, Integer.BYTES)];
        numLabelBytesPerArc = new int[numBytesPerArc.length];
      }
    }

    arcCount += nodeIn.numArcs;

    final int lastArc = nodeIn.numArcs - 1;

    long lastArcStart = 0;
    int maxBytesPerArc = 0;
    int maxBytesPerArcWithoutLabel = 0;
    for (int arcIdx = 0; arcIdx < nodeIn.numArcs; arcIdx++) {//遍历每一条边
      final FSTCompiler.Arc<T> arc = nodeIn.arcs[arcIdx];
      final FSTCompiler.CompiledNode target = (FSTCompiler.CompiledNode) arc.target;
      int flags = 0;
      // System.out.println("  arc " + arcIdx + " label=" + arc.label + " -> target=" +
      // target.node);

      if (arcIdx == lastArc) {
        flags += BIT_LAST_ARC;//是否这个节点最后一条边
      }

      if (lastFrozenNode == target.node && !doFixedLengthArcs) {//
        // TODO: for better perf (but more RAM used) we
        // could avoid this except when arc is "near" the
        // last arc:
        flags += BIT_TARGET_NEXT;
      }

      if (arc.isFinal) {//例如:ab,ac,ad。那么b,c,d都是最后一条边
        flags += BIT_FINAL_ARC;
        if (arc.nextFinalOutput != NO_OUTPUT) {
          flags += BIT_ARC_HAS_FINAL_OUTPUT;
        }
      } else {
        assert arc.nextFinalOutput == NO_OUTPUT;
      }

      boolean targetHasArcs = target.node > 0;

      if (!targetHasArcs) {// 比如本term对应了一批blocks
        flags += BIT_STOP_NODE;
      }

      if (arc.output != NO_OUTPUT) {
        flags += BIT_ARC_HAS_OUTPUT;
      }

      scratchBytes.writeByte((byte) flags);//首先写入标识位
      long labelStart = scratchBytes.getPosition();
      writeLabel(scratchBytes, arc.label);// 其次写入label
      int numLabelBytes = (int) (scratchBytes.getPosition() - labelStart);// label占用空间

      // System.out.println("  write arc: label=" + (char) arc.label + " flags=" + flags + "
      // target=" + target.node + " pos=" + bytes.getPosition() + " output=" +
      // outputs.outputToString(arc.output));

      if (arc.output != NO_OUTPUT) {
        fst.outputs.write(arc.output, scratchBytes);//往scratchBytes
        // System.out.println("    write output");
      }

      if (arc.nextFinalOutput != NO_OUTPUT) {
        // System.out.println("    write final output");
        fst.outputs.writeFinalOutput(arc.nextFinalOutput, scratchBytes);
      }

      if (targetHasArcs && (flags & BIT_TARGET_NEXT) == 0) {
        assert target.node > 0;
        // System.out.println("    write target");
        scratchBytes.writeVLong(target.node);// 只有最后一个边，才不需要写入target.node
      }

      // just write the arcs "like normal" on first pass, but record how many bytes each one took
      // and max byte size:
      if (doFixedLengthArcs) {
        int numArcBytes = (int) (scratchBytes.getPosition() - lastArcStart);
        numBytesPerArc[arcIdx] = numArcBytes;
        numLabelBytesPerArc[arcIdx] = numLabelBytes;
        lastArcStart = scratchBytes.getPosition();
        maxBytesPerArc = Math.max(maxBytesPerArc, numArcBytes);
        maxBytesPerArcWithoutLabel =
            Math.max(maxBytesPerArcWithoutLabel, numArcBytes - numLabelBytes);
        // System.out.println("    arcBytes=" + numArcBytes + " labelBytes=" + numLabelBytes);
      }
    }

    // TODO: try to avoid wasteful cases: disable doFixedLengthArcs in that case
    /*
     *
     * LUCENE-4682: what is a fair heuristic here?
     * It could involve some of these:
     * 1. how "busy" the node is: nodeIn.inputCount relative to frontier[0].inputCount?
     * 2. how much binSearch saves over scan: nodeIn.numArcs
     * 3. waste: numBytes vs numBytesExpanded
     *
     * the one below just looks at #3
    if (doFixedLengthArcs) {
      // rough heuristic: make this 1.25 "waste factor" a parameter to the phd ctor????
      int numBytes = lastArcStart - startAddress;
      int numBytesExpanded = maxBytesPerArc * nodeIn.numArcs;
      if (numBytesExpanded > numBytes*1.25) {
        doFixedLengthArcs = false;
      }
    }
    */

    if (doFixedLengthArcs) {
      assert maxBytesPerArc > 0;
      // 2nd pass just "expands" all arcs to take up a fixed byte size

      int labelRange = nodeIn.arcs[nodeIn.numArcs - 1].label - nodeIn.arcs[0].label + 1;
      assert labelRange > 0;
      boolean continuousLabel = labelRange == nodeIn.numArcs;
      if (continuousLabel && version >= FST.VERSION_CONTINUOUS_ARCS) {
        writeNodeForDirectAddressingOrContinuous(
            nodeIn, maxBytesPerArcWithoutLabel, labelRange, true);
        continuousNodeCount++;
      } else if (shouldExpandNodeWithDirectAddressing(
          nodeIn, maxBytesPerArc, maxBytesPerArcWithoutLabel, labelRange)) {
        writeNodeForDirectAddressingOrContinuous(
            nodeIn, maxBytesPerArcWithoutLabel, labelRange, false);
        directAddressingNodeCount++;
      } else {
        writeNodeForBinarySearch(nodeIn, maxBytesPerArc);
        binarySearchNodeCount++;
      }
    }
    // 仅仅
    reverseScratchBytes();//倒过来后是：target.node:label:flags （实际读取的时候又是倒叙读取，从flags开始读取， 可参考FST.findTargetArc和OffHeapFSTStore）
    // write the padding byte if needed
    if (paddingBytePending) {
      writePaddingByte();
    }
    scratchBytes.writeTo(dataOutput);// 再转移下结果
    numBytesWritten += scratchBytes.getPosition();

    nodeCount++;
    return numBytesWritten - 1;// 返回的是存放在dataOutput中的endoffset信息
  }

  /**
   * Write the padding byte, ensure no node gets address 0 which is reserved to mean the stop state
   * w/ no arcs
   */
  private void writePaddingByte() throws IOException {
    assert paddingBytePending;
    dataOutput.writeByte((byte) 0);
    paddingBytePending = false;
  }

  private void writeLabel(DataOutput out, int v) throws IOException {
    assert v >= 0 : "v=" + v;
    if (fst.metadata.inputType == INPUT_TYPE.BYTE1) {
      assert v <= 255 : "v=" + v;
      out.writeByte((byte) v);
    } else if (fst.metadata.inputType == INPUT_TYPE.BYTE2) {
      assert v <= 65535 : "v=" + v;
      out.writeShort((short) v);
    } else {
      out.writeVInt(v);
    }
  }

  /**
   * Returns whether the given node should be expanded with fixed length arcs. Nodes will be
   * expanded depending on their depth (distance from the root node) and their number of arcs.
   *
   * <p>Nodes with fixed length arcs use more space, because they encode all arcs with a fixed
   * number of bytes, but they allow either binary search or direct addressing on the arcs (instead
   * of linear scan) on lookup by arc label.
   */
  private boolean shouldExpandNodeWithFixedLengthArcs(FSTCompiler.UnCompiledNode<T> node) {
    return allowFixedLengthArcs
        && ((node.depth <= FIXED_LENGTH_ARC_SHALLOW_DEPTH
                && node.numArcs >= FIXED_LENGTH_ARC_SHALLOW_NUM_ARCS)// 边大于5过
            || node.numArcs >= FIXED_LENGTH_ARC_DEEP_NUM_ARCS);// 边大于10
  }

  /**
   * Returns whether the given node should be expanded with direct addressing instead of binary
   * search.
   *
   * <p>Prefer direct addressing for performance if it does not oversize binary search byte size too
   * much, so that the arcs can be directly addressed by label.
   *
   * @see FSTCompiler#getDirectAddressingMaxOversizingFactor()
   */
  private boolean shouldExpandNodeWithDirectAddressing(
      FSTCompiler.UnCompiledNode<T> nodeIn,
      int numBytesPerArc,
      int maxBytesPerArcWithoutLabel,
      int labelRange) {
    // Anticipate precisely the size of the encodings.
    int sizeForBinarySearch = numBytesPerArc * nodeIn.numArcs;
    int sizeForDirectAddressing =
        getNumPresenceBytes(labelRange)
            + numLabelBytesPerArc[0]
            + maxBytesPerArcWithoutLabel * nodeIn.numArcs;

    // Determine the allowed oversize compared to binary search.
    // This is defined by a parameter of FST Builder (default 1: no oversize).
    int allowedOversize = (int) (sizeForBinarySearch * getDirectAddressingMaxOversizingFactor());
    int expansionCost = sizeForDirectAddressing - allowedOversize;

    // Select direct addressing if either:
    // - Direct addressing size is smaller than binary search.
    //   In this case, increment the credit by the reduced size (to use it later).
    // - Direct addressing size is larger than binary search, but the positive credit allows the
    // oversizing.
    //   In this case, decrement the credit by the oversize.
    // In addition, do not try to oversize to a clearly too large node size
    // (this is the DIRECT_ADDRESSING_MAX_OVERSIZE_WITH_CREDIT_FACTOR parameter).
    if (expansionCost <= 0
        || (directAddressingExpansionCredit >= expansionCost
            && sizeForDirectAddressing
                <= allowedOversize * DIRECT_ADDRESSING_MAX_OVERSIZE_WITH_CREDIT_FACTOR)) {
      directAddressingExpansionCredit -= expansionCost;
      return true;
    }
    return false;
  }

  private void writeNodeForBinarySearch(FSTCompiler.UnCompiledNode<T> nodeIn, int maxBytesPerArc) {
    // Build the header in a buffer.
    // It is a false/special arc which is in fact a node header with node flags followed by node
    // metadata.
    fixedLengthArcsBuffer
        .resetPosition()
        .writeByte(ARCS_FOR_BINARY_SEARCH)
        .writeVInt(nodeIn.numArcs)
        .writeVInt(maxBytesPerArc);
    int headerLen = fixedLengthArcsBuffer.getPosition();

    // Expand the arcs in place, backwards.
    int srcPos = scratchBytes.getPosition();
    int destPos = headerLen + nodeIn.numArcs * maxBytesPerArc;
    assert destPos >= srcPos;
    if (destPos > srcPos) {
      scratchBytes.setPosition(destPos);
      for (int arcIdx = nodeIn.numArcs - 1; arcIdx >= 0; arcIdx--) {
        destPos -= maxBytesPerArc;
        int arcLen = numBytesPerArc[arcIdx];
        srcPos -= arcLen;
        if (srcPos != destPos) {
          assert destPos > srcPos
              : "destPos="
                  + destPos
                  + " srcPos="
                  + srcPos
                  + " arcIdx="
                  + arcIdx
                  + " maxBytesPerArc="
                  + maxBytesPerArc
                  + " arcLen="
                  + arcLen
                  + " nodeIn.numArcs="
                  + nodeIn.numArcs;
          // copy the bytes from srcPos to destPos, essentially expanding the arc from variable
          // length to fixed length
          writeScratchBytes(destPos, scratchBytes.getBytes(), srcPos, arcLen);
        }
      }
    }

    // Finally write the header
    writeScratchBytes(0, fixedLengthArcsBuffer.getBytes(), 0, headerLen);
  }

  /**
   * Reverse the scratch bytes in place. This operation does not affect scratchBytes.getPosition().
   */
  private void reverseScratchBytes() {
    int pos = scratchBytes.getPosition();
    byte[] bytes = scratchBytes.getBytes();
    int limit = pos / 2;
    for (int i = 0; i < limit; i++) {
      byte b = bytes[i];
      bytes[i] = bytes[pos - 1 - i];
      bytes[pos - 1 - i] = b;
    }
  }

  /**
   * Write bytes from a source byte[] to the scratch bytes. The written bytes must fit within what
   * was already written in the scratch bytes.
   *
   * <p>This operation does not affect scratchBytes.getPosition().
   *
   * @param destPos the position in the scratch bytes
   * @param bytes the source byte[]
   * @param offset the offset inside the source byte[]
   * @param length the number of bytes to write
   */
  private void writeScratchBytes(int destPos, byte[] bytes, int offset, int length) {
    assert destPos + length <= scratchBytes.getPosition();
    System.arraycopy(bytes, offset, scratchBytes.getBytes(), destPos, length);
  }

  private void writeNodeForDirectAddressingOrContinuous(
      FSTCompiler.UnCompiledNode<T> nodeIn,
      int maxBytesPerArcWithoutLabel,
      int labelRange,
      boolean continuous) {
    // Expand the arcs backwards in a buffer because we remove the labels.
    // So the obtained arcs might occupy less space. This is the reason why this
    // whole method is more complex.
    // Drop the label bytes since we can infer the label based on the arc index,
    // the presence bits, and the first label. Keep the first label.
    int headerMaxLen = 11;
    int numPresenceBytes = continuous ? 0 : getNumPresenceBytes(labelRange);
    int srcPos = scratchBytes.getPosition();
    int totalArcBytes = numLabelBytesPerArc[0] + nodeIn.numArcs * maxBytesPerArcWithoutLabel;
    int bufferOffset = headerMaxLen + numPresenceBytes + totalArcBytes;
    byte[] buffer = fixedLengthArcsBuffer.ensureCapacity(bufferOffset).getBytes();
    // Copy the arcs to the buffer, dropping all labels except first one.
    for (int arcIdx = nodeIn.numArcs - 1; arcIdx >= 0; arcIdx--) {
      bufferOffset -= maxBytesPerArcWithoutLabel;
      int srcArcLen = numBytesPerArc[arcIdx];
      srcPos -= srcArcLen;
      int labelLen = numLabelBytesPerArc[arcIdx];
      // Copy the flags.
      scratchBytes.writeTo(srcPos, buffer, bufferOffset, 1);
      // Skip the label, copy the remaining.
      int remainingArcLen = srcArcLen - 1 - labelLen;
      if (remainingArcLen != 0) {
        scratchBytes.writeTo(srcPos + 1 + labelLen, buffer, bufferOffset + 1, remainingArcLen);
      }
      if (arcIdx == 0) {
        // Copy the label of the first arc only.
        bufferOffset -= labelLen;
        scratchBytes.writeTo(srcPos + 1, buffer, bufferOffset, labelLen);
      }
    }
    assert bufferOffset == headerMaxLen + numPresenceBytes;

    // Build the header in the buffer.
    // It is a false/special arc which is in fact a node header with node flags followed by node
    // metadata.
    fixedLengthArcsBuffer
        .resetPosition()
        .writeByte(continuous ? ARCS_FOR_CONTINUOUS : ARCS_FOR_DIRECT_ADDRESSING)
        .writeVInt(labelRange) // labelRange instead of numArcs.
        .writeVInt(
            maxBytesPerArcWithoutLabel); // maxBytesPerArcWithoutLabel instead of maxBytesPerArc.
    int headerLen = fixedLengthArcsBuffer.getPosition();

    // Write the header.
    scratchBytes.setPosition(0);
    scratchBytes.writeBytes(fixedLengthArcsBuffer.getBytes(), 0, headerLen);

    // Write the presence bits
    if (continuous == false) {
      writePresenceBits(nodeIn);
      assert scratchBytes.getPosition() == headerLen + numPresenceBytes;
    }

    // Write the first label and the arcs.
    scratchBytes.writeBytes(fixedLengthArcsBuffer.getBytes(), bufferOffset, totalArcBytes);
    assert scratchBytes.getPosition() == headerLen + numPresenceBytes + totalArcBytes;
  }

  private void writePresenceBits(FSTCompiler.UnCompiledNode<T> nodeIn) {
    byte presenceBits = 1; // The first arc is always present.
    int presenceIndex = 0;
    int previousLabel = nodeIn.arcs[0].label;
    for (int arcIdx = 1; arcIdx < nodeIn.numArcs; arcIdx++) {
      int label = nodeIn.arcs[arcIdx].label;
      assert label > previousLabel;
      presenceIndex += label - previousLabel;
      while (presenceIndex >= Byte.SIZE) {
        scratchBytes.writeByte(presenceBits);
        presenceBits = 0;
        presenceIndex -= Byte.SIZE;
      }
      // Set the bit at presenceIndex to flag that the corresponding arc is present.
      presenceBits |= 1 << presenceIndex;
      previousLabel = label;
    }
    assert presenceIndex == (nodeIn.arcs[nodeIn.numArcs - 1].label - nodeIn.arcs[0].label) % 8;
    assert presenceBits != 0; // The last byte is not 0.
    assert (presenceBits & (1 << presenceIndex)) != 0; // The last arc is always present.
    scratchBytes.writeByte(presenceBits);
  }

  private void freezeTail(int prefixLenPlus1) throws IOException {// 开始从后面向前面冰冻

    final int downTo = Math.max(1, prefixLenPlus1);//这里downTo大于等于1可以保证根节点不会被写入到FST中去，根节点必须要所有节点写完之后才能写到FST

    for (int idx = lastInput.length(); idx >= downTo; idx--) {// 节点idx，从后向前，只压缩存储不同的后缀

      final UnCompiledNode<T> node = frontier[idx];
      final int prevIdx = idx - 1;
      final UnCompiledNode<T> parent = frontier[prevIdx];
      // We need use this variable rather than node.output to call replaceLast later, because
      // compileNode(node) will clear node's state.
      final T nextFinalOutput = node.output;// 后面节点的output

      // If this node has no outgoing arcs, it should be final.
      assert node.numArcs != 0 || node.isFinal;
      // We need use this variable rather than node.isFinal to call replaceLast later, because
      // compileNode(node) will clear node's state.
      final boolean isFinal = node.isFinal;

      // this node makes it and we now compile it.  first,
      // compile any targets that were previously
      // undecided:
      parent.replaceLast(lastInput.intAt(prevIdx), compileNode(node), nextFinalOutput, isFinal);// 最后一条边就是当前工作的边
    }
  }

  /**
   * Add the next input/output pair. The provided input must be sorted after the previous one
   * according to {@link IntsRef#compareTo}. It's also OK to add the same input twice in a row with
   * different outputs, as long as {@link Outputs} implements the {@link Outputs#merge} method. Note
   * that input is fully consumed after this method is returned (so caller is free to reuse), but
   * output is not. So if your outputs are changeable (eg {@link ByteSequenceOutputs} or {@link
   * IntSequenceOutputs}) then you cannot reuse across calls.
   */
  public void add(IntsRef input, T output) throws IOException { //input：该节点前缀，output该节点存储的数据再tim中的起始位置
    // De-dup NO_OUTPUT since it must be a singleton:
    if (output.equals(NO_OUTPUT)) { // input前缀长度
      output = NO_OUTPUT;
    }

    assert lastInput.length() == 0 || input.compareTo(lastInput.get()) >= 0
        : "inputs are added out of order lastInput=" + lastInput.get() + " vs input=" + input;
    assert validOutput(output);

    // System.out.println("\nadd: " + input);
    if (input.length == 0) {// 当输入term是否为空，一定会发生，主要是Lucene90BlockTreeTermsWriter.writeBlocks(0, pending.size())中，将前缀长度指定为0
      // empty input: only allowed as first input.  we have
      // to special case this because the packed FST
      // format cannot represent the empty input since
      // 'finalness' is stored on the incoming arc, not on
      // the node
      frontier[0].isFinal = true;// 就确定把这个HEAD0为null，挂靠到跟节点上了
      setEmptyOutput(output); // 这里output设置为emptyOutput
      return;
    }

    // compare shared prefix length
    int pos1 = 0;// 找到和上一个词的公共长度下标
    int pos2 = input.offset;
    final int pos1Stop = Math.min(lastInput.length(), input.length);
    while (pos1 < pos1Stop && lastInput.intAt(pos1) == input.ints[pos2]) {// 遍历当前term与上个term的公共前缀的长度
      pos1++;
      pos2++;
    }
    final int prefixLenPlus1 = pos1 + 1;// 公共长度+1

    if (frontier.length < input.length + 1) {// 每个字母都会占数组的一个元素，// 扩容
      final UnCompiledNode<T>[] next = ArrayUtil.grow(frontier, input.length + 1);
      for (int idx = frontier.length; idx < next.length; idx++) {
        next[idx] = new UnCompiledNode<>(this, idx);
      }
      frontier = next;
    }

    // minimize/compile states from previous input's
    // orphan'd suffix
    freezeTail(prefixLenPlus1); // 冰冻不同的psuffix
   //  当前剩余的字符继续写入, 后面会针对边添加
    // init tail states for current input
    for (int idx = prefixLenPlus1; idx <= input.length; idx++) {
      frontier[idx - 1].addArc(input.ints[input.offset + idx - 1], frontier[idx]); // 增加一个公共边，但是都不是isFinal
    }
    // 给新增了一个尾部节点，设置isFinal为true
    final UnCompiledNode<T> lastNode = frontier[input.length]; //
    if (lastInput.length() != input.length || prefixLenPlus1 != input.length + 1) {// 保证lastInput和input是不一样的
      lastNode.isFinal = true;
      lastNode.output = NO_OUTPUT;
    }

    // push conflicting outputs forward, only as far as
    // needed
    for (int idx = 1; idx < prefixLenPlus1; idx++) {//  怎么共享output，只要两个存在的边和新写入的边有相同部分，就共享
      final UnCompiledNode<T> node = frontier[idx];
      final UnCompiledNode<T> parentNode = frontier[idx - 1];
      // 既然可以共享前缀，那么前缀边和本边是一致的，那么一定是一条直线，没有分叉。
      final T lastOutput = parentNode.getLastOutput(input.ints[input.offset + idx - 1]);
      assert validOutput(lastOutput);

      final T commonOutputPrefix;

      if (lastOutput != NO_OUTPUT) {// 读取上一个的output，若存在，检查是否可以和新的合并
        commonOutputPrefix = fst.outputs.common(output, lastOutput);// 比字符，
        assert validOutput(commonOutputPrefix);
        T wordSuffix = fst.outputs.subtract(lastOutput, commonOutputPrefix);// lastOutput-commonOutputPrefix=不同的后缀部分
        assert validOutput(wordSuffix);
        parentNode.setLastOutput(input.ints[input.offset + idx - 1], commonOutputPrefix);
        node.prependOutput(wordSuffix);//将节点不相同的部分向后面的边移动
      } else {
        commonOutputPrefix = NO_OUTPUT;// 那么没有什么好减少的了,遇到了，那么感觉就该退出了
      }

      output = fst.outputs.subtract(output, commonOutputPrefix);// 循环减去剩余部分
      assert validOutput(output);
    }
     // 本次输入的内容和上次输入的内容一样，则重新分配output
    if (lastInput.length() == input.length && prefixLenPlus1 == 1 + input.length) {
      // same input more than 1 time in a row, mapping to
      // multiple outputs
      lastNode.output = fst.outputs.merge(lastNode.output, output);// 那么看output是否定义了将outpue合并的方法，比如1+1=2，就存储2
    } else {
      // this new arc is private to this new input; set its
      // arc output to the leftover output:
      frontier[prefixLenPlus1 - 1].setLastOutput( //再设置这次的终点设置output
          input.ints[input.offset + prefixLenPlus1 - 1], output);
    }

    // save last input
    lastInput.copyInts(input);
  }

  void setEmptyOutput(T v) {
    if (fst.metadata.emptyOutput != null) {
      fst.metadata.emptyOutput = fst.outputs.merge(fst.metadata.emptyOutput, v);
    } else {
      fst.metadata.emptyOutput = v;// 第一个fst
    }
  }

  void finish(long newStartNode) {// 从结尾到最开始header逐个写入，header存放的fst下表
    assert newStartNode <= numBytesWritten;
    if (fst.metadata.startNode != -1) {
      throw new IllegalStateException("already finished");
    }
    if (newStartNode == FINAL_END_NODE && fst.metadata.emptyOutput != null) {
      newStartNode = 0;
    }
    fst.metadata.startNode = newStartNode;
    fst.metadata.numBytes = numBytesWritten;
    // freeze the dataOutput if applicable
    if (dataOutput instanceof ReadWriteDataOutput) {
      ((ReadWriteDataOutput) dataOutput).freeze();
    }
  }

  private boolean validOutput(T output) {
    return output == NO_OUTPUT || !output.equals(NO_OUTPUT);
  }

  /**
   * Returns the metadata of the final FST. NOTE: this will return null if nothing is accepted by
   * the FST themselves.
   *
   * <p>To create the FST, you need to:
   *
   * <p>- If a FSTReader DataOutput was used, such as the one returned by {@link
   * #getOnHeapReaderWriter(int)}
   *
   * <pre class="prettyprint">
   *     fstMetadata = fstCompiler.compile();
   *     fst = FST.fromFSTReader(fstMetadata, fstCompiler.getFSTReader());
   * </pre>
   *
   * <p>- If a non-FSTReader DataOutput was used, such as {@link
   * org.apache.lucene.store.IndexOutput}, you need to first create the corresponding {@link
   * org.apache.lucene.store.DataInput}, such as {@link org.apache.lucene.store.IndexInput} then
   * pass it to the FST construct
   *
   * <pre class="prettyprint">
   *     fstMetadata = fstCompiler.compile();
   *     fst = new FST&lt;&gt;(fstMetadata, dataInput, new OffHeapFSTStore());
   * </pre>
   */
  public FST.FSTMetadata<T> compile() throws IOException {

    final UnCompiledNode<T> root = frontier[0];

    // minimize nodes in the last word's suffix
    freezeTail(0);// 先解决之前的
    if (root.numArcs == 0) {
      if (fst.metadata.emptyOutput == null) {
        // return null for completely empty FST which accepts nothing
        return null;
      } else {
        // we haven't written the padding byte so far, but the FST is still valid
        writePaddingByte();
      }
    }

    // if (DEBUG) System.out.println("  builder.finish root.isFinal=" + root.isFinal + "
    // root.output=" + root.output);
    finish(compileNode(root).node);

    return fst.metadata;
  }

  /** Expert: holds a pending (seen but not yet serialized) arc. */
  static class Arc<T> {// FST中也有一个Arc
    int label; // really an "unsigned" byte 该节点对应的那个字符  10进制
    Node target;// 该边目标节点
    boolean isFinal; // 其target是-1，例如:ab,ac,ad。那么b,c,d都是最后一条边，这三个边都指向EMPYTY_NODE
    T output;// 就是add(key, output)，在lucene中，存放的是：用来指向26个term的fp等信息
    T nextFinalOutput;//
  }

  // NOTE: not many instances of Node or CompiledNode are in
  // memory while the FST is being built; it's only the
  // current "frontier":

  interface Node {
    boolean isCompiled();
  }

  public long fstRamBytesUsed() {
    long ramBytesUsed = scratchBytes.ramBytesUsed();
    if (dataOutput instanceof Accountable) {
      ramBytesUsed += ((Accountable) dataOutput).ramBytesUsed();
    }
    return ramBytesUsed;
  }

  public long fstSizeInBytes() {
    return numBytesWritten;
  }

  static final class CompiledNode implements Node {
    long node; // 下游边在fst中存储的起始offset位置信息。

    @Override
    public boolean isCompiled() {
      return true;
    }
  }

  /** Expert: holds a pending (seen but not yet serialized) Node. */
  static final class UnCompiledNode<T> implements Node {
    final FSTCompiler<T> owner;
    int numArcs; // 该节点的出边条数
    Arc<T>[] arcs;// 该节点总共的出边
    // TODO: instead of recording isFinal/output on the
    // node, maybe we should use -1 arc to mean "end" (like
    // we do when reading the FST).  Would simplify much
    // code here...
    T output; //实际是在在PendingBlock.compileIndex()中构建的，用来指向26个term的fp等信息
    boolean isFinal; // 表示从Node出发的Arc数量是0，当Node是Final Node时，output才有值。

    /** This node's depth, starting from the automaton root. */
    final int depth; // 从根节点到本节点的深度

    /**
     * @param depth The node's depth starting from the automaton root. Needed for LUCENE-2934 (node
     *     expansion based on conditions other than the fanout size).
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    UnCompiledNode(FSTCompiler<T> owner, int depth) {
      this.owner = owner;
      arcs = (Arc<T>[]) new Arc[1];
      arcs[0] = new Arc<>();
      output = owner.NO_OUTPUT;
      this.depth = depth;
    }

    @Override
    public boolean isCompiled() {
      return false;
    }

    void clear() {
      numArcs = 0; // 代表清了
      isFinal = false;
      output = owner.NO_OUTPUT;

      // We don't clear the depth here because it never changes
      // for nodes on the frontier (even when reused).
    }

    T getLastOutput(int labelToMatch) {
      assert numArcs > 0;
      assert arcs[numArcs - 1].label == labelToMatch;
      return arcs[numArcs - 1].output;
    }
    // 该节点添加一个出边,
    void addArc(int label, Node target) { // targe=UnCompiledNode
      assert label >= 0; // 该节点对应的字母
      assert numArcs == 0 || label > arcs[numArcs - 1].label
          : "arc[numArcs-1].label="
              + arcs[numArcs - 1].label
              + " new label="
              + label
              + " numArcs="
              + numArcs;
      if (numArcs == arcs.length) {// 可以伸缩扩展的
        final Arc<T>[] newArcs = ArrayUtil.grow(arcs);
        for (int arcIdx = numArcs; arcIdx < newArcs.length; arcIdx++) {
          newArcs[arcIdx] = new Arc<>();
        }
        arcs = newArcs;
      }
      final Arc<T> arc = arcs[numArcs++];//出边记录+1
      arc.label = label;
      arc.target = target;
      arc.output = arc.nextFinalOutput = owner.NO_OUTPUT;// 记录output全部为null
      arc.isFinal = false;
    }
    // 将该节点的下游给替换掉已达到共享的目的     target:默认fst存储位置   nextFinalOutput：
    void replaceLast(int labelToMatch, Node target, T nextFinalOutput, boolean isFinal) {
      assert numArcs > 0;
      final Arc<T> arc = arcs[numArcs - 1];// 最后一条边就是当前工作的边
      assert arc.label == labelToMatch : "arc.label=" + arc.label + " vs " + labelToMatch;
      arc.target = target; // CompiledNode，编译后，此时target存放着该字母fst的存储位置
      // assert target.node != -2;
      arc.nextFinalOutput = nextFinalOutput;
      arc.isFinal = isFinal;
    }
    // 设置最后一条边的output
    void setLastOutput(int labelToMatch, T newOutput) {
      assert owner.validOutput(newOutput);
      assert numArcs > 0;
      final Arc<T> arc = arcs[numArcs - 1];
      assert arc.label == labelToMatch;
      arc.output = newOutput;
    }

    // pushes an output prefix forward onto all arcs
    void prependOutput(T outputPrefix) {
      assert owner.validOutput(outputPrefix);
// 把新的output向后推， 实际是每个边已经存在的output和新的output合并再写入本边，
      for (int arcIdx = 0; arcIdx < numArcs; arcIdx++) {
        arcs[arcIdx].output = owner.fst.outputs.add(outputPrefix, arcs[arcIdx].output); // 把两个BytesRef的byte强制给合并到同一个中
        assert owner.validOutput(arcs[arcIdx].output);
      }

      if (isFinal) { // 若是结尾节点， 节点output也得更正
        output = owner.fst.outputs.add(outputPrefix, output);
        assert owner.validOutput(output);
      }
    }
  }

  /**
   * Reusable buffer for building nodes with fixed length arcs (binary search or direct addressing).
   */
  static class FixedLengthArcsBuffer {

    // Initial capacity is the max length required for the header of a node with fixed length arcs:
    // header(byte) + numArcs(vint) + numBytes(vint)
    private byte[] bytes = new byte[11];
    private final ByteArrayDataOutput bado = new ByteArrayDataOutput(bytes);

    /** Ensures the capacity of the internal byte array. Enlarges it if needed. */
    FixedLengthArcsBuffer ensureCapacity(int capacity) {
      if (bytes.length < capacity) {
        bytes = new byte[ArrayUtil.oversize(capacity, Byte.BYTES)];
        bado.reset(bytes);
      }
      return this;
    }

    FixedLengthArcsBuffer resetPosition() {
      bado.reset(bytes);
      return this;
    }

    FixedLengthArcsBuffer writeByte(byte b) {
      bado.writeByte(b);
      return this;
    }

    FixedLengthArcsBuffer writeVInt(int i) {
      try {
        bado.writeVInt(i);
      } catch (IOException e) { // Never thrown.
        throw new RuntimeException(e);
      }
      return this;
    }

    int getPosition() {
      return bado.getPosition();
    }

    /** Gets the internal byte array. */
    byte[] getBytes() {
      return bytes;
    }
  }
}
