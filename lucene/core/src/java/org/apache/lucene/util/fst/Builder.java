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


import java.io.IOException;

import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.FST.INPUT_TYPE; // javadoc

// TODO: could we somehow stream an FST to disk while we
// build it?

/**
 * Builds a minimal FST (maps an IntsRef term to an arbitrary
 * output) from pre-sorted terms with outputs.  The FST
 * becomes an FSA if you use NoOutputs.  The FST is written
 * on-the-fly into a compact serialized format byte array, which can
 * be saved to / loaded from a Directory or used directly
 * for traversal.  The FST is always finite (no cycles).
 *
 * <p>NOTE: The algorithm is described at
 * http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.24.3698</p>
 *
 * <p>The parameterized type T is the output type.  See the
 * subclasses of {@link Outputs}.
 *
 * <p>FSTs larger than 2.1GB are now possible (as of Lucene
 * 4.2).  FSTs containing more than 2.1B nodes are also now
 * possible, however they cannot be packed.
 *
 * @lucene.experimental
 */

public class Builder<T> {

  /**
   * Default oversizing factor used to decide whether to encode a node with direct addressing or binary search.
   * Default is 1: ensure no oversizing on average.
   * <p>
   * This factor does not determine whether to encode a node with a list of variable length arcs or with
   * fixed length arcs. It only determines the effective encoding of a node that is already known to be
   * encoded with fixed length arcs.
   * See {@code FST.shouldExpandNodeWithFixedLengthArcs()}
   * and {@code FST.shouldExpandNodeWithDirectAddressing()}.
   * <p>
   * For English words we measured 217K nodes, only 3.27% nodes are encoded with fixed length arcs,
   * and 99.99% of them with direct addressing. Overall FST memory reduced by 1.67%.
   * <p>
   * For worst case we measured 168K nodes, 50% of them are encoded with fixed length arcs,
   * and 14% of them with direct encoding. Overall FST memory reduced by 0.8%.
   * <p>
   * Use {@code TestFstDirectAddressing.main()}
   * and {@code TestFstDirectAddressing.testWorstCaseForDirectAddressing()}
   * to evaluate a change.
   *
   * @see #setDirectAddressingMaxOversizingFactor
   */
  static final float DIRECT_ADDRESSING_MAX_OVERSIZING_FACTOR = 1.0f;

  private final NodeHash<T> dedupHash;
  final FST<T> fst;
  private final T NO_OUTPUT;

  // private static final boolean DEBUG = true;

  // simplistic pruning: we prune node (and all following
  // nodes) if less than this number of terms go through it:
  private final int minSuffixCount1; // 0

  // better pruning: we prune node (and all following
  // nodes) if the prior node has less than this number of
  // terms go through it:
  private final int minSuffixCount2; // 0

  private final boolean doShareNonSingletonNodes;
  private final int shareMaxTailLength;

  private final IntsRefBuilder lastInput = new IntsRefBuilder(); // 上一个相同的前缀

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
  long lastFrozenNode; // 最后冷冻的那个节点存储位置

  // Reused temporarily while building the FST:
  int[] numBytesPerArc = new int[4];// 记录的当前节点所有边的长度，临时变量
  int[] numLabelBytesPerArc = new int[numBytesPerArc.length];
  final FixedLengthArcsBuffer fixedLengthArcsBuffer = new FixedLengthArcsBuffer();

  long arcCount; // 该fst中总共边的个数
  long nodeCount;// 该fst中节点的个数
  long binarySearchNodeCount;
  long directAddressingNodeCount;

  boolean allowFixedLengthArcs;// 默认为true
  float directAddressingMaxOversizingFactor = DIRECT_ADDRESSING_MAX_OVERSIZING_FACTOR;
  long directAddressingExpansionCredit;

  BytesStore bytes; // 就是从FST中取值的

  /**
   * Instantiates an FST/FSA builder without any pruning. A shortcut to {@link
   * #Builder(FST.INPUT_TYPE, int, int, boolean, boolean, int, Outputs, boolean, int)} with
   * pruning options turned off.
   */
  public Builder(FST.INPUT_TYPE inputType, Outputs<T> outputs) {
    this(inputType, 0, 0, true, true, Integer.MAX_VALUE, outputs, true, 15);
  }

  /**
   * Instantiates an FST/FSA builder with all the possible tuning and construction
   * tweaks. Read parameter documentation carefully.
   * 
   * @param inputType 
   *    The input type (transition labels). Can be anything from {@link INPUT_TYPE}
   *    enumeration. Shorter types will consume less memory. Strings (character sequences) are 
   *    represented as {@link INPUT_TYPE#BYTE4} (full unicode codepoints). 
   *     
   * @param minSuffixCount1
   *    If pruning the input graph during construction, this threshold is used for telling
   *    if a node is kept or pruned. If transition_count(node) &gt;= minSuffixCount1, the node
   *    is kept. 
   *    
   * @param minSuffixCount2
   *    (Note: only Mike McCandless knows what this one is really doing...) 
   * 
   * @param doShareSuffix 
   *    If <code>true</code>, the shared suffixes will be compacted into unique paths.
   *    This requires an additional RAM-intensive hash map for lookups in memory. Setting this parameter to
   *    <code>false</code> creates a single suffix path for all input sequences. This will result in a larger
   *    FST, but requires substantially less memory and CPU during building.  
   *
   * @param doShareNonSingletonNodes
   *    Only used if doShareSuffix is true.  Set this to
   *    true to ensure FST is fully minimal, at cost of more
   *    CPU and more RAM during building.
   *
   * @param shareMaxTailLength
   *    Only used if doShareSuffix is true.  Set this to
   *    Integer.MAX_VALUE to ensure FST is fully minimal, at cost of more
   *    CPU and more RAM during building.
   *
   * @param outputs The output type for each input sequence. Applies only if building an FST. For
   *    FSA, use {@link NoOutputs#getSingleton()} and {@link NoOutputs#getNoOutput()} as the
   *    singleton output object.
   *
   * @param allowFixedLengthArcs Pass false to disable the fixed length arc optimization (binary search or
   *    direct addressing) while building the FST; this will make the resulting FST smaller but slower to
   *    traverse.
   *
   * @param bytesPageBits How many bits wide to make each
   *    byte[] block in the BytesStore; if you know the FST
   *    will be large then make this larger.  For example 15
   *    bits = 32768 byte pages.
   */
  public Builder(FST.INPUT_TYPE inputType, int minSuffixCount1, int minSuffixCount2, boolean doShareSuffix,
                 boolean doShareNonSingletonNodes, int shareMaxTailLength, Outputs<T> outputs,
                 boolean allowFixedLengthArcs, int bytesPageBits) {
    this.minSuffixCount1 = minSuffixCount1;// 0
    this.minSuffixCount2 = minSuffixCount2; // 0
    this.doShareNonSingletonNodes = doShareNonSingletonNodes;// False
    this.shareMaxTailLength = shareMaxTailLength;
    this.allowFixedLengthArcs = allowFixedLengthArcs;// 为true
    fst = new FST<>(inputType, outputs, bytesPageBits);// inputType=INPUT_TYPE.BYTE1， 也是新创建的
    bytes = fst.bytes;
    assert bytes != null;
    if (doShareSuffix) { // 是true
      dedupHash = new NodeHash<>(fst, bytes.getReverseReader(false));
    } else {
      dedupHash = null;
    }
    NO_OUTPUT = outputs.getNoOutput();

    @SuppressWarnings({"rawtypes","unchecked"}) final UnCompiledNode<T>[] f =
        (UnCompiledNode<T>[]) new UnCompiledNode[10];
    frontier = f;
    for(int idx=0;idx<frontier.length;idx++) {
      frontier[idx] = new UnCompiledNode<>(this, idx);
    }
  }

  /**
   * Overrides the default the maximum oversizing of fixed array allowed to enable direct addressing
   * of arcs instead of binary search.
   * <p>
   * Setting this factor to a negative value (e.g. -1) effectively disables direct addressing,
   * only binary search nodes will be created.
   *
   * @see #DIRECT_ADDRESSING_MAX_OVERSIZING_FACTOR
   */
  public Builder<T> setDirectAddressingMaxOversizingFactor(float factor) {
    directAddressingMaxOversizingFactor = factor;
    return this;
  }

  /**
   * @see #setDirectAddressingMaxOversizingFactor(float)
   */
  public float getDirectAddressingMaxOversizingFactor() {
    return directAddressingMaxOversizingFactor;
  }

  public long getTermCount() {
    return frontier[0].inputCount;
  }

  public long getNodeCount() {
    // 1+ in order to count the -1 implicit final node
    return 1+nodeCount;
  }
  
  public long getArcCount() {
    return arcCount;
  }

  public long getMappedStateCount() {
    return dedupHash == null ? 0 : nodeCount;
  }
  // compileNode可能返回的是一个已经写入 FST的 Node的 ID，这样就达到了共享 Node 的目的。tailLength是可能共享的长度
  private CompiledNode compileNode(UnCompiledNode<T> nodeIn, int tailLength) throws IOException {
    final long node;
    long bytesPosStart = bytes.getPosition();
    if (dedupHash != null && (doShareNonSingletonNodes || nodeIn.numArcs <= 1) && tailLength <= shareMaxTailLength) {// doShareNonSingletonNodes默认为false
      if (nodeIn.numArcs == 0) { // 说明该节点是结尾。
        node = fst.addNode(this, nodeIn);// Node的Arc数量为0，fst.addNode 会返回 -1。已经将nodeId的value存放了fst中
        lastFrozenNode = node; // 每次重新输入一条边，又开始置位-1
      } else { // root节点也会进来
        node = dedupHash.add(this, nodeIn); // 这个就是node
      }
    } else {
      node = fst.addNode(this, nodeIn); // 把node写入fst中
    }
    assert node != -2;

    long bytesPosEnd = bytes.getPosition(); // 看看是否有写入
    if (bytesPosEnd != bytesPosStart) { // 有写入
      // The FST added a new node:
      assert bytesPosEnd > bytesPosStart;
      lastFrozenNode = node;
    }
    // 这里将这个nodeIn给清空了，意味着该边被compile了，
    nodeIn.clear();

    final CompiledNode fn = new CompiledNode();
    fn.node = node;
    return fn;
  }
  // 开始从后面向前面冰冻
  private void freezeTail(int prefixLenPlus1) throws IOException {
    //System.out.println("  compileTail " + prefixLenPlus1);
    final int downTo = Math.max(1, prefixLenPlus1);//这里downTo大于等于1可以保证根节点不会被写入到FST中去，根节点必须要所有节点写完之后才能写到FST
    for(int idx=lastInput.length(); idx >= downTo; idx--) { // 节点idx，从后向前，只压缩存储不同的后缀

      boolean doPrune = false; // 是否需要修剪
      boolean doCompile = false;

      final UnCompiledNode<T> node = frontier[idx]; // 结尾那个NULL的node
      final UnCompiledNode<T> parent = frontier[idx-1];
// 如果Node的出度小于minSuffixCount1，会被删除
      if (node.inputCount < minSuffixCount1) { // 始终不成立
        doPrune = true;
        doCompile = true;
        } else if (idx > prefixLenPlus1) {
        // prune if parent's inputCount is less than suffixMinCount2   // 判断父亲节点
        if (parent.inputCount < minSuffixCount2 || (minSuffixCount2 == 1 && parent.inputCount == 1 && idx > 1)) { // 永远不成立
          // my parent, about to be compiled, doesn't make the cut, so
          // I'm definitely pruned 

          // if minSuffixCount2 is 1, we keep only up
          // until the 'distinguished edge', ie we keep only the
          // 'divergent' part of the FST. if my parent, about to be
          // compiled, has inputCount 1 then we are already past the
          // distinguished edge.  NOTE: this only works if
          // the FST outputs are not "compressible" (simple
          // ords ARE compressible).
          doPrune = true;
        } else {
          // my parent, about to be compiled, does make the cut, so
          // I'm definitely not pruned 
          doPrune = false;
        }
        doCompile = true;
      } else {
        // if pruning is disabled (count is 0) we can always
        // compile current node
        doCompile = minSuffixCount2 == 0;
      }

      //System.out.println("    label=" + ((char) lastInput.ints[lastInput.offset+idx-1]) + " idx=" + idx + " inputCount=" + frontier[idx].inputCount + " doCompile=" + doCompile + " doPrune=" + doPrune);
      // 永远不成立
      if (node.inputCount < minSuffixCount2 || (minSuffixCount2 == 1 && node.inputCount == 1 && idx > 1)) {
        // drop all arcs
        for(int arcIdx=0;arcIdx<node.numArcs;arcIdx++) {
          @SuppressWarnings({"rawtypes","unchecked"}) final UnCompiledNode<T> target =
          (UnCompiledNode<T>) node.arcs[arcIdx].target;
          target.clear();
        }
        node.numArcs = 0;
      }
      // 删除当前节点，那么父节点需要删除边
      if (doPrune) { // 一直为false
        // this node doesn't make it -- deref it
        node.clear();
        parent.deleteLast(lastInput.intAt(idx-1), node);
      } else { // 永远进来

        if (minSuffixCount2 != 0) {
          compileAllTargets(node, lastInput.length()-idx);
        }
        final T nextFinalOutput = node.output; // 结尾那个NULL 节点

        // We "fake" the node as being final if it has no
        // outgoing arcs; in theory we could leave it
        // as non-final (the FST can represent this), but
        // FSTEnum, Util, etc., have trouble w/ non-final
        // dead-end states:
        final boolean isFinal = node.isFinal || node.numArcs == 0; // 若本节点没有出边，可以freeze起来
        // 用compileNode 函数将 Node写入FST，得到一个 CompiledNode，替换掉 Arc的target
        if (doCompile) { // 始终进来
          // this node makes it and we now compile it.  first,
          // compile any targets that were previously
          // undecided:
          parent.replaceLast(lastInput.intAt(idx-1),
                             compileNode(node, 1+lastInput.length()-idx),// compileNode可能返回的是一个已经写入 FST的 Node的 ID，这样就达到了共享 Node 的目的
                             nextFinalOutput,
                             isFinal);
        } else {
          // replaceLast just to install
          // nextFinalOutput/isFinal onto the arc
          parent.replaceLast(lastInput.intAt(idx-1),
                             node,
                             nextFinalOutput,
                             isFinal);
          // this node will stay in play for now, since we are
          // undecided on whether to prune it.  later, it
          // will be either compiled or pruned, so we must
          // allocate a new node:
          frontier[idx] = new UnCompiledNode<>(this, idx);
        }
      }
    }
  }

  // for debugging
  /*
  private String toString(BytesRef b) {
    try {
      return b.utf8ToString() + " " + b;
    } catch (Throwable t) {
      return b.toString();
    }
  }
  */

  /** Add the next input/output pair.  The provided input
   *  must be sorted after the previous one according to
   *  {@link IntsRef#compareTo}.  It's also OK to add the same
   *  input twice in a row with different outputs, as long
   *  as {@link Outputs} implements the {@link Outputs#merge}
   *  method. Note that input is fully consumed after this
   *  method is returned (so caller is free to reuse), but  随意重新使用
   *  output is not.  So if your outputs are changeable (eg
   *  {@link ByteSequenceOutputs} or {@link
   *  IntSequenceOutputs}) then you cannot reuse across
   *  calls. */ // 相同前缀
  public void add(IntsRef input, T output) throws IOException { // output: BytesRef
    /*  // input前缀长度
    if (DEBUG) {
      BytesRef b = new BytesRef(input.length);
      for(int x=0;x<input.length;x++) {
        b.bytes[x] = (byte) input.ints[x];
      }
      b.length = input.length;
      if (output == NO_OUTPUT) {
        System.out.println("\nFST ADD: input=" + toString(b) + " " + b);
      } else {
        System.out.println("\nFST ADD: input=" + toString(b) + " " + b + " output=" + fst.outputs.outputToString(output));
      }
    }
    */

    // De-dup NO_OUTPUT since it must be a singleton:
    if (output.equals(NO_OUTPUT)) {
      output = NO_OUTPUT;
    }

    assert lastInput.length() == 0 || input.compareTo(lastInput.get()) >= 0: "inputs are added out of order lastInput=" + lastInput.get() + " vs input=" + input;
    assert validOutput(output);

    //System.out.println("\nadd: " + input);
    if (input.length == 0) {// 判断输入的term是否为空，
      // empty input: only allowed as first input.  we have
      // to special case this because the packed FST
      // format cannot represent the empty input since
      // 'finalness' is stored on the incoming arc, not on
      // the node
      frontier[0].inputCount++;
      frontier[0].isFinal = true; // 把这个为null，挂靠到跟节点上了
      fst.setEmptyOutput(output); // 这里设置为
      return;
    }

    // compare shared prefix length
    int pos1 = 0;
    int pos2 = input.offset;
    final int pos1Stop = Math.min(lastInput.length(), input.length);
    while(true) {// 遍历当前term与上个term的公共前缀的长度
      frontier[pos1].inputCount++; // 首先
      //System.out.println("  incr " + pos1 + " ct=" + frontier[pos1].inputCount + " n=" + frontier[pos1]);
      if (pos1 >= pos1Stop || lastInput.intAt(pos1) != input.ints[pos2]) {
        break;
      }
      pos1++;
      pos2++;
    }
    final int prefixLenPlus1 = pos1+1; // 公共长度+1
    // 每个字母都会占数组的一个元素
    if (frontier.length < input.length+1) { // 扩容
      final UnCompiledNode<T>[] next = ArrayUtil.grow(frontier, input.length+1);
      for(int idx=frontier.length;idx<next.length;idx++) {
        next[idx] = new UnCompiledNode<>(this, idx);
      }
      frontier = next;
    }
    // 冰冻不同的psuffix
    // minimize/compile states from previous input's
    // orphan'd suffix
    freezeTail(prefixLenPlus1);
     //
    // init tail states for current input  当前剩余的字符继续写入, 后面会针对边添加
    for(int idx=prefixLenPlus1;idx<=input.length;idx++) {
      frontier[idx-1].addArc(input.ints[input.offset + idx - 1], // 公共前缀第一个字符
                             frontier[idx]);
      frontier[idx].inputCount++; // 下一个节点，多了一条指入的边
    }
   // 新增的边，最后一个给设置为true
    final UnCompiledNode<T> lastNode = frontier[input.length];
    if (lastInput.length() != input.length || prefixLenPlus1 != input.length + 1) {
      lastNode.isFinal = true; //
      lastNode.output = NO_OUTPUT;
    }

    // push conflicting outputs forward, only as far as
    // needed  怎么共享output，只要两个存在的边和新写入的边有相同部分，就共享
    for(int idx=1;idx<prefixLenPlus1;idx++) {
      final UnCompiledNode<T> node = frontier[idx];
      final UnCompiledNode<T> parentNode = frontier[idx-1];
      // 既然可以共享前缀，那么前缀边和本边是一致的，那么一定是一条直线，没有分叉。
      final T lastOutput = parentNode.getLastOutput(input.ints[input.offset + idx - 1]);
      assert validOutput(lastOutput);

      final T commonOutputPrefix;// BytesRef
      final T wordSuffix;// BytesRef

      if (lastOutput != NO_OUTPUT) { // 读取上一个的output，若存在，检查是否可以和新的合并
        commonOutputPrefix = fst.outputs.common(output, lastOutput); // 比字符，
        assert validOutput(commonOutputPrefix);
        wordSuffix = fst.outputs.subtract(lastOutput, commonOutputPrefix); // lastOutput-commonOutputPrefix=不同的后缀部分
        assert validOutput(wordSuffix);
        parentNode.setLastOutput(input.ints[input.offset + idx - 1], commonOutputPrefix);
        node.prependOutput(wordSuffix); //将节点不相同的部分向后面的边移动
      } else {
        commonOutputPrefix = wordSuffix = NO_OUTPUT;
      }

      output = fst.outputs.subtract(output, commonOutputPrefix); // 循环减去剩余部分
      assert validOutput(output);
    }
    // 本次输入的内容和上次输入的内容一样，则重新分配output
    if (lastInput.length() == input.length && prefixLenPlus1 == 1+input.length) {
      // same input more than 1 time in a row, mapping to
      // multiple outputs
      lastNode.output = fst.outputs.merge(lastNode.output, output);
    } else {
      // this new arc is private to this new input; set its
      // arc output to the leftover output:
      frontier[prefixLenPlus1-1].setLastOutput(input.ints[input.offset + prefixLenPlus1-1], output);
    }

    // save last input
    lastInput.copyInts(input);

    //System.out.println("  count[0]=" + frontier[0].inputCount);
  }

  private boolean validOutput(T output) {
    return output == NO_OUTPUT || !output.equals(NO_OUTPUT);
  }

  /** Returns final FST.  NOTE: this will return null if
   *  nothing is accepted by the FST. */
  public FST<T> finish() throws IOException {

    final UnCompiledNode<T> root = frontier[0];
     // 将所有节点写入FST
    // minimize nodes in the last word's suffix
    freezeTail(0); // 传入的参数0，代表要处理frontier数组维护的除根节点之外的节点， 将所有节点写入FST
    if (root.inputCount < minSuffixCount1 || root.inputCount < minSuffixCount2 || root.numArcs == 0) { // 不会进来
      if (fst.emptyOutput == null) {
        return null;
      } else if (minSuffixCount1 > 0 || minSuffixCount2 > 0) {
        // empty string got pruned
        return null;
      }
    } else {
      if (minSuffixCount2 != 0) {
        compileAllTargets(root, lastInput.length());
      }
    } //结束FST的写入
    //if (DEBUG) System.out.println("  builder.finish root.isFinal=" + root.isFinal + " root.output=" + root.output);
    fst.finish(compileNode(root, lastInput.length()).node); // 将头也写入

    return fst;
  }

  private void compileAllTargets(UnCompiledNode<T> node, int tailLength) throws IOException {
    for(int arcIdx=0;arcIdx<node.numArcs;arcIdx++) {
      final Arc<T> arc = node.arcs[arcIdx];
      if (!arc.target.isCompiled()) {
        // not yet compiled
        @SuppressWarnings({"rawtypes","unchecked"}) final UnCompiledNode<T> n = (UnCompiledNode<T>) arc.target;
        if (n.numArcs == 0) {
          //System.out.println("seg=" + segment + "        FORCE final arc=" + (char) arc.label);
          arc.isFinal = n.isFinal = true;
        }
        arc.target = compileNode(n, tailLength-1);
      }
    }
  }
  // fst的边
  /** Expert: holds a pending (seen but not yet serialized) arc. */
  public static class Arc<T> {
    public int label;  // really an "unsigned" byte 该节点对应的那个字符  10进制
    public Node target; // 该边目标节点
    public boolean isFinal; // isFinal表示Arc已经是终止Arc，其target是-1，不可以继续深度遍历，不可以继续freeze。//例如:ab,ac,ad。那么b,c,d都是最后一条边
    public T output; // 表示当前Arc上有输出
    public T nextFinalOutput; //只有这种情况可用，
  }

  // NOTE: not many instances of Node or CompiledNode are in
  // memory while the FST is being built; it's only the
  // current "frontier":

  static interface Node {
    boolean isCompiled();
  }

  public long fstRamBytesUsed() {
    return fst.ramBytesUsed();
  }

  static final class CompiledNode implements Node {
    long node; // 这个node是干啥的
    @Override
    public boolean isCompiled() {
      return true;
    }
  }
 // fst的节点
  /** Expert: holds a pending (seen but not yet serialized) Node. */
  public static final class UnCompiledNode<T> implements Node {
    final Builder<T> owner;
    public int numArcs; // 该节点的出边条数
    public Arc<T>[] arcs; // 该节点总共的出边
    // TODO: instead of recording isFinal/output on the
    // node, maybe we should use -1 arc to mean "end" (like
    // we do when reading the FST).  Would simplify much
    // code here...
    public T output;  //
    public boolean isFinal; // 表示从Node出发的Arc数量是0，当Node是Final Node时，output才有值。
    public long inputCount; //多少个边把这个UnCompiledNode作为终点&终点，实际并没有用上

    /** This node's depth, starting from the automaton root. */
    public final int depth; // 从根节点到本节点的深度

    /**
     * @param depth
     *          The node's depth starting from the automaton root. Needed for
     *          LUCENE-2934 (node expansion based on conditions other than the
     *          fanout size).
     */
    @SuppressWarnings({"rawtypes","unchecked"})
    public UnCompiledNode(Builder<T> owner, int depth) {
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

    public void clear() {
      numArcs = 0; // 控制着边个数
      isFinal = false;
      output = owner.NO_OUTPUT;
      inputCount = 0;

      // We don't clear the depth here because it never changes 
      // for nodes on the frontier (even when reused).
    }
    // 获取
    public T getLastOutput(int labelToMatch) {
      assert numArcs > 0;
      assert arcs[numArcs-1].label == labelToMatch;
      return arcs[numArcs-1].output;
    }
    // 该节点添加一个出边
    public void addArc(int label, Node target) { // targe=UnCompiledNode
      assert label >= 0;  // 该节点对应的字母
      assert numArcs == 0 || label > arcs[numArcs-1].label: "arc[numArcs-1].label=" + arcs[numArcs-1].label + " new label=" + label + " numArcs=" + numArcs;
      if (numArcs == arcs.length) { // 可以伸缩扩展的
        final Arc<T>[] newArcs = ArrayUtil.grow(arcs, numArcs+1);
        for(int arcIdx=numArcs;arcIdx<newArcs.length;arcIdx++) {
          newArcs[arcIdx] = new Arc<>();
        }
        arcs = newArcs;
      }
      final Arc<T> arc = arcs[numArcs++];
      arc.label = label;
      arc.target = target;
      arc.output = arc.nextFinalOutput = owner.NO_OUTPUT;
      arc.isFinal = false;
    }
    // 将该节点的下游给替换掉已达到共享的目的
    public void replaceLast(int labelToMatch, Node target, T nextFinalOutput, boolean isFinal) {
      assert numArcs > 0;
      final Arc<T> arc = arcs[numArcs-1]; //
      assert arc.label == labelToMatch: "arc.label=" + arc.label + " vs " + labelToMatch;
      arc.target = target; // CompiledNode，编译后，此时target存放着该字母fst的存储位置
      //assert target.node != -2;
      arc.nextFinalOutput = nextFinalOutput;
      arc.isFinal = isFinal;
    }
    // 删除最后一条边
    public void deleteLast(int label, Node target) {
      assert numArcs > 0;
      assert label == arcs[numArcs-1].label;
      assert target == arcs[numArcs-1].target;
      numArcs--;
    }
    // 设置最后一条边的label和output
    public void setLastOutput(int labelToMatch, T newOutput) {
      assert owner.validOutput(newOutput);
      assert numArcs > 0;
      final Arc<T> arc = arcs[numArcs-1];
      assert arc.label == labelToMatch;
      arc.output = newOutput;
    }

    // pushes an output prefix forward onto all arcs
    public void prependOutput(T outputPrefix) {
      assert owner.validOutput(outputPrefix);
      // 把新的output向后推， 实际是每个边已经存在的output和新的output合并再写入本边，
      for(int arcIdx=0;arcIdx<numArcs;arcIdx++) {
        arcs[arcIdx].output = owner.fst.outputs.add(outputPrefix, arcs[arcIdx].output); // 把两个BytesRef的byte强制给合并到同一个中
        assert owner.validOutput(arcs[arcIdx].output);
      }
      // 若是结尾节点， 节点output也得更正
      if (isFinal) {
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
