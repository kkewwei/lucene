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

package org.apache.lucene.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.InPlaceMergeSorter;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;
// 为每一个segmentValueCount小于globalValueCount的segment，保存了一份segment ord到global ord的mapping（LongValues）。对于segment valueCount等于globalValueCount的segment，原本的segment ord就是global ord，后续获取ord时，直接从SortedSetDV(dvd)中读取。
/** Maps per-segment ordinals to/from global ordinal space, using a compact packed-ints representation.
 * Maps per-segment ordinals to/from global ordinal space, using a compact packed-ints
 * representation.
 *
 * <p><b>NOTE</b>: this is a costly operation, as it must merge sort all terms, and may require
 * non-trivial RAM once done. It's better to operate in segment-private ordinal space instead when
 * possible.
 *
 * @lucene.internal
 */
public class OrdinalMap implements Accountable {// 排序值（Ordinal）
  // TODO: we could also have a utility method to merge Terms[] and use size() as a weight when we
  // need it
  // TODO: use more efficient packed ints structures?

  private static class TermsEnumPriorityQueue extends PriorityQueue<TermsEnumIndex> {

    TermsEnumPriorityQueue(int size) {
      super(size);
    }

    @Override
    protected boolean lessThan(TermsEnumIndex a, TermsEnumIndex b) {// 并没有考虑文档id大小的问题
      return a.compareTermTo(b) < 0;// 对比的是当前currentTerm，小的放前面
    }
  }

  private static class SegmentMap implements Accountable {
    private static final long BASE_RAM_BYTES_USED =
        RamUsageEstimator.shallowSizeOfInstance(SegmentMap.class);

    /** Build a map from an index into a sorted view of `weights` to an index into `weights`. */
    private static int[] map(final long[] weights) {
      final int[] newToOld = new int[weights.length];
      for (int i = 0; i < weights.length; ++i) {
        newToOld[i] = i;
      }
      new InPlaceMergeSorter() { // segment中词多的在前面
        @Override
        protected void swap(int i, int j) {
          final int tmp = newToOld[i];
          newToOld[i] = newToOld[j];
          newToOld[j] = tmp;
        }

        @Override
        protected int compare(int i, int j) {// 从大到小排序
          // j first since we actually want higher weights first
          return Long.compare(weights[newToOld[j]], weights[newToOld[i]]);
        }
      }.sort(0, weights.length);
      return newToOld;
    }

    /** Inverse the map. */
    private static int[] inverse(int[] map) {
      final int[] inverse = new int[map.length];
      for (int i = 0; i < map.length; ++i) {
        inverse[map[i]] = i;
      }
      return inverse;
    }
    // newToOld[0]=3，按照词个数排序，排第0位，是原来第三个词
    private final int[] newToOld, oldToNew; // 为了按照segment词个数从到小读取

    SegmentMap(long[] weights) {
      newToOld = map(weights); // newToOld[0]=3，按照词个数大小排序，排第0位词个数最多，是原来第三个segment
      oldToNew = inverse(newToOld);  // oldToNew[0]=3。 第0个segment，目前词大小排序，是排第3的
      assert Arrays.equals(newToOld, inverse(oldToNew));
    }
    // 得到实际的那个segment
    int newToOld(int segment) {
      return newToOld[segment];
    }

    int oldToNew(int segment) {
      return oldToNew[segment];
    }

    @Override
    public long ramBytesUsed() {
      return BASE_RAM_BYTES_USED
          + RamUsageEstimator.sizeOf(newToOld)
          + RamUsageEstimator.sizeOf(oldToNew);
    }
  }

  /**
   * Create an ordinal map that uses the number of unique values of each {@link SortedDocValues}
   * instance as a weight.
   *
   * @see #build(IndexReader.CacheKey, TermsEnum[], long[], float)
   */
  public static OrdinalMap build(
      IndexReader.CacheKey owner, SortedDocValues[] values, float acceptableOverheadRatio)
      throws IOException {
    final TermsEnum[] subs = new TermsEnum[values.length];
    final long[] weights = new long[values.length];
    for (int i = 0; i < values.length; ++i) {
      subs[i] = values[i].termsEnum();
      weights[i] = values[i].getValueCount();
    }
    return build(owner, subs, weights, acceptableOverheadRatio);
  }

  /**
   * Create an ordinal map that uses the number of unique values of each {@link SortedSetDocValues}
   * instance as a weight.
   *
   * @see #build(IndexReader.CacheKey, TermsEnum[], long[], float)
   */
  public static OrdinalMap build(
      IndexReader.CacheKey owner, SortedSetDocValues[] values, float acceptableOverheadRatio)
      throws IOException {
    final TermsEnum[] subs = new TermsEnum[values.length];//多少个segment
    final long[] weights = new long[values.length];// 每个segment多少个独立的词
    for (int i = 0; i < values.length; ++i) {// 几个segment
      subs[i] = values[i].termsEnum();// 跑到将进入SingletonSortedSetDocValues.termsEnum(),映射docValue部分
      weights[i] = values[i].getValueCount();// 每个segment docValue多少个独立的词,将进入SingletonSortedSetDocValues.getValueCount()
    }
    return build(owner, subs, weights, acceptableOverheadRatio);
  }

  /**
   * Creates an ordinal map that allows mapping ords to/from a merged space from <code>subs</code>.
   *
   * @param owner a cache key
   * @param subs TermsEnums that support {@link TermsEnum#ord()}. They need not be dense (e.g. can
   *     be FilteredTermsEnums}.
   * @param weights a weight for each sub. This is ideally correlated with the number of unique
   *     terms that each sub introduces compared to the other subs
   * @throws IOException if an I/O error occurred.
   */
  public static OrdinalMap build(
      IndexReader.CacheKey owner, TermsEnum[] subs, long[] weights, float acceptableOverheadRatio)
      throws IOException {
    if (subs.length != weights.length) {
      throw new IllegalArgumentException("subs and weights must have the same length");
    }

    // enums are not sorted, so let's sort to save memory
    final SegmentMap segmentMap = new SegmentMap(weights);
    return new OrdinalMap(owner, subs, segmentMap, acceptableOverheadRatio);
  }// 获取这个semgment docvalue每个term在全局的order

  private static final long BASE_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(OrdinalMap.class);

  /** Cache key of whoever asked for this awful thing */
  public final IndexReader.CacheKey owner;

  // number of global ordinals
  final long valueCount;
  // globalOrd -> (globalOrd - segmentOrd) where segmentOrd is the ordinal in the first segment
  // that contains this term
  final LongValues globalOrdDeltas;// 和firstSegments一起使用的，和global序号之间的差值
  // globalOrd -> first segment container
  final LongValues firstSegments;// 第一次出现在哪个segment上
  // for every segment, segmentOrd -> globalOrd
  final LongValues[] segmentToGlobalOrds;// 每个segment对应一个元素，保留segment内排序Id->全局排序id的映射
  // the map from/to segment ids
  final SegmentMap segmentMap; // segment从旧到新的映射（按照全局terms个数进行过排序）
  // ram usage
  final long ramBytesUsed;

  /**
   * Here is how the OrdinalMap encodes the mapping from global ords to local segment ords. Assume
   * we have the following global mapping for a doc values field: <br>
   * bar -&gt; 0, cat -&gt; 1, dog -&gt; 2, foo -&gt; 3 <br>
   * And our index is split into 2 segments with the following local mappings for that same doc
   * values field: <br>
   * Segment 0: bar -&gt; 0, foo -&gt; 1 <br>
   * Segment 1: cat -&gt; 0, dog -&gt; 1 <br>
   * We will then encode delta between the local and global mapping in a packed 2d array keyed by
   * (segmentIndex, segmentOrd). So the following 2d array will be created by OrdinalMap: <br>
   * [[0, 2], [1, 1]]
   *
   * <p>The general algorithm for creating an OrdinalMap (skipping over some implementation details
   * and optimizations) is as follows:
   *
   * <p>[1] Create and populate a PQ with ({@link TermsEnum}, index) tuples where index is the
   * position of the termEnum in an array of termEnum's sorted by descending size. The PQ itself
   * will be ordered by {@link TermsEnum#term()}
   *
   * <p>[2] We will iterate through every term in the index now. In order to do so, we will start
   * with the first term at the top of the PQ . We keep track of a global ord, and track the
   * difference between the global ord and {@link TermsEnum#ord()} in ordDeltas, which maps: <br>
   * (segmentIndex, {@link TermsEnum#ord()}) -> globalTermOrdinal - {@link TermsEnum#ord()} <br>
   * We then call {@link TermsEnum#next()} then update the PQ to iterate (remember the PQ maintains
   * and order based on {@link TermsEnum#term()} which changes on the next() calls). If the current
   * term exists in some other segment, the top of the queue will contain that segment. If not, the
   * top of the queue will contain a segment with the next term in the index and the global ord will
   * also be incremented.
   *
   * <p>[3] We use some information gathered in the previous step to perform optimizations on memory
   * usage and building time in the following steps, for more detail on those, look at the code.
   *
   * <p>[4] We will then populate segmentToGlobalOrds, which maps (segmentIndex, segmentOrd) -&gt;
   * globalOrd. Using the information we tracked in ordDeltas, we can construct this information
   * relatively easily.
   *
   * @param owner For caching purposes
   * @param subs A TermsEnum[], where each index corresponds to a segment
   * @param segmentMap Provides two maps, newToOld which lists segments in descending 'weight' order
   *     (see {@link SegmentMap} for more details) and a oldToNew map which maps each original
   *     segment index to their position in newToOld
   * @param acceptableOverheadRatio Acceptable overhead memory usage for some packed data structures
   * @throws IOException throws IOException
   */
  OrdinalMap(
      IndexReader.CacheKey owner,
      TermsEnum[] subs,
      SegmentMap segmentMap,
      float acceptableOverheadRatio)
      throws IOException {
    // create the ordinal mappings by pulling a termsenum over each sub's
    // unique terms, and walking a multitermsenum over those
    this.owner = owner;
    this.segmentMap = segmentMap;
    // even though we accept an overhead ratio, we keep these ones with COMPACT
    // since they are only used to resolve values given a global ord, which is
    // slow anyway
    PackedLongValues.Builder globalOrdDeltas =
        PackedLongValues.monotonicBuilder(PackedInts.COMPACT);
    PackedLongValues.Builder firstSegments = PackedLongValues.packedBuilder(PackedInts.COMPACT);
    long firstSegmentBits = 0L;
    final PackedLongValues.Builder[] ordDeltas = new PackedLongValues.Builder[subs.length];// 每个segment一个。实际每个segment与全局order是存在这里的
    for (int i = 0; i < ordDeltas.length; i++) { // 8个segment
      ordDeltas[i] = PackedLongValues.monotonicBuilder(acceptableOverheadRatio);
    }
    long[] ordDeltaBits = new long[subs.length];// 记录每个segment的ordDelta占几位
    long[] segmentOrds = new long[subs.length];// 与segment个数相关，每个segment已经处理的词的个数

    // Just merge-sorts by term:
    TermsEnumPriorityQueue queue = new TermsEnumPriorityQueue(subs.length);//  每个segment最小的词读取处理   多路并归读取
//每个segment里面的词都读取出来，然后排序
    for (int i = 0; i < subs.length; i++) {//几个segment，读取每个segment里面docValue的最小值(第一个值),类似n路归并排序
      TermsEnumIndex sub = new TermsEnumIndex(subs[segmentMap.newToOld(i)], i);// 从term个数从大到小的顺序排序
      if (sub.next() != null) {// 先把
        queue.add(sub);
      }
    }

    TermsEnumIndex.TermState topState = new TermsEnumIndex.TermState();

    long globalOrd = 0; // 词全局序号
    while (queue.size() != 0) {// 开始一个新的字段
      TermsEnumIndex top = queue.top();// OridinalMap$TermsEnumIndex中
      topState.copyFrom(top);

      int firstSegmentIndex = Integer.MAX_VALUE; // 这个词第一次出现在哪个segment中
      long globalOrdDelta = Long.MAX_VALUE;//这个词的全局排序的差值

      // Advance past this term, recording the per-segment ord deltas:
      while (true) {// 这次循环把这个词给读取完
        long segmentOrd = top.termsEnum.ord();// 读取到这个segment的第几个词了（编号）
        long delta = globalOrd - segmentOrd; // 这个词全局排序-当前segment内序号
        int segmentIndex = top.subIndex;// 从小到大读取排序，是第几个segment
        // We compute the least segment where the term occurs. In case the
        // first segment contains most (or better all) values, this will
        // help save significant memory
        if (segmentIndex < firstSegmentIndex) { //仅仅始终记录最小的那个segment Index，以便都往一起跑，节约内存
          firstSegmentIndex = segmentIndex;
          globalOrdDelta = delta;
        }
        ordDeltaBits[segmentIndex] |= delta;

        // for each per-segment ord, map it back to the global term; the while loop is needed
        // in case the incoming TermsEnums don't have compact ordinals (some ordinal values
        // are skipped), which can happen e.g. with a FilteredTermsEnum:
        assert segmentOrds[segmentIndex] <= segmentOrd;// 当前这个segment已经处理的order，肯定比即将来的这个orde小(等于)

        // TODO: we could specialize this case (the while loop is not needed when the ords
        // are compact)
        do {//实际上并不会循环
          ordDeltas[segmentIndex].add(delta);// 和全局的order给存储起来
          segmentOrds[segmentIndex]++;//  这个segment读取到第几个词了
        } while (segmentOrds[segmentIndex] <= segmentOrd);// 小于当前这个词的sgment内排序

        if (top.next() == null) { // 这个segment继续读取下一个term，放在对应的OridinalMap$TermsEnumIndex中
          queue.pop();// 将这个segment丢弃了
          if (queue.size() == 0) {
            break;
          }
          top = queue.top();
        } else {
          top = queue.updateTop();// 更新下这个queue的顶部
        }// 若下个词和当前词的词不一样，相同的词已经读取完了，再出现的词，肯定比这个词大。
        if (top.termEquals(topState) == false) {// 上面更新，这里使用
          break;// 当前词已经变了
        }
      }

      // for each unique term, just mark the first segment index/delta where it occurs
      firstSegments.add(firstSegmentIndex);// 针对每个unique 词，仅仅记录第一个segment的index(最小的那个segment)
      firstSegmentBits |= firstSegmentIndex;
      globalOrdDeltas.add(globalOrdDelta);// 针对每个unique 词，仅仅记录第一个segment的index(最小的那个segment)和全局的差值
      globalOrd++;
    }

    long ramBytesUsed = BASE_RAM_BYTES_USED + segmentMap.ramBytesUsed();
    this.valueCount = globalOrd;

    // If the first segment contains all of the global ords, then we can apply a small optimization
    // and hardcode the first segment indices and global ord deltas as all zeroes.
    if (ordDeltaBits.length > 0 && ordDeltaBits[0] == 0L && firstSegmentBits == 0L) {// 如果第一个segment包含全部segment，那么我们也可以优化掉这里使用
      this.firstSegments = LongValues.ZEROES;
      this.globalOrdDeltas = LongValues.ZEROES;
    } else {
      PackedLongValues packedFirstSegments = firstSegments.build(); // 上面计算仅仅为了获取ordDeltas，相当于全局排序了
      PackedLongValues packedGlobalOrdDeltas = globalOrdDeltas.build();
      this.firstSegments = packedFirstSegments;
      this.globalOrdDeltas = packedGlobalOrdDeltas;
      ramBytesUsed += packedFirstSegments.ramBytesUsed() + packedGlobalOrdDeltas.ramBytesUsed();
    }

    // ordDeltas is typically the bottleneck, so let's see what we can do to make it faster
    segmentToGlobalOrds = new LongValues[subs.length];
    ramBytesUsed += RamUsageEstimator.shallowSizeOf(segmentToGlobalOrds);
    for (int i = 0; i < ordDeltas.length; ++i) {// 遍历segment的差值
      final PackedLongValues deltas = ordDeltas[i].build();
      if (ordDeltaBits[i] == 0L) { // 全部为0，说明segment内order排序和全局排序一模一样
        // segment ords perfectly match global ordinals
        // likely in case of low cardinalities and large segments
        segmentToGlobalOrds[i] = LongValues.IDENTITY;
      } else {// 本地semgment docValue内词的排序和全局排序不一样
        final int bitsRequired =
            ordDeltaBits[i] < 0 ? 64 : PackedInts.bitsRequired(ordDeltaBits[i]);
        final long monotonicBits = deltas.ramBytesUsed() * 8;
        final long packedBits = bitsRequired * deltas.size();// 总共占用多少byte
        if (deltas.size() <= Integer.MAX_VALUE // 怎么压缩，一般都跑下面了
            && packedBits <= monotonicBits * (1 + acceptableOverheadRatio)) {
          // monotonic compression mostly adds overhead, let's keep the mapping in plain packed ints
          final int size = (int) deltas.size();
          final PackedInts.Mutable newDeltas =
              PackedInts.getMutable(size, bitsRequired, acceptableOverheadRatio);
          final PackedLongValues.Iterator it = deltas.iterator();
          for (int ord = 0; ord < size; ++ord) {// 便利每个order
            newDeltas.set(ord, it.next());
          }
          assert it.hasNext() == false;
          segmentToGlobalOrds[i] =
              new LongValues() {
                @Override
                public long get(long ord) {
                  return ord + newDeltas.get((int) ord);
                }
              };
          ramBytesUsed += newDeltas.ramBytesUsed();
        } else {// 一般跑这里
          segmentToGlobalOrds[i] =
              new LongValues() {
                @Override// 这个segment中的第几位映射到全局globaOrder的位数
                public long get(long ord) {// 得到这个segment第ord大的词，
                  return ord + deltas.get(ord); // 实际是全局第几个词
                }
              };
          ramBytesUsed += deltas.ramBytesUsed();
        }
        ramBytesUsed += RamUsageEstimator.shallowSizeOf(segmentToGlobalOrds[i]);
      }
    }

    this.ramBytesUsed = ramBytesUsed;
  }

  /**
   * Given a segment number, return a {@link LongValues} instance that maps segment ordinals to
   * global ordinals.
   */
  public LongValues getGlobalOrds(int segmentIndex) { // 获取本segment docValue的全局映射表（segment内排序Id->全局排序id的映射）
    return segmentToGlobalOrds[segmentMap.oldToNew(segmentIndex)];
  }

  /**
   * Given global ordinal, returns the ordinal of the first segment which contains this ordinal (the
   * corresponding to the segment return {@link #getFirstSegmentNumber}).
   */
  public long getFirstSegmentOrd(long globalOrd) {
    return globalOrd - globalOrdDeltas.get(globalOrd);
  }

  /** Given a global ordinal, returns the index of the first segment that contains this term. */
  public int getFirstSegmentNumber(long globalOrd) {// 获取到原始segmentId
    return segmentMap.newToOld((int) firstSegments.get(globalOrd));
  }

  /** Returns the total number of unique terms in global ord space. */
  public long getValueCount() { // 全局term的总个数
    return valueCount;
  }

  @Override
  public long ramBytesUsed() {
    return ramBytesUsed;
  }

  @Override
  public Collection<Accountable> getChildResources() {
    List<Accountable> resources = new ArrayList<>();
    resources.add(Accountables.namedAccountable("segment map", segmentMap));
    // TODO: would be nice to return the ordinal and segment maps too, but it's not straightforward
    //  because of optimizations.
    return resources;
  }
}
