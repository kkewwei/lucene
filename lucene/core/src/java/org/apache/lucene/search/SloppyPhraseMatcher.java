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
package org.apache.lucene.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import org.apache.lucene.index.FreqAndNormBuffer;
import org.apache.lucene.index.Impacts;
import org.apache.lucene.index.ImpactsSource;
import org.apache.lucene.index.Term;
import org.apache.lucene.internal.hppc.IntHashSet;
import org.apache.lucene.search.similarities.Similarity.SimScorer;
import org.apache.lucene.util.FixedBitSet;

/**
 * Find all slop-valid position-combinations (matches) encountered while traversing/hopping the
 * PhrasePositions. <br>
 * The sloppy frequency contribution of a match depends on the distance: <br>
 * - highest freq for distance=0 (exact match). <br>
 * - freq gets lower as distance gets higher. <br>
 * Example: for query "a b"~2, a document "x a b a y" can be matched twice: once for "a b"
 * (distance=0), and once for "b a" (distance=2). <br>
 * Possibly not all valid combinations are encountered, because for efficiency we always propagate
 * the least PhrasePosition. This allows to base on PriorityQueue and move forward faster. As
 * result, for example, document "a b c b a" would score differently for queries "a b c"~4 and "c b
 * a"~4, although they really are equivalent. Similarly, for doc "a b c b a f g", query "c b"~2
 * would get same score as "g f"~2, although "c b"~2 could be matched twice. We may want to fix this
 * in the future (currently not, for performance reasons).
 *
 * @lucene.internal
 */
public final class SloppyPhraseMatcher extends PhraseMatcher {

  private final PhrasePositions[] phrasePositions;

  private final int slop;
  private final int numPostings;
  private final PhraseQueue pq; // for advancing min position
  private final boolean captureLeadMatch;// 是否存储零头term的位置信息

  private final DocIdSetIterator approximation;
  private final ImpactsDISI impactsApproximation;
  // 当前短语匹配位置中误差最大的那个term的position（在文中position-传入短语term的position）
  private int end; // current largest phrase position

  private int leadPosition;
  private int leadOffset;
  private int leadEndOffset;
  private int leadOrd;
  // 查询query中，是否有重复的词
  private boolean
      hasRpts; // flag indicating that there are repetitions (as checked in first candidate doc)
  private boolean checkedRpts; // flag to only check for repetitions in first candidate doc 是否检查过重叠情况，只会处理一次
  private boolean hasMultiTermRpts; //
  private PhrasePositions[][]
      rptGroups; // in each group are PPs that repeats each other (i.e. same term), sorted by
  // (query) offset
  private PhrasePositions[] rptStack; // temporary stack for switching colliding repeating pps
  // 用来快速判断每个词分配时后面是否还有匹配的词组。
  private boolean positioned;
  private int matchLength;// 当前最合适的长度
  private boolean freqsLoaded;

  public SloppyPhraseMatcher(
      PhraseQuery.PostingsAndFreq[] postings,
      int slop,
      ScoreMode scoreMode,
      SimScorer scorer,
      float matchCost,
      boolean captureLeadMatch) {
    super(matchCost);
    this.slop = slop;
    this.numPostings = postings.length;
    this.captureLeadMatch = captureLeadMatch;
    pq = new PhraseQueue(postings.length);
    phrasePositions = new PhrasePositions[postings.length];
    for (int i = 0; i < postings.length; ++i) {
      phrasePositions[i] =
          new PhrasePositions(postings[i].postings, postings[i].position, i, postings[i].terms);
    }
    //approximation=ConjunctionDISI， 根据每个词出现的文档频率，排序了
    approximation =
        ConjunctionUtils.intersectIterators(Arrays.stream(postings).map(p -> p.postings).toList());
    // What would be a good upper bound of the sloppy frequency? A sum of the
    // sub frequencies would be correct, but it is usually so much higher than
    // the actual sloppy frequency that it doesn't help skip irrelevant
    // documents. As a consequence for now, sloppy phrase queries use dummy
    // impacts:
    final ImpactsSource impactsSource =
        new ImpactsSource() {
          @Override
          public Impacts getImpacts() throws IOException {
            return new Impacts() {

              private final FreqAndNormBuffer impactBuffer = new FreqAndNormBuffer();

              {
                impactBuffer.add(Integer.MAX_VALUE, 1);
              }

              @Override
              public int numLevels() {
                return 1;
              }

              @Override
              public FreqAndNormBuffer getImpacts(int level) {
                return impactBuffer;
              }

              @Override
              public int getDocIdUpTo(int level) {
                return DocIdSetIterator.NO_MORE_DOCS;
              }
            };
          }

          @Override
          public void advanceShallow(int target) throws IOException {}
        };
    impactsApproximation = new ImpactsDISI(approximation, new MaxScoreCache(impactsSource, scorer));
  }

  @Override
  DocIdSetIterator approximation() {
    return approximation;
  }

  @Override
  ImpactsDISI impactsApproximation() {
    return impactsApproximation;
  }

  @Override
  float maxFreq() throws IOException {
    // Load freqs eagerly so maxFreq() can be called before resetPositions() in TOP_SCORES
    // mode. PhraseScorer uses this to short-circuit non-competitive documents
    // before paying the cost of resetPositions() + initPhrasePositions().
    float maxFreq = 0;
    for (PhrasePositions phrasePosition : phrasePositions) {
      phrasePosition.freq = phrasePosition.postings.freq();
      maxFreq += phrasePosition.freq;
    }
    freqsLoaded = true;
    return maxFreq;
  }

  @Override
  public void resetPositions() throws IOException {
    if (freqsLoaded) {
      // Freqs already loaded by maxFreq().
      freqsLoaded = false;
    } else {
      // Freqs not yet loaded. Load them now.
      for (PhrasePositions phrasePosition : phrasePositions) {
        phrasePosition.freq = phrasePosition.postings.freq();
      }
    }
    this.positioned = initPhrasePositions();
    this.matchLength = Integer.MAX_VALUE;
    this.leadPosition = Integer.MAX_VALUE;
  }

  @Override
  float sloppyWeight() {
    return 1f / (1f + matchLength);
  }
  //nextMatch是找下一个匹配位置。
  @Override
  public boolean nextMatch() throws IOException {
    if (!positioned) {
      return false;
    }
    PhrasePositions pp = pq.pop(); // PhrasePos最小的出来
    assert pp != null; // if the pq is not full, then positioned == false
    captureLead(pp);
    matchLength = end - pp.position;// 当前匹配位置的匹配的长度。这里可以直接返回，只要最大的position(end)-最小的position(优先队列第一个)<=slop就可以了。
    int next = pq.top().position;  // 要保证最小的前进不能超过第二小的term，用来判断当前查找轮次的结束时机
    while (advancePP(pp)) {// 当前找到的不一定是满足slop距离的或者不是最优匹配，需要继续查找
      if (hasRpts && !advanceRpts(pp)) {
        break; // pps exhausted
      }
      if (pp.position > next) { // done minimizing current match-length   要保证最小的前进不能超过第二小的term
        pq.add(pp);// 再把pp放进队列
        if (matchLength <= slop) {//找到合适的了，那么就推出
          return true;
        }
        pp = pq.pop();// 更新下最小header
        next = pq.top().position;
        assert pp != null; // if the pq is not full, then positioned == false
        matchLength = end - pp.position;
      } else { // 选中这个，那么这个term在文档中的index就前进
        int matchLength2 = end - pp.position;
        if (matchLength2 < matchLength) {
          matchLength = matchLength2; //更新更近的距离
        }
      }
      captureLead(pp);
    }
    positioned = false;
    return matchLength <= slop;// 是否匹配了
  }
  // 存储leader的位置信息
  private void captureLead(PhrasePositions pp) throws IOException {
    if (captureLeadMatch == false) {
      return;
    }
    leadOrd = pp.ord;
    leadPosition = pp.position + pp.offset;
    leadOffset = pp.postings.startOffset();
    leadEndOffset = pp.postings.endOffset();
  }

  @Override
  public int startPosition() {
    // when a match is detected, the top postings is advanced until it has moved
    // beyond its successor, to ensure that the match is of minimal width.  This
    // means that we need to record the lead position before it is advanced.
    // However, the priority queue doesn't guarantee that the top postings is in fact the
    // earliest in the list, so we need to cycle through all terms to check.
    // this is slow, but Matches is slow anyway...
    int leadPosition = this.leadPosition;
    for (PhrasePositions pp : phrasePositions) {
      leadPosition = Math.min(leadPosition, pp.position + pp.offset);
    }
    return leadPosition;
  }

  @Override
  public int endPosition() {
    int endPosition = leadPosition;
    for (PhrasePositions pp : phrasePositions) {
      if (pp.ord != leadOrd) {
        endPosition = Math.max(endPosition, pp.position + pp.offset);
      }
    }
    return endPosition;
  }

  @Override
  public int startOffset() throws IOException {
    // when a match is detected, the top postings is advanced until it has moved
    // beyond its successor, to ensure that the match is of minimal width.  This
    // means that we need to record the lead offset before it is advanced.
    // However, the priority queue doesn't guarantee that the top postings is in fact the
    // earliest in the list, so we need to cycle through all terms to check
    // this is slow, but Matches is slow anyway...
    int leadOffset = this.leadOffset;
    for (PhrasePositions pp : phrasePositions) {
      leadOffset = Math.min(leadOffset, pp.postings.startOffset());
    }
    return leadOffset;
  }

  @Override
  public int endOffset() throws IOException {
    int endOffset = leadEndOffset;
    for (PhrasePositions pp : phrasePositions) {
      if (pp.ord != leadOrd) {
        endOffset = Math.max(endOffset, pp.postings.endOffset());
      }
    }
    return endOffset;
  }

  /** advance a PhrasePosition and update 'end', return false if exhausted */
  private boolean advancePP(PhrasePositions pp) throws IOException {
    if (!pp.nextPosition()) {// 找该term下一个position, 没找到
      return false;
    }
    if (pp.position > end) {// 跟新下最大距离
      end = pp.position;
    }
    return true;
  }

  /**
   * pp was just advanced. If that caused a repeater collision, resolve by advancing the lesser of
   * the two colliding pps. Note that there can only be one collision, as by the initialization
   * there were no collisions before pp was advanced.
   */
  private boolean advanceRpts(PhrasePositions pp) throws IOException {
    if (pp.rptGroup < 0) {// 没有重复的词。
      return true; // not a repeater
    }
    PhrasePositions[] rg = rptGroups[pp.rptGroup];
    FixedBitSet bits = new FixedBitSet(rg.length); // for re-queuing after collisions are resolved
    int k0 = pp.rptInd;
    int k;
    while ((k = collide(pp)) >= 0) {// 和pp冲突的PhrasePositions 在组中的下标
      pp = lesser(pp, rg[k]); // always advance the lesser of the (only) two colliding pps
      if (!advancePP(pp)) { // 继续往下找一个。已经找到了，那么就
        return false; // exhausted
      }// k0对应最开始的pp，还没在队列中，不用管
      if (k != k0) { // careful: mark only those currently in the queue 记录冲动的哪些需要重新进queue排队
        bits = FixedBitSet.ensureCapacity(bits, k);
        bits.set(k); // mark that pp2 need to be re-queued
      }
    }
    // collisions resolved, now re-queue
    // empty (partially) the queue until seeing all pps advanced for resolving collisions
    int n = 0;
    // TODO would be good if we can avoid calling cardinality() in each iteration!
    int numBits = bits.length(); // larges bit we set
    while (bits.cardinality() > 0) {// 对更新过PhrasePos的PhrasePositions，再重新出队再入队
      PhrasePositions pp2 = pq.pop();// 实际小堆里面的没发生变化
      rptStack[n++] = pp2;
      if (pp2.rptGroup >= 0// 如果pp2是更新过PhrasePos的，则清空位图中的标记
          && pp2.rptInd < numBits // this bit may not have been set
          && bits.get(pp2.rptInd)) {
        bits.clear(pp2.rptInd);
      }
    }
    // add back to queue
    for (int i = n - 1; i >= 0; i--) {// 重新放入队列中
      pq.add(rptStack[i]);
    }
    return true;
  }

  /** compare two pps, but only by position and offset */
  private PhrasePositions lesser(PhrasePositions pp, PhrasePositions pp2) {
    if (pp.position < pp2.position || (pp.position == pp2.position && pp.offset < pp2.offset)) {
      return pp;
    }
    return pp2;
  }

  /** index of a pp2 colliding with pp, or -1 if none */
  private int collide(PhrasePositions pp) {// 返回和pp冲突的PhrasePositions 在组中的下标
    int tpPos = tpPos(pp);
    PhrasePositions[] rg = rptGroups[pp.rptGroup];
    for (PhrasePositions pp2 : rg) {// 遍历query 同组每个term,
      if (pp2 != pp && tpPos(pp2) == tpPos) {
        return pp2.rptInd;
      }
    }
    return -1;
  }

  /**
   * Initialize PhrasePositions in place. A one time initialization for this scorer (on first doc
   * matching all terms):
   *
   * <ul>
   *   <li>Check if there are repetitions
   *   <li>If there are, find groups of repetitions.
   * </ul>
   *
   * Examples:
   *
   * <ol>
   *   <li>no repetitions: <b>"ho my"~2</b>
   *   <li>repetitions: <b>"ho my my"~2</b>
   *   <li>repetitions: <b>"my ho my"~2</b>
   * </ol>
   *
   * @return false if PPs are exhausted (and so current doc will not be a match)
   */
  private boolean initPhrasePositions() throws IOException {
    end = Integer.MIN_VALUE;
    if (!checkedRpts) {
      return initFirstTime();
    }
    if (!hasRpts) {// 还没检查过是否有重叠的term，则需要进行一次检查，只会执行一次
      initSimple();
      return true; // PPs available
    }
    return initComplex();
  }

  /**
   * no repeats: simplest case, and most common. It is important to keep this piece of the code
   * simple and efficient
   */
  private void initSimple() throws IOException {
    // System.err.println("initSimple: doc: "+min.doc);
    pq.clear();
    // position pps and build queue from list
    for (PhrasePositions pp : phrasePositions) {
      pp.firstPosition();
      if (pp.position > end) {
        end = pp.position;
      }
      pq.add(pp);
    }
  }

  /** with repeats: not so simple. */
  private boolean initComplex() throws IOException {
    // System.err.println("initComplex: doc: "+min.doc);
    placeFirstPositions();
    if (!advanceRepeatGroups()) {
      return false; // PPs exhausted
    }
    fillQueue();
    return true; // PPs available
  }

  /** move all PPs to their first position */
  private void placeFirstPositions() throws IOException {
    for (PhrasePositions pp : phrasePositions) {
      pp.firstPosition();
    }
  }

  /** Fill the queue (all pps are already placed */
  private void fillQueue() {//  将phrasePositions全部转移到qp优先级队列中
    pq.clear();
    for (PhrasePositions pp : phrasePositions) { // iterate cyclic list: done once handled max
      if (pp.position > end) {
        end = pp.position; //当前误差最大的那个值
      }
      pq.add(pp);
    }
  }

  /**
   * At initialization (each doc), each repetition group is sorted by (query) offset. This provides
   * the start condition: no collisions.
   *
   * <p>Case 1: no multi-term repeats<br>
   * It is sufficient to advance each pp in the group by one less than its group index. So lesser pp
   * is not advanced, 2nd one advance once, 3rd one advanced twice, etc.
   *
   * <p>Case 2: multi-term repeats<br>
   *
   * @return false if PPs are exhausted.
   */
  private boolean advanceRepeatGroups() throws IOException {// 需要对有重复的term单独处理下，使得每个倒排都定位到唯一的位置
    for (PhrasePositions[] rg : rptGroups) {
      if (hasMultiTermRpts) {
        // more involved, some may not collide
        int incr;
        for (int i = 0; i < rg.length; i += incr) {
          incr = 1;
          PhrasePositions pp = rg[i];
          int k;
          while ((k = collide(pp)) >= 0) {
            PhrasePositions pp2 = lesser(pp, rg[k]);
            if (!advancePP(pp2)) { // at initialization always advance pp with higher offset
              return false; // exhausted
            }
            if (pp2.rptInd < i) { // should not happen?
              incr = 0;
              break;
            }
          }
        }
      } else {
        // simpler, we know exactly how much to advance
        for (int j = 1; j < rg.length; j++) {// 从第二个词开始，每个词的往后延后
          for (int k=0; k<j; k++) { //若是第3个重复的词，那么给他分配可用词的话，就往后多分配几次。比如针对a, 第一个a占用第一个a的index，第二个a占用第二个a的index。
            if (!rg[j].nextPosition()) {
              return false; // PPs exhausted
            }
          }
        }
      }
    }
    return true; // PPs available
  }

  /**
   * initialize with checking for repeats. Heavy work, but done only for the first candidate doc.
   *
   * <p>If there are repetitions, check if multi-term postings (MTP) are involved.
   *
   * <p>Without MTP, once PPs are placed in the first candidate doc, repeats (and groups) are
   * visible.<br>
   * With MTP, a more complex check is needed, up-front, as there may be "hidden collisions".<br>
   * For example P1 has {A,B}, P1 has {B,C}, and the first doc is: "A C B". At start, P1 would point
   * to "A", p2 to "C", and it will not be identified that P1 and P2 are repetitions of each other.
   *
   * <p>The more complex initialization has two parts:<br>
   * (1) identification of repetition groups.<br>
   * (2) advancing repeat groups at the start of the doc.<br>
   * For (1), a possible solution is to just create a single repetition group, made of all repeating
   * pps. But this would slow down the check for collisions, as all pps would need to be checked.
   * Instead, we compute "connected regions" on the bipartite graph of postings and terms.
   */
  private boolean initFirstTime() throws IOException {
    // System.err.println("initFirstTime: doc: "+min.doc);
    checkedRpts = true;
    placeFirstPositions();
// 查找重复的term，value是重复term出现的编号，从0开始
    LinkedHashMap<Term, Integer> rptTerms = repeatingTerms();
    hasRpts = !rptTerms.isEmpty();

    if (hasRpts) {// 词有重复的
      rptStack = new PhrasePositions[numPostings]; // needed with repetitions
      ArrayList<ArrayList<PhrasePositions>> rgs = gatherRptGroups(rptTerms);// 根据重复的term进行分组
      sortRptGroups(rgs);
      if (!advanceRepeatGroups()) { // 每个重复词，都给他分配了一个位置
        return false; // PPs exhausted
      }
    }
//将phrasePositions全部转移到qp优先级队列中
    fillQueue();
    return true; // PPs available
  }

  /**
   * sort each repetition group by (query) offset. Done only once (at first doc) and allows to
   * initialize faster for each doc.
   */
  private void sortRptGroups(ArrayList<ArrayList<PhrasePositions>> rgs) {// 查询query中相同term的多个位置排序+编号
    rptGroups = new PhrasePositions[rgs.size()][];
    Comparator<PhrasePositions> cmprtr = Comparator.comparingInt(pp -> pp.offset);//term的offset排序
    for (int i = 0; i < rptGroups.length; i++) {
      PhrasePositions[] rg = rgs.get(i).toArray(PhrasePositions[]::new);
      Arrays.sort(rg, cmprtr);
      rptGroups[i] = rg;
      for (int j = 0; j < rg.length; j++) { // 相同的每个term都分配一个下标
        rg[j].rptInd = j; // we use this index for efficient re-queuing
      }   // 设置 PhrasePositions 在组中的下标
    }
  }
  // 根据重复的term进行分组
  /** Detect repetition groups. Done once - for first doc */
  private ArrayList<ArrayList<PhrasePositions>> gatherRptGroups(
      LinkedHashMap<Term, Integer> rptTerms) throws IOException {
    PhrasePositions[] rpp = repeatingPPs(rptTerms); // 哪几个词是重读的
    ArrayList<ArrayList<PhrasePositions>> res = new ArrayList<>();
    if (!hasMultiTermRpts) {
      // simpler - no multi-terms - can base on positions in first doc
      for (int i = 0; i < rpp.length; i++) {// 对所有query 语句中重复的term遍历
        PhrasePositions pp = rpp[i];
        if (pp.rptGroup >=0) continue; // already marked as a repetition  // 单term的情况下，如果有重复，最多需要处理一次。所以已经标记为重复的了，不需要再处理。
        int tpPos = tpPos(pp);// 获取是这个term在文档中的真实位置
        for (int j=i+1; j<rpp.length; j++) { // 找后续重复的每个词
          PhrasePositions pp2 = rpp[j];
          if (pp2.rptGroup >= 0 // already marked as a repetition
              || pp2.offset == pp.offset // not a repetition: two PPs are originally in same offset
              || tpPos(pp2) != tpPos) { // not a repetition query确定的不是同一个词
            continue;
          }
          // a repetition
          int g = pp.rptGroup; //查询语句中重复的term
          if (g < 0) { // 还没有分过组
            g = res.size();
            pp.rptGroup = g;
            ArrayList<PhrasePositions> rl = new ArrayList<>(2);
            rl.add(pp);
            res.add(rl);
          }
          pp2.rptGroup = g;
          res.get(g).add(pp2);
        }
      }
    } else {
      // more involved - has multi-terms
      ArrayList<HashSet<PhrasePositions>> tmp = new ArrayList<>();
      ArrayList<FixedBitSet> bb = ppTermsBitSets(rpp, rptTerms);
      unionTermGroups(bb);
      HashMap<Term, Integer> tg = termGroups(rptTerms, bb);
      int numDistinctGroupIds = new IntHashSet(tg.values()).size();
      for (int i = 0; i < numDistinctGroupIds; i++) {
        tmp.add(new HashSet<>());
      }
      for (PhrasePositions pp : rpp) {
        for (Term t : pp.terms) {
          if (rptTerms.containsKey(t)) {
            int g = tg.get(t);
            tmp.get(g).add(pp);
            assert pp.rptGroup == -1 || pp.rptGroup == g;
            pp.rptGroup = g;
          }
        }
      }
      for (HashSet<PhrasePositions> hs : tmp) {
        res.add(new ArrayList<>(hs));
      }
    }
    return res;
  }

  /** Actual position in doc of a PhrasePosition, relies on that position = tpPos - offset */
  private int tpPos(PhrasePositions pp) {
    return pp.position + pp.offset;
  }

  /** find repeating terms and assign them ordinal values */
  private LinkedHashMap<Term, Integer> repeatingTerms() {// 查看待查询的词中是否与重复出现的词
    LinkedHashMap<Term, Integer> tord = new LinkedHashMap<>();
    HashMap<Term, Integer> tcnt = new HashMap<>();
    for (PhrasePositions pp : phrasePositions) {
      for (Term t : pp.terms) {
        Integer cnt = tcnt.compute(t, (key, old) -> old == null ? 1 : 1 + old);
        if (cnt == 2) {// 仅统计第二次出现的相同的term。只要统计第二个，就知道哪个重复了
          tord.put(t, tord.size());
        }
      }
    }
    return tord;
  }

  /** find repeating pps, and for each, if has multi-terms, update this.hasMultiTermRpts */
  private PhrasePositions[] repeatingPPs(HashMap<Term, Integer> rptTerms) {
    ArrayList<PhrasePositions> rp = new ArrayList<>();
    for (PhrasePositions pp : phrasePositions) {
      for (Term t : pp.terms) {
        if (rptTerms.containsKey(t)) {
          rp.add(pp);
          hasMultiTermRpts |= (pp.terms.length > 1);// 如果PhrasePositions中有多个term，则设置hasMultiTermRpts
          break;
        }
      }
    }
    return rp.toArray(PhrasePositions[]::new);
  }

  /**
   * bit-sets - for each repeating pp, for each of its repeating terms, the term ordinal values is
   * set
   */
  private ArrayList<FixedBitSet> ppTermsBitSets(
      PhrasePositions[] rpp, HashMap<Term, Integer> tord) {
    ArrayList<FixedBitSet> bb = new ArrayList<>(rpp.length);
    for (PhrasePositions pp : rpp) {
      FixedBitSet b = new FixedBitSet(tord.size());
      Integer ord;
      for (Term t : pp.terms) {
        if ((ord = tord.get(t)) != null) {
          b.set(ord);
        }
      }
      bb.add(b);
    }
    return bb;
  }

  /**
   * union (term group) bit-sets until they are disjoint (O(n^^2)), and each group have different
   * terms
   */
  private void unionTermGroups(ArrayList<FixedBitSet> bb) {
    int incr;
    for (int i = 0; i < bb.size() - 1; i += incr) {
      incr = 1;
      int j = i + 1;
      while (j < bb.size()) {
        if (bb.get(i).intersects(bb.get(j))) {
          bb.get(i).or(bb.get(j));
          bb.remove(j);
          incr = 0;
        } else {
          ++j;
        }
      }
    }
  }

  /** map each term to the single group that contains it */
  private HashMap<Term, Integer> termGroups(
      LinkedHashMap<Term, Integer> tord, ArrayList<FixedBitSet> bb) throws IOException {
    HashMap<Term, Integer> tg = new HashMap<>();
    Term[] t = tord.keySet().toArray(Term[]::new);
    for (int i = 0; i < bb.size(); i++) { // i is the group no.
      FixedBitSet bits = bb.get(i);
      for (int ord = bits.nextSetBit(0);
          ord != DocIdSetIterator.NO_MORE_DOCS;
          ord =
              ord + 1 >= bits.length() ? DocIdSetIterator.NO_MORE_DOCS : bits.nextSetBit(ord + 1)) {
        tg.put(t[ord], i);
      }
    }
    return tg;
  }
}
