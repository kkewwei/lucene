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
package org.apache.lucene.codecs.blocktree;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.FutureArrays;
import org.apache.lucene.util.FutureObjects;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.compress.LZ4;
import org.apache.lucene.util.compress.LowercaseAsciiCompression;
import org.apache.lucene.util.fst.Builder;
import org.apache.lucene.util.fst.ByteSequenceOutputs;
import org.apache.lucene.util.fst.BytesRefFSTEnum;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.Util;

/*
  TODO:
  
    - Currently there is a one-to-one mapping of indexed
      term to term block, but we could decouple the two, ie,
      put more terms into the index than there are blocks.
      The index would take up more RAM but then it'd be able
      to avoid seeking more often and could make PK/FuzzyQ
      faster if the additional indexed terms could store
      the offset into the terms block.

    - The blocks are not written in true depth-first
      order, meaning if you just next() the file pointer will
      sometimes jump backwards.  For example, block foo* will
      be written before block f* because it finished before.
      This could possibly hurt performance if the terms dict is
      not hot, since OSs anticipate sequential file access.  We
      could fix the writer to re-order the blocks as a 2nd
      pass.

    - Each block encodes the term suffixes packed
      sequentially using a separate vInt per term, which is
      1) wasteful and 2) slow (must linear scan to find a
      particular suffix).  We should instead 1) make
      random-access array so we can directly access the Nth
      suffix, and 2) bulk-encode this array using bulk int[]
      codecs; then at search time we can binary search when
      we seek a particular term.
*/

/**
 * Block-based terms index and dictionary writer.
 * <p>
 * Writes terms dict and index, block-encoding (column
 * stride) each term's metadata for each set of terms
 * between two index terms.
 * <p>
 *
 * Files:
 * <ul>
 *   <li><tt>.tim</tt>: <a href="#Termdictionary">Term Dictionary</a></li>
 *   <li><tt>.tip</tt>: <a href="#Termindex">Term Index</a></li>
 * </ul>
 * <p>
 * <a name="Termdictionary"></a>
 * <h3>Term Dictionary</h3>
 *
 * <p>The .tim file contains the list of terms in each
 * field along with per-term statistics (such as docfreq)
 * and per-term metadata (typically pointers to the postings list
 * for that term in the inverted index).
 * </p>
 *
 * <p>The .tim is arranged in blocks: with blocks containing
 * a variable number of entries (by default 25-48), where
 * each entry is either a term or a reference to a
 * sub-block.</p>
 *
 * <p>NOTE: The term dictionary can plug into different postings implementations:
 * the postings writer/reader are actually responsible for encoding 
 * and decoding the Postings Metadata and Term Metadata sections.</p>
 *
 * <ul>
 *    <li>TermsDict (.tim) --&gt; Header, <i>PostingsHeader</i>, NodeBlock<sup>NumBlocks</sup>,
 *                               FieldSummary, DirOffset, Footer</li>
 *    <li>NodeBlock --&gt; (OuterNode | InnerNode)</li>
 *    <li>OuterNode --&gt; EntryCount, SuffixLength, Byte<sup>SuffixLength</sup>, StatsLength, &lt; TermStats &gt;<sup>EntryCount</sup>, MetaLength, &lt;<i>TermMetadata</i>&gt;<sup>EntryCount</sup></li>
 *    <li>InnerNode --&gt; EntryCount, SuffixLength[,Sub?], Byte<sup>SuffixLength</sup>, StatsLength, &lt; TermStats ? &gt;<sup>EntryCount</sup>, MetaLength, &lt;<i>TermMetadata ? </i>&gt;<sup>EntryCount</sup></li>
 *    <li>TermStats --&gt; DocFreq, TotalTermFreq </li>
 *    <li>FieldSummary --&gt; NumFields, &lt;FieldNumber, NumTerms, RootCodeLength, Byte<sup>RootCodeLength</sup>,
 *                            SumTotalTermFreq?, SumDocFreq, DocCount, LongsSize, MinTerm, MaxTerm&gt;<sup>NumFields</sup></li>
 *    <li>Header --&gt; {@link CodecUtil#writeHeader CodecHeader}</li>
 *    <li>DirOffset --&gt; {@link DataOutput#writeLong Uint64}</li>
 *    <li>MinTerm,MaxTerm --&gt; {@link DataOutput#writeVInt VInt} length followed by the byte[]</li>
 *    <li>EntryCount,SuffixLength,StatsLength,DocFreq,MetaLength,NumFields,
 *        FieldNumber,RootCodeLength,DocCount,LongsSize --&gt; {@link DataOutput#writeVInt VInt}</li>
 *    <li>TotalTermFreq,NumTerms,SumTotalTermFreq,SumDocFreq --&gt; 
 *        {@link DataOutput#writeVLong VLong}</li>
 *    <li>Footer --&gt; {@link CodecUtil#writeFooter CodecFooter}</li>
 * </ul>
 * <p>Notes:</p>
 * <ul>
 *    <li>Header is a {@link CodecUtil#writeHeader CodecHeader} storing the version information
 *        for the BlockTree implementation.</li>
 *    <li>DirOffset is a pointer to the FieldSummary section.</li>
 *    <li>DocFreq is the count of documents which contain the term.</li>
 *    <li>TotalTermFreq is the total number of occurrences of the term. This is encoded
 *        as the difference between the total number of occurrences and the DocFreq.</li>
 *    <li>FieldNumber is the fields number from {@link FieldInfos}. (.fnm)</li>
 *    <li>NumTerms is the number of unique terms for the field.</li>
 *    <li>RootCode points to the root block for the field.</li>
 *    <li>SumDocFreq is the total number of postings, the number of term-document pairs across
 *        the entire field.</li>
 *    <li>DocCount is the number of documents that have at least one posting for this field.</li>
 *    <li>LongsSize records how many long values the postings writer/reader record per term
 *        (e.g., to hold freq/prox/doc file offsets).
 *    <li>MinTerm, MaxTerm are the lowest and highest term in this field.</li>
 *    <li>PostingsHeader and TermMetadata are plugged into by the specific postings implementation:
 *        these contain arbitrary per-file data (such as parameters or versioning information) 
 *        and per-term data (such as pointers to inverted files).</li>
 *    <li>For inner nodes of the tree, every entry will steal one bit to mark whether it points
 *        to child nodes(sub-block). If so, the corresponding TermStats and TermMetaData are omitted </li>
 * </ul>
 * <a name="Termindex"></a>
 * <h3>Term Index</h3>
 * <p>The .tip file contains an index into the term dictionary, so that it can be 
 * accessed randomly.  The index is also used to determine
 * when a given term cannot exist on disk (in the .tim file), saving a disk seek.</p>
 * <ul>
 *   <li>TermsIndex (.tip) --&gt; Header, FSTIndex<sup>NumFields</sup>
 *                                &lt;IndexStartFP&gt;<sup>NumFields</sup>, DirOffset, Footer</li>
 *   <li>Header --&gt; {@link CodecUtil#writeHeader CodecHeader}</li>
 *   <li>DirOffset --&gt; {@link DataOutput#writeLong Uint64}</li>
 *   <li>IndexStartFP --&gt; {@link DataOutput#writeVLong VLong}</li>
 *   <!-- TODO: better describe FST output here -->
 *   <li>FSTIndex --&gt; {@link FST FST&lt;byte[]&gt;}</li>
 *   <li>Footer --&gt; {@link CodecUtil#writeFooter CodecFooter}</li>
 * </ul>
 * <p>Notes:</p>
 * <ul>
 *   <li>The .tip file contains a separate FST for each
 *       field.  The FST maps a term prefix to the on-disk
 *       block that holds all terms starting with that
 *       prefix.  Each field's IndexStartFP points to its
 *       FST.</li>
 *   <li>DirOffset is a pointer to the start of the IndexStartFPs
 *       for all fields</li>
 *   <li>It's possible that an on-disk block would contain
 *       too many terms (more than the allowed maximum
 *       (default: 48)).  When this happens, the block is
 *       sub-divided into new blocks (called "floor
 *       blocks"), and then the output in the FST for the
 *       block's prefix encodes the leading byte of each
 *       sub-block, and its file pointer.
 * </ul>
 *
 * @see BlockTreeTermsReader
 * @lucene.experimental
 */ // 倒排索引对应的Codec，其中倒排表部分使用Lucene50PostingsWriter(Block方式写入倒排链)和Lucene50SkipWriter(对Block的SkipList索引)，词典部分则是使用FST（针对倒排表Block级的词典索引）
public final class BlockTreeTermsWriter extends FieldsConsumer {

  /** Suggested default value for the {@code
   *  minItemsInBlock} parameter to {@link
   *  #BlockTreeTermsWriter(SegmentWriteState,PostingsWriterBase,int,int)}. */
  public final static int DEFAULT_MIN_BLOCK_SIZE = 25;

  /** Suggested default value for the {@code
   *  maxItemsInBlock} parameter to {@link
   *  #BlockTreeTermsWriter(SegmentWriteState,PostingsWriterBase,int,int)}. */
  public final static int DEFAULT_MAX_BLOCK_SIZE = 48;

  //public static boolean DEBUG = false;
  //public static boolean DEBUG2 = false;

  //private final static boolean SAVE_DOT_FILES = false;

  private final IndexOutput metaOut;
  private final IndexOutput termsOut; // Term词典    tim文件
  private final IndexOutput indexOut; // tip  指向Term词典的索引
  final int maxDoc; // 当前待刷新的segment内最大文档个数
  final int minItemsInBlock; // 25
  final int maxItemsInBlock; // 48

  final PostingsWriterBase postingsWriter; // Lucene84PostingsWriter（里面是构建doc、pos、pay文件的地方）
  final FieldInfos fieldInfos;

  private final List<ByteBuffersDataOutput> fields = new ArrayList<>();// 将一个域的fst写入tip文件后，就会再放入这里

  /** Create a new writer.  The number of items (terms or
   *  sub-blocks) per block will aim to be between
   *  minItemsPerBlock and maxItemsPerBlock, though in some
   *  cases the blocks may be smaller than the min. */
  public BlockTreeTermsWriter(SegmentWriteState state,
                              PostingsWriterBase postingsWriter,
                              int minItemsInBlock,
                              int maxItemsInBlock)
    throws IOException
  {
    validateSettings(minItemsInBlock,
                     maxItemsInBlock);

    this.minItemsInBlock = minItemsInBlock; // 25
    this.maxItemsInBlock = maxItemsInBlock; // 48

    this.maxDoc = state.segmentInfo.maxDoc();
    this.fieldInfos = state.fieldInfos;
    this.postingsWriter = postingsWriter;  //Lucene50PostingsWriter
    // _1_Lucene50_0.tim
    final String termsName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, BlockTreeTermsReader.TERMS_EXTENSION);
    termsOut = state.directory.createOutput(termsName, state.context);
    boolean success = false;
    IndexOutput metaOut = null, indexOut = null;
    try {
      CodecUtil.writeIndexHeader(termsOut, BlockTreeTermsReader.TERMS_CODEC_NAME, BlockTreeTermsReader.VERSION_CURRENT,
                                 state.segmentInfo.getId(), state.segmentSuffix);
       // _1_Lucene50_0.tip
      final String indexName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, BlockTreeTermsReader.TERMS_INDEX_EXTENSION);
      indexOut = state.directory.createOutput(indexName, state.context);
      CodecUtil.writeIndexHeader(indexOut, BlockTreeTermsReader.TERMS_INDEX_CODEC_NAME, BlockTreeTermsReader.VERSION_CURRENT,
                                 state.segmentInfo.getId(), state.segmentSuffix);
//      segment = state.segmentInfo.name;
      // metaName = _n_Lucene84_0.tmd
      final String metaName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, BlockTreeTermsReader.TERMS_META_EXTENSION);
      metaOut = state.directory.createOutput(metaName, state.context);
      CodecUtil.writeIndexHeader(metaOut, BlockTreeTermsReader.TERMS_META_CODEC_NAME, BlockTreeTermsReader.VERSION_CURRENT,
          state.segmentInfo.getId(), state.segmentSuffix);

      postingsWriter.init(metaOut, state);                          // have consumer write its format/header

      this.metaOut = metaOut;
      this.indexOut = indexOut;
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(metaOut, termsOut, indexOut);
      }
    }
  }

  /** Throws {@code IllegalArgumentException} if any of these settings
   *  is invalid. */
  public static void validateSettings(int minItemsInBlock, int maxItemsInBlock) {
    if (minItemsInBlock <= 1) {
      throw new IllegalArgumentException("minItemsInBlock must be >= 2; got " + minItemsInBlock);
    }
    if (minItemsInBlock > maxItemsInBlock) {
      throw new IllegalArgumentException("maxItemsInBlock must be >= minItemsInBlock; got maxItemsInBlock=" + maxItemsInBlock + " minItemsInBlock=" + minItemsInBlock);
    }
    if (2*(minItemsInBlock-1) > maxItemsInBlock) {
      throw new IllegalArgumentException("maxItemsInBlock must be at least 2*(minItemsInBlock-1); got maxItemsInBlock=" + maxItemsInBlock + " minItemsInBlock=" + minItemsInBlock);
    }
  }

  @Override
  public void write(Fields fields, NormsProducer norms) throws IOException {
    //if (DEBUG) System.out.println("\nBTTW.write seg=" + segment);

    String lastField = null;
    for(String field : fields) { // // 遍历需要建立索引的field
      assert lastField == null || lastField.compareTo(field) < 0;
      lastField = field;

      //if (DEBUG) System.out.println("\nBTTW.write seg=" + segment + " field=" + field);
      Terms terms = fields.terms(field); //  FreqProxFields$FreqProxTerms
      if (terms == null) {
        continue;
      }

      TermsEnum termsEnum = terms.iterator(); // 遍历FreqProxTermsWriterPerField里面每个termId使用的
      TermsWriter termsWriter = new TermsWriter(fieldInfos.fieldInfo(field)); // 一个域单独产生一个
      while (true) { //遍历这个field下每个词
        BytesRef term = termsEnum.next(); // 读取这个词的具体内容
        //if (DEBUG) System.out.println("BTTW: next term " + term);

        if (term == null) {
          break;
        }

        //if (DEBUG) System.out.println("write field=" + fieldInfo.name + " term=" + brToString(term));
        termsWriter.write(term, termsEnum, norms); // 对doc,tim等文件的构建
      }

      termsWriter.finish();// 完成field 的构建。每个单词一个finish。将pending中剩余的全部打包起来

      //if (DEBUG) System.out.println("\nBTTW.write done seg=" + segment + " field=" + field);
    }
  }
  
  static long encodeOutput(long fp, boolean hasTerms, boolean isFloor) {
    assert fp < (1L << 62);
    return (fp << 2) | (hasTerms ? BlockTreeTermsReader.OUTPUT_FLAG_HAS_TERMS : 0) | (isFloor ? BlockTreeTermsReader.OUTPUT_FLAG_IS_FLOOR : 0);
  } // 最高62为存放tim起始位置，低一位存放是否有terms，最低位存放是否是floor

  private static class PendingEntry {
    public final boolean isTerm; // PendingTerm默认是true, PendingBlock默认是false

    protected PendingEntry(boolean isTerm) {
      this.isTerm = isTerm;
    }
  }

  private static final class PendingTerm extends PendingEntry {
    public final byte[] termBytes; // 当前词
    // stats + metadata
    public final BlockTermState state;

    public PendingTerm(BytesRef term, BlockTermState state) {
      super(true);
      this.termBytes = new byte[term.length];
      System.arraycopy(term.bytes, term.offset, termBytes, 0, term.length);
      this.state = state;
    }

    @Override
    public String toString() {
      return "TERM: " + brToString(termBytes);
    }
  }

  // for debugging
  @SuppressWarnings("unused")
  static String brToString(BytesRef b) {
    if (b == null) {
      return "(null)";
    } else {
      try {
        return b.utf8ToString() + " " + b;
      } catch (Throwable t) {
        // If BytesRef isn't actually UTF8, or it's eg a
        // prefix of UTF8 that ends mid-unicode-char, we
        // fallback to hex:
        return b.toString();
      }
    }
  }

  // for debugging
  @SuppressWarnings("unused")
  static String brToString(byte[] b) {
    return brToString(new BytesRef(b));
  }

  private static final class PendingBlock extends PendingEntry {
    public final BytesRef prefix; // 作为整个term，来参与字典的创建，整个没有相同的前缀。就是block0的term
    public final long fp;  // 获取当前block在tim中的起始位置
    public FST<BytesRef> index; // 产生的就放这里
    public List<FST<BytesRef>> subIndices; // 这个block里面的子fst
    public final boolean hasTerms;
    public final boolean isFloor; // 该
    public final int floorLeadByte; //// floorLeadLabel除第一个是-1，其他bloc时都是和上一个block不同的那个字母

    public PendingBlock(BytesRef prefix, long fp, boolean hasTerms, boolean isFloor, int floorLeadByte, List<FST<BytesRef>> subIndices) {
      super(false);
      this.prefix = prefix;
      this.fp = fp; // 获取当前block在tim中的起始位置
      this.hasTerms = hasTerms;
      this.isFloor = isFloor;
      this.floorLeadByte = floorLeadByte; // floorLeadLabel除第一个是-1，其他bloc时都是和上一个block不同的那个字母
      this.subIndices = subIndices;
    }

    @Override
    public String toString() {
      return "BLOCK: prefix=" + brToString(prefix);
    }
    // 所有block一起处理。scratchBytes进来时是空的，作为output使用
    public void compileIndex(List<PendingBlock> blocks, RAMOutputStream scratchBytes, IntsRefBuilder scratchIntsRef) throws IOException {

      assert (isFloor && blocks.size() > 1) || (isFloor == false && blocks.size() == 1): "isFloor=" + isFloor + " blocks=" + blocks;
      assert this == blocks.get(0);

      assert scratchBytes.getFilePointer() == 0; // 没有写任何数据

      // TODO: try writing the leading vLong in MSB order
      // (opposite of what Lucene does today), for better
      // outputs sharing in the FST
      scratchBytes.writeVLong(encodeOutput(fp, hasTerms, isFloor)); //写入scratchBytes：最高62为存放tim起始位置，低一位存放是否有terms，最低位存放是否是floor
      if (isFloor) {
        scratchBytes.writeVInt(blocks.size()-1); // block个剩余数
        for (int i=1;i<blocks.size();i++) { // 遍历每一个block，把当前block当做一次output
          PendingBlock sub = blocks.get(i); // 第二个
          assert sub.floorLeadByte != -1; // 只有第一个block该值为-1
          //if (DEBUG) {
          //  System.out.println("    write floorLeadByte=" + Integer.toHexString(sub.floorLeadByte&0xff));
          //}
          scratchBytes.writeByte((byte) sub.floorLeadByte); // 下一个block与上个block相比，不同的字符
          assert sub.fp > fp;// 两个block在tim中的length
          scratchBytes.writeVLong((sub.fp - fp) << 1 | (sub.hasTerms ? 1 : 0));
        }
      }
      // 这里会新产生以Builder
      final ByteSequenceOutputs outputs = ByteSequenceOutputs.getSingleton();
      final Builder<BytesRef> indexBuilder = new Builder<>(FST.INPUT_TYPE.BYTE1,
                                                           0, 0, true, false, Integer.MAX_VALUE,
                                                           outputs, true, 15);
      //if (DEBUG) {
      //  System.out.println("  compile index for prefix=" + prefix);
      //}
      //indexBuilder.DEBUG = false;
      final byte[] bytes = new byte[(int) scratchBytes.getFilePointer()]; // scratchBytes里面的长度。存的是fp&hasTerms&isFloor
      assert bytes.length > 0;
      scratchBytes.writeTo(bytes, 0); // 把scratchBytes写入bytes中, output
      indexBuilder.add(Util.toIntsRef(prefix, scratchIntsRef), new BytesRef(bytes, 0, bytes.length)); // 把本身节点装进去，prefix可为""
      scratchBytes.reset(); // 用完就清空
      // 将 sub-block 的所有 index 写入indexBuilder(比较重要)
      // Copy over index for all sub-blocks
      for(PendingBlock block : blocks) {
        if (block.subIndices != null) { // 遍历所有的block。只有子block有fst，才会加入
          for(FST<BytesRef> subIndex : block.subIndices) {  // 遍历子fst
            append(indexBuilder, subIndex, scratchIntsRef); // 当做普通字符串再加入新的fst中
          }
          block.subIndices = null;
        }
      }
      // 生成新的FST
      index = indexBuilder.finish();

      assert subIndices == null;

      /*
      Writer w = new OutputStreamWriter(new FileOutputStream("out.dot"));
      Util.toDot(index, w, false, false);
      System.out.println("SAVED to out.dot");
      w.close();
      */
    }

    // TODO: maybe we could add bulk-add method to
    // Builder?  Takes FST and unions it w/ current
    // FST.
    private void append(Builder<BytesRef> builder, FST<BytesRef> subIndex, IntsRefBuilder scratchIntsRef) throws IOException {
      final BytesRefFSTEnum<BytesRef> subIndexEnum = new BytesRefFSTEnum<>(subIndex);
      BytesRefFSTEnum.InputOutput<BytesRef> indexEnt;
      while((indexEnt = subIndexEnum.next()) != null) {
        //if (DEBUG) {
        //  System.out.println("      add sub=" + indexEnt.input + " " + indexEnt.input + " output=" + indexEnt.output);
        //}
        builder.add(Util.toIntsRef(indexEnt.input, scratchIntsRef), indexEnt.output);
      }
    }
  }

  private final RAMOutputStream scratchBytes = new RAMOutputStream(); // 每次写入一个term都是一个新的
  private final IntsRefBuilder scratchIntsRef = new IntsRefBuilder(); // 每次写入一个都是一个新的

  static final BytesRef EMPTY_BYTES_REF = new BytesRef();

  private static class StatsWriter {

    private final DataOutput out;
    private final boolean hasFreqs;
    private int singletonCount;

    StatsWriter(DataOutput out, boolean hasFreqs) {
      this.out = out;
      this.hasFreqs = hasFreqs;
    }

    void add(int df, long ttf) throws IOException {
      // Singletons (DF==1, TTF==1) are run-length encoded
      if (df == 1 && (hasFreqs == false || ttf == 1)) {
        singletonCount++;
      } else {
        finish();
        out.writeVInt(df << 1);
        if (hasFreqs) {
          out.writeVLong(ttf - df);
        }
      }
    }

    void finish() throws IOException {
      if (singletonCount > 0) {
        out.writeVInt(((singletonCount - 1) << 1) | 1);
        singletonCount = 0;
      }
    }

  }
  // 一个域单独拥有一个
  class TermsWriter {
    private final FieldInfo fieldInfo;
    private long numTerms;
    final FixedBitSet docsSeen; // 在flush阶段从解析到文档中读取出来，作为可查询的文档
    long sumTotalTermFreq;
    long sumDocFreq;

    // Records index into pending where the current prefix at that
    // length "started"; for example, if current term starts with 't',
    // startsByPrefix[0] is the index into pending for the first
    // term/sub-block starting with 't'.  We use this to figure out when
    // to write a new block:
    private final BytesRefBuilder lastTerm = new BytesRefBuilder();
    private int[] prefixStarts = new int[8];// 统计的是后一个元素与前一个元素的相似性情况。若元素数值越小，则该位前缀越相似。prefixStarts 长度会扩容的

    // Pending stack of terms and blocks.  As terms arrive (in sorted order)
    // we append to this stack, and once the top of the stack has enough
    // terms starting with a common prefix, we write a new block with
    // those terms and replace those terms in the stack with a new block:
    private final List<PendingEntry> pending = new ArrayList<>(); //待索引词典列表

    // Reused in writeBlocks:
    private final List<PendingBlock> newBlocks = new ArrayList<>();

    private PendingTerm firstPendingTerm; // 这个域写入的第一个词
    private PendingTerm lastPendingTerm;  // 这个域写入的最后一个词
    // 将最后prefixTopSize产生一个fst，放入PendingBlock中，然后再放入pending中
    /** Writes the top count entries in pending, using prevTerm to compute the prefix. */
    void writeBlocks(int prefixLength, int count) throws IOException { // 前缀长度相同

      assert count > 0;

      //if (DEBUG2) {
      //  BytesRef br = new BytesRef(lastTerm.bytes());
      //  br.length = prefixLength;
      //  System.out.println("writeBlocks: seg=" + segment + " prefix=" + brToString(br) + " count=" + count);
      //}

      // Root block better write all remaining pending entries:
      assert prefixLength > 0 || count == pending.size();

      int lastSuffixLeadLabel = -1;

      // True if we saw at least one term in this block (we record if a block
      // only points to sub-blocks in the terms index so we can avoid seeking
      // to it when we are looking for a term):
      boolean hasTerms = false;// 如果这个值为true，那么这个block中至少有一个term
      boolean hasSubBlocks = false;// 如果这个值为true，那么这个block中至少有一个新产生的block

      int start = pending.size()-count; // 计算起始位置
      int end = pending.size();// 终止位置
      int nextBlockStart = start;// 记录下
      int nextFloorLeadLabel = -1;// 当前block中第一个term[prefixLength],特点就是和上个block最后一个term[prefixLength]不同

      for (int i=start; i<end; i++) {

        PendingEntry ent = pending.get(i);// term 的后缀的第一个term
        // 保存了树中某个节点下的各个Term的byte
        int suffixLeadLabel;
       // term 的后缀的第一个字符
        if (ent.isTerm) { // 要进来
          PendingTerm term = (PendingTerm) ent; //
          if (term.termBytes.length == prefixLength) { // 和前缀一样长
            // Suffix is 0, i.e. prefix 'foo' and term is
            // 'foo' so the term has empty string suffix
            // in this block
            assert lastSuffixLeadLabel == -1: "i=" + i + " lastSuffixLeadLabel=" + lastSuffixLeadLabel;
            suffixLeadLabel = -1; // 后缀值
          } else {
            suffixLeadLabel = term.termBytes[prefixLength] & 0xff; // 就是prefixLength上某个字符
          }
        } else {
          PendingBlock block = (PendingBlock) ent;
          assert block.prefix.length > prefixLength;
          suffixLeadLabel = block.prefix.bytes[block.prefix.offset + prefixLength] & 0xff; // 不同那个后缀
        }
        // if (DEBUG) System.out.println("  i=" + i + " ent=" + ent + " suffixLeadLabel=" + suffixLeadLabel);

        if (suffixLeadLabel != lastSuffixLeadLabel) { // 第prefixLength个字符和前一次第prefixLength字符不一致
          int itemsInBlock = i - nextBlockStart;// 这个block的长度
          if (itemsInBlock >= minItemsInBlock && end-nextBlockStart > maxItemsInBlock) {// 如果Entry的个数超过minItemsInBlock，且小于maxItemsInBlock，则将这个Entry集合写入到磁盘的同一个block
            // The count is too large for one block, so we must break it into "floor" blocks, where we record
            // the leading label of the suffix of the first term in each floor block, so at search time we can
            // jump to the right floor block.  We just use a naive greedy segmenter here: make a new floor
            // block as soon as we have at least minItemsInBlock.  This is not always best: it often produces
            // a too-small block as the final block:
            boolean isFloor = itemsInBlock < count; // 若这个block小于这次满足需求的总block, 那么就拆分
            newBlocks.add(writeBlock(prefixLength, isFloor, nextFloorLeadLabel, nextBlockStart, i, hasTerms, hasSubBlocks));

            hasTerms = false;
            hasSubBlocks = false;
            nextFloorLeadLabel = suffixLeadLabel;// 下一个block的不相同的字母。
            nextBlockStart = i;// 记录下一个block的起始位置
          }

          lastSuffixLeadLabel = suffixLeadLabel;// 更新term 的后缀的第一个字符
        }

        if (ent.isTerm) {
          hasTerms = true;
        } else {
          hasSubBlocks = true;
        }
      }
      // 最后一个
      // Write last block, if any:
      if (nextBlockStart < end) {
        int itemsInBlock = end - nextBlockStart;
        boolean isFloor = itemsInBlock < count;
        newBlocks.add(writeBlock(prefixLength, isFloor, nextFloorLeadLabel, nextBlockStart, end, hasTerms, hasSubBlocks));
      }

      assert newBlocks.isEmpty() == false;

      PendingBlock firstBlock = newBlocks.get(0);

      assert firstBlock.isFloor || newBlocks.size() == 1;
      // 将一个block的信息写入FST结构中（保存在其成员变量index中），FST是有限状态机的缩写，其实就是将一棵树的信息保存在其自身的结构中，而这颗树是由所有Term的每个byte形成的
      firstBlock.compileIndex(newBlocks, scratchBytes, scratchIntsRef); // scratchBytes，scratchIntsRef还没有存储
      // 对每个写入磁盘的block的前缀 prefix构建一个FST索引 // 所有block的FST索引联合成一个FST索引，并将联合的FST写入 root block
      // Remove slice from the top of the pending stack, that we just wrote:
      pending.subList(pending.size()-count, pending.size()).clear();

      // Append new block
      pending.add(firstBlock); // 向里面写入了一个block

      newBlocks.clear();
    }

    private boolean allEqual(byte[] b, int startOffset, int endOffset, byte value) {
      FutureObjects.checkFromToIndex(startOffset, endOffset, b.length);
      for (int i = startOffset; i < endOffset; ++i) {
        if (b[i] != value) {
          return false;
        }
      }
      return true;
    }

    /** Writes the specified slice (start is inclusive, end is exclusive)
     *  from pending stack as a new block.  If isFloor is true, there
     *  were too many (more than maxItemsInBlock) entries sharing the
     *  same prefix, and so we broke it into multiple floor blocks where // floorLeadLabel除第一个是-1，其他bloc时都是和上一个block不同的那个字母
     *  we record the starting label of the suffix of each floor block. */   // 将该block索引信息全部写入tim中了
    private PendingBlock writeBlock(int prefixLength, boolean isFloor, int floorLeadLabel, int start, int end,
                                    boolean hasTerms, boolean hasSubBlocks) throws IOException {
     // 可以看下这里的介绍：https://www.2cto.com/kf/201608/540054.html
      assert end > start;

      long startFP = termsOut.getFilePointer(); // 获取当前block在tim中的起始位置

      boolean hasFloorLeadLabel = isFloor && floorLeadLabel != -1;

      final BytesRef prefix = new BytesRef(prefixLength + (hasFloorLeadLabel ? 1 : 0));
      System.arraycopy(lastTerm.get().bytes, 0, prefix.bytes, 0, prefixLength);
      prefix.length = prefixLength;

      //if (DEBUG2) System.out.println("    writeBlock field=" + fieldInfo.name + " prefix=" + brToString(prefix) + " fp=" + startFP + " isFloor=" + isFloor + " isLastInFloor=" + (end == pending.size()) + " floorLeadLabel=" + floorLeadLabel + " start=" + start + " end=" + end + " hasTerms=" + hasTerms + " hasSubBlocks=" + hasSubBlocks);

      // Write block header:
      int numEntries = end - start;
      int code = numEntries << 1; // block的term个数
      if (end == pending.size()) {
        // Last block:
        code |= 1;  // 标志是最后一个block
      }
      termsOut.writeVInt(code); // tim

      /*
      if (DEBUG) {
        System.out.println("  writeBlock " + (isFloor ? "(floor) " : "") + "seg=" + segment + " pending.size()=" + pending.size() + " prefixLength=" + prefixLength + " indexPrefix=" + brToString(prefix) + " entCount=" + (end-start+1) + " startFP=" + startFP + (isFloor ? (" floorLeadLabel=" + Integer.toHexString(floorLeadLabel)) : ""));
      }
      */

      // 1st pass: pack term suffix bytes into byte[] blob
      // TODO: cutover to bulk int codec... simple64?

      // We optimize the leaf block case (block has only terms), writing a more
      // compact format in this case:
      boolean isLeafBlock = hasSubBlocks == false;// 如果这个值为true，那么这个block中没有一个是新产生的block

      //System.out.println("  isLeaf=" + isLeafBlock);

      final List<FST<BytesRef>> subIndices; // 存放的是子fst结构

      boolean absolute = true;

      if (isLeafBlock) { // block仅有terms, 而没有block。压缩率可以更少
        // Block contains only ordinary terms:
        subIndices = null;
        StatsWriter statsWriter = new StatsWriter(this.statsWriter, fieldInfo.getIndexOptions() != IndexOptions.DOCS);
        for (int i=start;i<end;i++) { // 遍历每一个term，将其不同的后缀给存储起来
          PendingEntry ent = pending.get(i);
          assert ent.isTerm: "i=" + i;

          PendingTerm term = (PendingTerm) ent;

          assert StringHelper.startsWith(term.termBytes, prefix): "term.term=" + term.termBytes + " prefix=" + prefix;
          BlockTermState state = term.state;
          final int suffix = term.termBytes.length - prefixLength; // 该词后缀长度
          //if (DEBUG2) {
          //  BytesRef suffixBytes = new BytesRef(suffix);
          //  System.arraycopy(term.termBytes, prefixLength, suffixBytes.bytes, 0, suffix);
          //  suffixBytes.length = suffix;
          //  System.out.println("    write term suffix=" + brToString(suffixBytes));
          //}

          // For leaf block we write suffix straight
          suffixLengthsWriter.writeVInt(suffix);
          suffixWriter.append(term.termBytes, prefixLength, suffix);
          assert floorLeadLabel == -1 || (term.termBytes[prefixLength] & 0xff) >= floorLeadLabel;

          // Write term stats, to separate byte[] blob:
          statsWriter.add(state.docFreq, state.totalTermFreq);

          // Write term meta data
          postingsWriter.encodeTerm(metaWriter, fieldInfo, state, absolute);
          absolute = false;
        }
        statsWriter.finish();
      } else {// 该block子pending中包含子Block
        // Block has at least one prefix term or a sub block:
        subIndices = new ArrayList<>();
        StatsWriter statsWriter = new StatsWriter(this.statsWriter, fieldInfo.getIndexOptions() != IndexOptions.DOCS);
        for (int i=start;i<end;i++) {
          PendingEntry ent = pending.get(i);
          if (ent.isTerm) {
            PendingTerm term = (PendingTerm) ent;

            assert StringHelper.startsWith(term.termBytes, prefix): "term.term=" + term.termBytes + " prefix=" + prefix;
            BlockTermState state = term.state;
            final int suffix = term.termBytes.length - prefixLength;
            //if (DEBUG2) {
            //  BytesRef suffixBytes = new BytesRef(suffix);
            //  System.arraycopy(term.termBytes, prefixLength, suffixBytes.bytes, 0, suffix);
            //  suffixBytes.length = suffix;
            //  System.out.println("      write term suffix=" + brToString(suffixBytes));
            //}

            // For non-leaf block we borrow 1 bit to record
            // if entry is term or sub-block, and 1 bit to record if
            // it's a prefix term.  Terms cannot be larger than ~32 KB
            // so we won't run out of bits:

            suffixLengthsWriter.writeVInt(suffix << 1);
            suffixWriter.append(term.termBytes, prefixLength, suffix);

            // Write term stats, to separate byte[] blob:
            statsWriter.add(state.docFreq, state.totalTermFreq);

            // TODO: now that terms dict "sees" these longs,
            // we can explore better column-stride encodings
            // to encode all long[0]s for this block at
            // once, all long[1]s, etc., e.g. using
            // Simple64.  Alternatively, we could interleave
            // stats + meta ... no reason to have them
            // separate anymore:

            // Write term meta data
            postingsWriter.encodeTerm(metaWriter, fieldInfo, state, absolute);
            absolute = false;
          } else {
            PendingBlock block = (PendingBlock) ent;
            assert StringHelper.startsWith(block.prefix, prefix);
            final int suffix = block.prefix.length - prefixLength;
            assert StringHelper.startsWith(block.prefix, prefix);

            assert suffix > 0;

            // For non-leaf block we borrow 1 bit to record
            // if entry is term or sub-block:f
            suffixLengthsWriter.writeVInt((suffix<<1)|1);
            suffixWriter.append(block.prefix.bytes, prefixLength, suffix);

            //if (DEBUG2) {
            //  BytesRef suffixBytes = new BytesRef(suffix);
            //  System.arraycopy(block.prefix.bytes, prefixLength, suffixBytes.bytes, 0, suffix);
            //  suffixBytes.length = suffix;
            //  System.out.println("      write sub-block suffix=" + brToString(suffixBytes) + " subFP=" + block.fp + " subCode=" + (startFP-block.fp) + " floor=" + block.isFloor);
            //}

            assert floorLeadLabel == -1 || (block.prefix.bytes[prefixLength] & 0xff) >= floorLeadLabel: "floorLeadLabel=" + floorLeadLabel + " suffixLead=" + (block.prefix.bytes[prefixLength] & 0xff);
            assert block.fp < startFP;

            suffixLengthsWriter.writeVLong(startFP - block.fp);
            subIndices.add(block.index); // 把子fst拿出来
          }
        }
        statsWriter.finish();

        assert subIndices.size() != 0;
      }

      // Write suffixes byte[] blob to terms dict output, either uncompressed, compressed with LZ4 or with LowercaseAsciiCompression.
      CompressionAlgorithm compressionAlg = CompressionAlgorithm.NO_COMPRESSION;
      // If there are 2 suffix bytes or less per term, then we don't bother compressing as suffix are unlikely what
      // makes the terms dictionary large, and it also tends to be frequently the case for dense IDs like
      // auto-increment IDs, so not compressing in that case helps not hurt ID lookups by too much.
      // We also only start compressing when the prefix length is greater than 2 since blocks whose prefix length is
      // 1 or 2 always all get visited when running a fuzzy query whose max number of edits is 2.
      if (suffixWriter.length() > 2L * numEntries && prefixLength > 2) {
        // LZ4 inserts references whenever it sees duplicate strings of 4 chars or more, so only try it out if the
        // average suffix length is greater than 6.
        if (suffixWriter.length() > 6L * numEntries) {
          LZ4.compress(suffixWriter.bytes(), 0, suffixWriter.length(), spareWriter, compressionHashTable);
          if (spareWriter.getFilePointer() < suffixWriter.length() - (suffixWriter.length() >>> 2)) {
            // LZ4 saved more than 25%, go for it
            compressionAlg = CompressionAlgorithm.LZ4;
          }
        }
        if (compressionAlg == CompressionAlgorithm.NO_COMPRESSION) {
          spareWriter.reset();
          if (spareBytes.length < suffixWriter.length()) {
            spareBytes = new byte[ArrayUtil.oversize(suffixWriter.length(), 1)];
          }
          if (LowercaseAsciiCompression.compress(suffixWriter.bytes(), suffixWriter.length(), spareBytes, spareWriter)) {
            compressionAlg = CompressionAlgorithm.LOWERCASE_ASCII;
          }
        }
      }
      long token = ((long) suffixWriter.length()) << 3;
      if (isLeafBlock) {
        token |= 0x04;
      }
      token |= compressionAlg.code;
      termsOut.writeVLong(token);
      if (compressionAlg == CompressionAlgorithm.NO_COMPRESSION) { // 进来
        termsOut.writeBytes(suffixWriter.bytes(), suffixWriter.length());
      } else {
        spareWriter.writeTo(termsOut);
      }
      suffixWriter.setLength(0);
      spareWriter.reset();

      // Write suffix lengths
      final int numSuffixBytes = Math.toIntExact(suffixLengthsWriter.getFilePointer());
      spareBytes = ArrayUtil.grow(spareBytes, numSuffixBytes);
      suffixLengthsWriter.writeTo(new ByteArrayDataOutput(spareBytes));
      suffixLengthsWriter.reset();
      if (allEqual(spareBytes, 1, numSuffixBytes, spareBytes[0])) {
        // Structured fields like IDs often have most values of the same length
        termsOut.writeVInt((numSuffixBytes << 1) | 1);
        termsOut.writeByte(spareBytes[0]);
      } else { // 进来
        termsOut.writeVInt(numSuffixBytes << 1);
        termsOut.writeBytes(spareBytes, numSuffixBytes);
      }

      // Stats
      final int numStatsBytes = Math.toIntExact(statsWriter.getFilePointer());
      termsOut.writeVInt(numStatsBytes);
      statsWriter.writeTo(termsOut);
      statsWriter.reset();

      // Write term meta data byte[] blob
      termsOut.writeVInt((int) metaWriter.getFilePointer());
      metaWriter.writeTo(termsOut); //
      metaWriter.reset();

      // if (DEBUG) {
      //   System.out.println("      fpEnd=" + out.getFilePointer());
      // }

      if (hasFloorLeadLabel) { // 这里给多加了一个字段
        // We already allocated to length+1 above:
        prefix.bytes[prefix.length++] = (byte) floorLeadLabel;
      }

      return new PendingBlock(prefix, startFP, hasTerms, isFloor, floorLeadLabel, subIndices); // 下一个的首字母
    }
    // 一个域单独拥有一个
    TermsWriter(FieldInfo fieldInfo) {
      this.fieldInfo = fieldInfo;
      assert fieldInfo.getIndexOptions() != IndexOptions.NONE;
      docsSeen = new FixedBitSet(maxDoc);// 在flush阶段从解析到文档中读取出来，作为可查询的文档
      postingsWriter.setField(fieldInfo);
    }
    // write函数会将term的倒排表写入磁盘
    /** Writes one term's worth of postings. */  // norms为null
    public void write(BytesRef text, TermsEnum termsEnum, NormsProducer norms) throws IOException {
      /*
      if (DEBUG) {
        int[] tmp = new int[lastTerm.length];
        System.arraycopy(prefixStarts, 0, tmp, 0, tmp.length);
        System.out.println("BTTW: write term=" + brToString(text) + " prefixStarts=" + Arrays.toString(tmp) + " pending.size()=" + pending.size());
      }
      */
      // 将term的存入文档，跳表放入内存，产生doc文件。
      BlockTermState state = postingsWriter.writeTerm(text, termsEnum, docsSeen, norms); // 针对的是一个词
      if (state != null) { //IntBlockTermState

        assert state.docFreq != 0;
        assert fieldInfo.getIndexOptions() == IndexOptions.DOCS || state.totalTermFreq >= state.docFreq: "postingsWriter=" + postingsWriter;
        pushTerm(text); // 需要进来
       
        PendingTerm term = new PendingTerm(text, state); // 函数判断pending是否满足构建索引的条件，并将当前term加入pending末尾
        pending.add(term);//当前term加入待索引列表
        //if (DEBUG) System.out.println("    add pending term = " + text + " pending.size()=" + pending.size());

        sumDocFreq += state.docFreq; // 该词在多少文档中出现过
        sumTotalTermFreq += state.totalTermFreq; //该词总的出现频次
        numTerms++;  //
        if (firstPendingTerm == null) {
          firstPendingTerm = term; // 写入的第一个词
        }
        lastPendingTerm = term;
      }
    }

    /** Pushes the new term to the top of the stack, and writes new blocks. */
    private void pushTerm(BytesRef text) throws IOException {
      // Find common prefix between last term and current term:// 本term和上一个term最小值。若有一个词为0，则返回0
      int prefixLength = FutureArrays.mismatch(lastTerm.bytes(), 0, lastTerm.length(), text.bytes, text.offset, text.offset + text.length);
      if (prefixLength == -1) { // Only happens for the first term, if it is empty
        assert lastTerm.length() == 0; // 。前后字符都是空
        prefixLength = 0;
      }

      // if (DEBUG) System.out.println("  shared=" + pos + "  lastTerm.length=" + lastTerm.length);
      // 尽量找一批term，
      // Close the "abandoned" suffix now:// 从后向前是为了更多可能取相似性前缀
      for(int i=lastTerm.length()-1;i>=prefixLength;i--) {
      // 计算与栈顶的Entry的公共前缀为 i 的Entry的数量
        // How many items on top of the stack share the current suffix
        // we are closing:
        int prefixTopSize = pending.size() - prefixStarts[i]; // 当前存量与多少后缀是不同的
        if (prefixTopSize >= minItemsInBlock) { // pending词的个数最少25个。
          // if (DEBUG) System.out.println("pushTerm i=" + i + " prefixTopSize=" + prefixTopSize + " minItemsInBlock=" + minItemsInBlock);
          writeBlocks(i+1, prefixTopSize);//。将最后prefixTopSize产生一个fst，放入PendingBlock中，然后再放入pending中
          prefixStarts[i] -= prefixTopSize-1;
        }// if选中之后并没有直接退出
      }

      if (prefixStarts.length < text.length) { // prefixStarts达不到最大size的话，会不断扩容
        prefixStarts = ArrayUtil.grow(prefixStarts, text.length);
      }

      // Init new tail:
      for(int i=prefixLength;i<text.length;i++) {// 修改不同的后缀。
        prefixStarts[i] = pending.size();
      }

      lastTerm.copyBytes(text); // 缓存上一次的term
    }

    // Finishes all terms in this field
    public void finish() throws IOException {
      if (numTerms > 0) { // 有词的写入
        // if (DEBUG) System.out.println("BTTW: finish prefixStarts=" + Arrays.toString(prefixStarts));

        // Add empty term to force closing of all final blocks:
        pushTerm(new BytesRef()); // 为了尽量再推一波产生block。这样的话，一定会产生term为null的情况

        // TODO: if pending.size() is already 1 with a non-zero prefix length
        // we can save writing a "degenerate" root block, but we have to
        // fix all the places that assume the root block's prefix is the empty string:
        pushTerm(new BytesRef());
        writeBlocks(0, pending.size()); // 将剩余的再次产生一个PendingTerm。从0开始的，说明全部要打包成一个FST
        // 有个最终root的 block
        // We better have one final "root" block:
        assert pending.size() == 1 && !pending.get(0).isTerm: "pending.size()=" + pending.size() + " pending=" + pending;
        final PendingBlock root = (PendingBlock) pending.get(0);
        assert root.prefix.length == 0;
        final BytesRef rootCode = root.index.getEmptyOutput();
        assert rootCode != null;

        ByteBuffersDataOutput metaOut = new ByteBuffersDataOutput();
        fields.add(metaOut);

        metaOut.writeVInt(fieldInfo.number);
        metaOut.writeVLong(numTerms);
        metaOut.writeVInt(rootCode.length);
        metaOut.writeBytes(rootCode.bytes, rootCode.offset, rootCode.length);
        assert fieldInfo.getIndexOptions() != IndexOptions.NONE;
        if (fieldInfo.getIndexOptions() != IndexOptions.DOCS) {
          metaOut.writeVLong(sumTotalTermFreq);
        }
        metaOut.writeVLong(sumDocFreq);
        metaOut.writeVInt(docsSeen.cardinality());
        writeBytesRef(metaOut, new BytesRef(firstPendingTerm.termBytes));
        writeBytesRef(metaOut, new BytesRef(lastPendingTerm.termBytes));
        metaOut.writeVLong(indexOut.getFilePointer());
        // Write FST to index
        root.index.save(metaOut, indexOut);// 将fst写入tip文件
        //System.out.println("  write FST " + indexStartFP + " field=" + fieldInfo.name);

        /*
        if (DEBUG) {
          final String dotFileName = segment + "_" + fieldInfo.name + ".dot";
          Writer w = new OutputStreamWriter(new FileOutputStream(dotFileName));
          Util.toDot(root.index, w, false, false);
          System.out.println("SAVED to " + dotFileName);
          w.close();
        }
        */

      } else {
        assert sumTotalTermFreq == 0 || fieldInfo.getIndexOptions() == IndexOptions.DOCS && sumTotalTermFreq == -1;
        assert sumDocFreq == 0;
        assert docsSeen.cardinality() == 0;
      }
    }

    private final RAMOutputStream suffixLengthsWriter = new RAMOutputStream();
    private final BytesRefBuilder suffixWriter = new BytesRefBuilder(); // 同一个block后缀内容写入地方
    private final RAMOutputStream statsWriter = new RAMOutputStream();// 统计了在多少文档中出现过，多出来的词频
    private final RAMOutputStream metaWriter = new RAMOutputStream();// 该词doc+pos+pay在相应文档的起始位置。
    private final RAMOutputStream spareWriter = new RAMOutputStream(); // 最终数据也写入了metaWriter
    private byte[] spareBytes = BytesRef.EMPTY_BYTES;
    private final LZ4.HighCompressionHashTable compressionHashTable = new LZ4.HighCompressionHashTable();
  }

  private boolean closed;
  // 这里是构建tip文件，还是挺重要的
  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;

    boolean success = false;
    try {
      metaOut.writeVInt(fields.size());
      for (ByteBuffersDataOutput fieldMeta : fields) {
        fieldMeta.copyTo(metaOut);
      }
      CodecUtil.writeFooter(indexOut);
      metaOut.writeLong(indexOut.getFilePointer());
      CodecUtil.writeFooter(termsOut);
      metaOut.writeLong(termsOut.getFilePointer());
      CodecUtil.writeFooter(metaOut);
      success = true;
    } finally {
      if (success) {
        IOUtils.close(metaOut, termsOut, indexOut, postingsWriter);// 这里对doc，pos，pay文件进行关闭
      } else {
        IOUtils.closeWhileHandlingException(metaOut, termsOut, indexOut, postingsWriter);
      }
    }
  }

  private static void writeBytesRef(DataOutput out, BytesRef bytes) throws IOException {
    out.writeVInt(bytes.length);
    out.writeBytes(bytes.bytes, bytes.offset, bytes.length);
  }
}
