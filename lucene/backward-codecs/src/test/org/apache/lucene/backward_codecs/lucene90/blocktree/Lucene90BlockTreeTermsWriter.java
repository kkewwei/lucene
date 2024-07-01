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
package org.apache.lucene.backward_codecs.lucene90.blocktree;

import static org.apache.lucene.util.fst.FSTCompiler.getOnHeapReaderWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
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
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.ToStringUtils;
import org.apache.lucene.util.compress.LZ4;
import org.apache.lucene.util.compress.LowercaseAsciiCompression;
import org.apache.lucene.util.fst.ByteSequenceOutputs;
import org.apache.lucene.util.fst.BytesRefFSTEnum;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.FSTCompiler;
import org.apache.lucene.util.fst.Util;
import org.apache.lucene.util.packed.PackedInts;

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
 *
 * <p>Writes terms dict and index, block-encoding (column stride) each term's metadata for each set
 * of terms between two index terms.
 *
 * <p>Files:
 *
 * <ul>
 *   <li><code>.tim</code>: <a href="#Termdictionary">Term Dictionary</a>
 *   <li><code>.tmd</code>: <a href="#Termmetadata">Term Metadata</a>
 *   <li><code>.tip</code>: <a href="#Termindex">Term Index</a>
 * </ul>
 *
 * <p><a id="Termdictionary"></a>
 *
 * <h2>Term Dictionary</h2>
 *
 * <p>The .tim file contains the list of terms in each field along with per-term statistics (such as
 * docfreq) and per-term metadata (typically pointers to the postings list for that term in the
 * inverted index).
 *
 * <p>The .tim is arranged in blocks: with blocks containing a variable number of entries (by
 * default 25-48), where each entry is either a term or a reference to a sub-block.
 *
 * <p>NOTE: The term dictionary can plug into different postings implementations: the postings
 * writer/reader are actually responsible for encoding and decoding the Postings Metadata and Term
 * Metadata sections.
 *
 * <ul>
 *   <li>TermsDict (.tim) --&gt; Header, FieldDict<sup>NumFields</sup>, Footer
 *   <li>FieldDict --&gt; <i>PostingsHeader</i>, NodeBlock<sup>NumBlocks</sup>
 *   <li>NodeBlock --&gt; (OuterNode | InnerNode)
 *   <li>OuterNode --&gt; EntryCount, SuffixLength, Byte<sup>SuffixLength</sup>, StatsLength, &lt;
 *       TermStats &gt;<sup>EntryCount</sup>, MetaLength,
 *       &lt;<i>TermMetadata</i>&gt;<sup>EntryCount</sup>
 *   <li>InnerNode --&gt; EntryCount, SuffixLength[,Sub?], Byte<sup>SuffixLength</sup>, StatsLength,
 *       &lt; TermStats ? &gt;<sup>EntryCount</sup>, MetaLength, &lt;<i>TermMetadata ?
 *       </i>&gt;<sup>EntryCount</sup>
 *   <li>TermStats --&gt; DocFreq, TotalTermFreq
 *   <li>Header --&gt; {@link CodecUtil#writeHeader CodecHeader}
 *   <li>EntryCount,SuffixLength,StatsLength,DocFreq,MetaLength --&gt; {@link DataOutput#writeVInt
 *       VInt}
 *   <li>TotalTermFreq --&gt; {@link DataOutput#writeVLong VLong}
 *   <li>Footer --&gt; {@link CodecUtil#writeFooter CodecFooter}
 * </ul>
 *
 * <p>Notes:
 *
 * <ul>
 *   <li>Header is a {@link CodecUtil#writeHeader CodecHeader} storing the version information for
 *       the BlockTree implementation.
 *   <li>DocFreq is the count of documents which contain the term.
 *   <li>TotalTermFreq is the total number of occurrences of the term. This is encoded as the
 *       difference between the total number of occurrences and the DocFreq.
 *   <li>PostingsHeader and TermMetadata are plugged into by the specific postings implementation:
 *       these contain arbitrary per-file data (such as parameters or versioning information) and
 *       per-term data (such as pointers to inverted files).
 *   <li>For inner nodes of the tree, every entry will steal one bit to mark whether it points to
 *       child nodes(sub-block). If so, the corresponding TermStats and TermMetaData are omitted.
 * </ul>
 *
 * <p><a id="Termmetadata"></a>
 *
 * <h2>Term Metadata</h2>
 *
 * <p>The .tmd file contains the list of term metadata (such as FST index metadata) and field level
 * statistics (such as sum of total term freq).
 *
 * <ul>
 *   <li>TermsMeta (.tmd) --&gt; Header, NumFields, &lt;FieldStats&gt;<sup>NumFields</sup>,
 *       TermIndexLength, TermDictLength, Footer
 *   <li>FieldStats --&gt; FieldNumber, NumTerms, RootCodeLength, Byte<sup>RootCodeLength</sup>,
 *       SumTotalTermFreq?, SumDocFreq, DocCount, MinTerm, MaxTerm, IndexStartFP, FSTHeader,
 *       <i>FSTMetadata</i>
 *   <li>Header,FSTHeader --&gt; {@link CodecUtil#writeHeader CodecHeader}
 *   <li>TermIndexLength, TermDictLength --&gt; {@link DataOutput#writeLong Uint64}
 *   <li>MinTerm,MaxTerm --&gt; {@link DataOutput#writeVInt VInt} length followed by the byte[]
 *   <li>NumFields,FieldNumber,RootCodeLength,DocCount --&gt; {@link DataOutput#writeVInt VInt}
 *   <li>NumTerms,SumTotalTermFreq,SumDocFreq,IndexStartFP --&gt; {@link DataOutput#writeVLong
 *       VLong}
 *   <li>Footer --&gt; {@link CodecUtil#writeFooter CodecFooter}
 * </ul>
 *
 * <p>Notes:
 *
 * <ul>
 *   <li>FieldNumber is the fields number from {@link FieldInfos}. (.fnm)
 *   <li>NumTerms is the number of unique terms for the field.
 *   <li>RootCode points to the root block for the field.
 *   <li>SumDocFreq is the total number of postings, the number of term-document pairs across the
 *       entire field.
 *   <li>DocCount is the number of documents that have at least one posting for this field.
 *   <li>MinTerm, MaxTerm are the lowest and highest term in this field.
 * </ul>
 *
 * <a id="Termindex"></a>
 *
 * <h2>Term Index</h2>
 *
 * <p>The .tip file contains an index into the term dictionary, so that it can be accessed randomly.
 * The index is also used to determine when a given term cannot exist on disk (in the .tim file),
 * saving a disk seek.
 *
 * <ul>
 *   <li>TermsIndex (.tip) --&gt; Header, FSTIndex<sup>NumFields</sup>Footer
 *   <li>Header --&gt; {@link CodecUtil#writeHeader CodecHeader}
 *       <!-- TODO: better describe FST output here -->
 *   <li>FSTIndex --&gt; {@link FST FST&lt;byte[]&gt;}
 *   <li>Footer --&gt; {@link CodecUtil#writeFooter CodecFooter}
 * </ul>
 *
 * <p>Notes:
 *
 * <ul>
 *   <li>The .tip file contains a separate FST for each field. The FST maps a term prefix to the
 *       on-disk block that holds all terms starting with that prefix. Each field's IndexStartFP
 *       points to its FST.
 *   <li>It's possible that an on-disk block would contain too many terms (more than the allowed
 *       maximum (default: 48)). When this happens, the block is sub-divided into new blocks (called
 *       "floor blocks"), and then the output in the FST for the block's prefix encodes the leading
 *       byte of each sub-block, and its file pointer.
 * </ul>
 *
 * @see Lucene90BlockTreeTermsReader
 * @lucene.experimental
 */
public final class Lucene90BlockTreeTermsWriter extends FieldsConsumer {

  /**
   * Suggested default value for the {@code minItemsInBlock} parameter to {@link
   * #Lucene90BlockTreeTermsWriter(SegmentWriteState,PostingsWriterBase,int,int)}.
   */
  public static final int DEFAULT_MIN_BLOCK_SIZE = 25;

  /**
   * Suggested default value for the {@code maxItemsInBlock} parameter to {@link
   * #Lucene90BlockTreeTermsWriter(SegmentWriteState,PostingsWriterBase,int,int)}.
   */
  public static final int DEFAULT_MAX_BLOCK_SIZE = 48;

  // public static boolean DEBUG = false;
  // public static boolean DEBUG2 = false;

  // private final static boolean SAVE_DOT_FILES = false;

  private final IndexOutput metaOut;// tmd文件
  private final IndexOutput termsOut;// tim文件
  private final IndexOutput indexOut;// tip文件
  final int maxDoc;
  final int minItemsInBlock;// 为25
  final int maxItemsInBlock;// 为48
  final int version;

  final PostingsWriterBase postingsWriter;
  final FieldInfos fieldInfos;

  private final List<ByteBuffersDataOutput> fields = new ArrayList<>();

  /**
   * Create a new writer. The number of items (terms or sub-blocks) per block will aim to be between
   * minItemsPerBlock and maxItemsPerBlock, though in some cases the blocks may be smaller than the
   * min.
   */
  public Lucene90BlockTreeTermsWriter(
      SegmentWriteState state,
      PostingsWriterBase postingsWriter,
      int minItemsInBlock,
      int maxItemsInBlock)
      throws IOException {
    this(
        state,
        postingsWriter,
        minItemsInBlock,
        maxItemsInBlock,
        Lucene90BlockTreeTermsReader.VERSION_CURRENT);
  }

  /** Expert constructor that allows configuring the version, used for bw tests. */
  public Lucene90BlockTreeTermsWriter(
      SegmentWriteState state,
      PostingsWriterBase postingsWriter,
      int minItemsInBlock,
      int maxItemsInBlock,
      int version)
      throws IOException {
    validateSettings(minItemsInBlock, maxItemsInBlock);

    this.minItemsInBlock = minItemsInBlock;
    this.maxItemsInBlock = maxItemsInBlock;
    if (version < Lucene90BlockTreeTermsReader.VERSION_START
        || version > Lucene90BlockTreeTermsReader.VERSION_CURRENT) {
      throw new IllegalArgumentException(
          "Expected version in range ["
              + Lucene90BlockTreeTermsReader.VERSION_START
              + ", "
              + Lucene90BlockTreeTermsReader.VERSION_CURRENT
              + "], but got "
              + version);
    }
    this.version = version;

    this.maxDoc = state.segmentInfo.maxDoc();
    this.fieldInfos = state.fieldInfos;
    this.postingsWriter = postingsWriter;

    final String termsName =// _4g_Lucene101_0.tim
        IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            Lucene90BlockTreeTermsReader.TERMS_EXTENSION);
    termsOut = state.directory.createOutput(termsName, state.context);
    boolean success = false;
    IndexOutput metaOut = null, indexOut = null;
    try {
      CodecUtil.writeIndexHeader(
          termsOut,
          Lucene90BlockTreeTermsReader.TERMS_CODEC_NAME,
          version,
          state.segmentInfo.getId(),
          state.segmentSuffix);

      final String indexName =// _4g_Lucene101_0.tip
          IndexFileNames.segmentFileName(
              state.segmentInfo.name,
              state.segmentSuffix,
              Lucene90BlockTreeTermsReader.TERMS_INDEX_EXTENSION);
      indexOut = state.directory.createOutput(indexName, state.context);
      CodecUtil.writeIndexHeader(
          indexOut,
          Lucene90BlockTreeTermsReader.TERMS_INDEX_CODEC_NAME,
          version,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      // segment = state.segmentInfo.name;

      final String metaName =// _4g_Lucene101_0.tmd
          IndexFileNames.segmentFileName(
              state.segmentInfo.name,
              state.segmentSuffix,
              Lucene90BlockTreeTermsReader.TERMS_META_EXTENSION);
      metaOut = state.directory.createOutput(metaName, state.context);
      CodecUtil.writeIndexHeader(
          metaOut,
          Lucene90BlockTreeTermsReader.TERMS_META_CODEC_NAME,
          version,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      // postingsWriter=Lucene101PostingsWriter, mateOut=tmd
      postingsWriter.init(metaOut, state); // have consumer write its format/header

      this.metaOut = metaOut;
      this.indexOut = indexOut;
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(metaOut, termsOut, indexOut);
      }
    }
  }

  /** Throws {@code IllegalArgumentException} if any of these settings is invalid. */
  public static void validateSettings(int minItemsInBlock, int maxItemsInBlock) {
    if (minItemsInBlock <= 1) {
      throw new IllegalArgumentException("minItemsInBlock must be >= 2; got " + minItemsInBlock);
    }
    if (minItemsInBlock > maxItemsInBlock) {
      throw new IllegalArgumentException(
          "maxItemsInBlock must be >= minItemsInBlock; got maxItemsInBlock="
              + maxItemsInBlock
              + " minItemsInBlock="
              + minItemsInBlock);
    }
    if (2 * (minItemsInBlock - 1) > maxItemsInBlock) {
      throw new IllegalArgumentException(
          "maxItemsInBlock must be at least 2*(minItemsInBlock-1); got maxItemsInBlock="
              + maxItemsInBlock
              + " minItemsInBlock="
              + minItemsInBlock);
    }
  }

  @Override
  public void write(Fields fields, NormsProducer norms) throws IOException {
    // if (DEBUG) System.out.println("\nBTTW.write seg=" + segment);

    String lastField = null;
    for (String field : fields) {// 遍历每个需要写入
      assert lastField == null || lastField.compareTo(field) < 0;
      lastField = field;

      // if (DEBUG) System.out.println("\nBTTW.write seg=" + segment + " field=" + field);
      Terms terms = fields.terms(field); // FreqProxFields$FreqProxDocsEnum
      if (terms == null) {
        continue;
      }

      TermsEnum termsEnum = terms.iterator();// FreqProxFields$FreqProxTerms
      TermsWriter termsWriter = new TermsWriter(fieldInfos.fieldInfo(field));
      while (true) {// 这个field，从小到大遍历每个term
        BytesRef term = termsEnum.next();///这个field下面遍历每一个term，实际从FreqProxPostingsArray中拿
        // if (DEBUG) System.out.println("BTTW: next term " + term);

        if (term == null) {
          break;
        }

        // if (DEBUG) System.out.println("write field=" + fieldInfo.name + " term=" +
        // ToStringUtils.bytesRefToString(term));
        termsWriter.write(term, termsEnum, norms);//很重要，会构建这个term的fst树
      }
      // 这个field所有terms都处理完了，
      termsWriter.finish();

      // if (DEBUG) System.out.println("\nBTTW.write done seg=" + segment + " field=" + field);
    }
  }

  static long encodeOutput(long fp, boolean hasTerms, boolean isFloor) {
    assert fp < (1L << 62);
    return (fp << 2)
        | (hasTerms ? Lucene90BlockTreeTermsReader.OUTPUT_FLAG_HAS_TERMS : 0)// 这个block里面包含具体的term
        | (isFloor ? Lucene90BlockTreeTermsReader.OUTPUT_FLAG_IS_FLOOR : 0);// block里面
  }

  private static class PendingEntry {
    public final boolean isTerm;

    protected PendingEntry(boolean isTerm) {
      this.isTerm = isTerm;
    }
  }

  private static final class PendingTerm extends PendingEntry {
    public final byte[] termBytes;
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
      return "TERM: " + ToStringUtils.bytesRefToString(termBytes);
    }
  }
   // 先写低位
  /**
   * Encodes long value to variable length byte[], in MSB order. Use {@link
   * FieldReader#readMSBVLong} to decode.
   *
   * <p>Package private for testing
   */
  static void writeMSBVLong(long l, DataOutput scratchBytes) throws IOException {
    assert l >= 0;
    // Keep zero bits on most significant byte to have more chance to get prefix bytes shared.
    // e.g. we expect 0x7FFF stored as [0x81, 0xFF, 0x7F] but not [0xFF, 0xFF, 0x40]
    final int bytesNeeded = (Long.SIZE - Long.numberOfLeadingZeros(l) - 1) / 7 + 1;// 计算需要多少个字节来表示这个数值
    l <<= Long.SIZE - bytesNeeded * 7;// 将数值左移，使得最高有效位对齐到最左边的字节
    for (int i = 1; i < bytesNeeded; i++) {
      scratchBytes.writeByte((byte) (((l >>> 57) & 0x7FL) | 0x80));//每次读取最高的7位
      l = l << 7;
    }
    scratchBytes.writeByte((byte) (((l >>> 57) & 0x7FL)));// 最高位写入
  }

  private final class PendingBlock extends PendingEntry {
    public final BytesRef prefix;// 这个block的前缀节点
    public final long fp; // // tim文件起始位置，记录了这个block下面26个term的后缀完整的value
    public FST<BytesRef> index;
    public List<FST<BytesRef>> subIndices;//这个block的子block的fst list
    public final boolean hasTerms;
    public final boolean isFloor;// 若非这个blocks不包含全部block，那么isFloor=true
    public final int floorLeadByte;// floorLeadByte是上个子block的第一个后缀字符串

    public PendingBlock(
        BytesRef prefix,
        long fp,
        boolean hasTerms,
        boolean isFloor,
        int floorLeadByte,
        List<FST<BytesRef>> subIndices) {
      super(false);
      this.prefix = prefix;
      this.fp = fp;
      this.hasTerms = hasTerms;
      this.isFloor = isFloor;
      this.floorLeadByte = floorLeadByte;
      this.subIndices = subIndices;
    }

    @Override
    public String toString() {
      return "BLOCK: prefix=" + ToStringUtils.bytesRefToString(prefix);
    }

    public void compileIndex(
        List<PendingBlock> blocks,
        ByteBuffersDataOutput scratchBytes,
        IntsRefBuilder scratchIntsRef)
        throws IOException {

      assert (isFloor && blocks.size() > 1) || (isFloor == false && blocks.size() == 1)
          : "isFloor=" + isFloor + " blocks=" + blocks;
      assert this == blocks.get(0);

      assert scratchBytes.size() == 0;// 置空

      // write the leading vLong in MSB order for better outputs sharing in the FST
      if (version >= Lucene90BlockTreeTermsReader.VERSION_MSB_VLONG_OUTPUT) {
        writeMSBVLong(encodeOutput(fp, hasTerms, isFloor), scratchBytes);// 编码fp和hasTerms，然后放入scratchBytes
      } else {
        scratchBytes.writeVLong(encodeOutput(fp, hasTerms, isFloor));
      }
      if (isFloor) {// 也就是这个blocks产生了不止一个pending block
        scratchBytes.writeVInt(blocks.size() - 1);// 几个子block
        for (int i = 1; i < blocks.size(); i++) {// 去除第一个block，因为本对象就是第一个block
          PendingBlock sub = blocks.get(i);
          assert sub.floorLeadByte != -1;
          // if (DEBUG) {
          //  System.out.println("    write floorLeadByte=" +
          // Integer.toHexString(sub.floorLeadByte&0xff));
          // }
          scratchBytes.writeByte((byte) sub.floorLeadByte);
          assert sub.fp > fp;
          scratchBytes.writeVLong((sub.fp - fp) << 1 | (sub.hasTerms ? 1 : 0));
        }
      }

      long estimateSize = prefix.length;
      for (PendingBlock block : blocks) {
        if (block.subIndices != null) {
          for (FST<BytesRef> subIndex : block.subIndices) {
            estimateSize += subIndex.numBytes();// 每个fst使用的内存大小
          }
        }
      }
      int estimateBitsRequired = PackedInts.bitsRequired(estimateSize);
      int pageBits = Math.min(15, Math.max(6, estimateBitsRequired));

      final ByteSequenceOutputs outputs = ByteSequenceOutputs.getSingleton();
      final int fstVersion;
      if (version >= Lucene90BlockTreeTermsReader.VERSION_FST_CONTINUOUS_ARCS) {
        fstVersion = FST.VERSION_CONTINUOUS_ARCS;
      } else {
        fstVersion = FST.VERSION_90;
      }
      final FSTCompiler<BytesRef> fstCompiler =// 这个fst树太低了吧
          new FSTCompiler.Builder<>(FST.INPUT_TYPE.BYTE1, outputs)
              // Disable suffixes sharing for block tree index because suffixes are mostly dropped
              // from the FST index and left in the term blocks.
              .suffixRAMLimitMB(0d)
              .dataOutput(getOnHeapReaderWriter(pageBits))
              .setVersion(fstVersion)
              .build();
      // if (DEBUG) {
      //  System.out.println("  compile index for prefix=" + prefix);
      // }
      // indexBuilder.DEBUG = false;
      final byte[] bytes = scratchBytes.toArrayCopy();// 这个就是根root的output，而不是fst
      assert bytes.length > 0;
      fstCompiler.add(Util.toIntsRef(prefix, scratchIntsRef), new BytesRef(bytes, 0, bytes.length));// 若prefix=0，则会将root node.final=true
      scratchBytes.reset();

      // Copy over index for all sub-blocks
      for (PendingBlock block : blocks) {// fst要求term小的先写，subIndices里面可能都是大的term
        if (block.subIndices != null) {
          for (FST<BytesRef> subIndex : block.subIndices) {
            append(fstCompiler, subIndex, scratchIntsRef);// 这里比较恐怖呀，最顶层的fst包含所有子blokc的fst的边（但是不包含46个term里面的边，可以这样想，若46个term产生一个blocks时，还没有subIndices）
          }// 仅仅将prefix放入了fstCompiler，并没有放46个term的value
          block.subIndices = null;// 然后将子fst的内容给清空了
        }
      }
      // 每个block的fst没有存放在一起，node等信息都是独立offset的
      index = FST.fromFSTReader(fstCompiler.compile(), fstCompiler.getFSTReader());//将这个fst转变为fst

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
    private void append(
        FSTCompiler<BytesRef> fstCompiler, FST<BytesRef> subIndex, IntsRefBuilder scratchIntsRef)
        throws IOException {
      final BytesRefFSTEnum<BytesRef> subIndexEnum = new BytesRefFSTEnum<>(subIndex);
      BytesRefFSTEnum.InputOutput<BytesRef> indexEnt;
      while ((indexEnt = subIndexEnum.next()) != null) {//遍历这个fst的每个枝->叶子链路。读取的是整个链路完整的路径，也就是一个完整的term
        // if (DEBUG) {
        //  System.out.println("      add sub=" + indexEnt.input + " " + indexEnt.input + " output="
        // + indexEnt.output);
        // }
        fstCompiler.add(Util.toIntsRef(indexEnt.input, scratchIntsRef), indexEnt.output);// 就是一个term或者block
      }
    }
  }

  private final ByteBuffersDataOutput scratchBytes = ByteBuffersDataOutput.newResettableInstance();
  private final IntsRefBuilder scratchIntsRef = new IntsRefBuilder();

  private static class StatsWriter {

    private final DataOutput out;
    private final boolean hasFreqs;
    private int singletonCount;

    StatsWriter(DataOutput out, boolean hasFreqs) {
      this.out = out;
      this.hasFreqs = hasFreqs;
    }

    void add(int df, long ttf) throws IOException {// df: 这个term在多少文档中出现过。ttf：这个term在所有文件中出现的总词频
      // Singletons (DF==1, TTF==1) are run-length encoded
      if (df == 1 && (hasFreqs == false || ttf == 1)) {
        singletonCount++;// 单独一个文档
      } else {
        finish();
        out.writeVInt(df << 1);// 出现频率，肯定小于128个term。非单个文档
        if (hasFreqs) {
          out.writeVLong(ttf - df);
        }
      }
    }

    void finish() throws IOException {
      if (singletonCount > 0) {// 多少个单独文档
        out.writeVInt(((singletonCount - 1) << 1) | 1);// 低位为1
        singletonCount = 0;
      }
    }
  }

  class TermsWriter {//主要是单个filed的倒排写入doc文件写入& 写入tim文件的block写入
    private final FieldInfo fieldInfo;
    private long numTerms; // 词典词的个数
    final FixedBitSet docsSeen;// 多少个文档包含这个字段
    long sumTotalTermFreq; //这个semgent这个字段所有词的count（重复词算多个）
    long sumDocFreq; // 每个词在多少个文档中出现过，实际就是每个词出现的文档次数相加

    // Records index into pending where the current prefix at that
    // length "started"; for example, if current term starts with 't',
    // startsByPrefix[0] is the index into pending for the first
    // term/sub-block starting with 't'.  We use this to figure out when
    // to write a new block:
    private final BytesRefBuilder lastTerm = new BytesRefBuilder();
    private int[] prefixStarts = new int[8];// 可以类似基数排序的每位基数，每个基数位是从penging哪层开始不一致的

    // Pending stack of terms and blocks.  As terms arrive (in sorted order)
    // we append to this stack, and once the top of the stack has enough
    // terms starting with a common prefix, we write a new block with
    // those terms and replace those terms in the stack with a new block:
    private final List<PendingEntry> pending = new ArrayList<>();

    // Reused in writeBlocks:
    private final List<PendingBlock> newBlocks = new ArrayList<>();// 每次blocks都是空的

    private PendingTerm firstPendingTerm;
    private PendingTerm lastPendingTerm;
     //count: 这批term的相同长度是prefixLength
    /** Writes the top count entries in pending, using prevTerm to compute the prefix. */
    void writeBlocks(int prefixLength, int count) throws IOException {// 仅仅写入pending中top count个term

      assert count > 0;

      // if (DEBUG2) {
      //  BytesRef br = new BytesRef(lastTerm.bytes());
      //  br.length = prefixLength;
      //  System.out.println("writeBlocks: seg=" + segment + " prefix=" +
      // ToStringUtils.bytesRefToString(br) + " count=" + count);
      // }

      // Root block better write all remaining pending entries:
      assert prefixLength > 0 || count == pending.size();

      int lastSuffixLeadLabel = -1;

      // True if we saw at least one term in this block (we record if a block
      // only points to sub-blocks in the terms index so we can avoid seeking
      // to it when we are looking for a term):
      boolean hasTerms = false;
      boolean hasSubBlocks = false;

      int start = pending.size() - count;//从后向前数
      int end = pending.size();
      int nextBlockStart = start;// 记录产生这个block的起始位置
      int nextFloorLeadLabel = -1;

      for (int i = start; i < end; i++) {

        PendingEntry ent = pending.get(i);

        int suffixLeadLabel;// 不同后缀

        if (ent.isTerm) {// 是否一个term节点
          PendingTerm term = (PendingTerm) ent;
          if (term.termBytes.length == prefixLength) {// 是否前缀长度
            // Suffix is 0, i.e. prefix 'foo' and term is
            // 'foo' so the term has empty string suffix
            // in this block
            assert lastSuffixLeadLabel == -1
                : "i=" + i + " lastSuffixLeadLabel=" + lastSuffixLeadLabel;
            suffixLeadLabel = -1;// 后缀开始第一个字母
          } else {
            suffixLeadLabel = term.termBytes[prefixLength] & 0xff;// 最后一个term的不同后缀
          }
        } else {// 若这个是block的话
          PendingBlock block = (PendingBlock) ent;
          assert block.prefix.length > prefixLength;
          suffixLeadLabel = block.prefix.bytes[block.prefix.offset + prefixLength] & 0xff;// 取得不同后缀
        }
        // if (DEBUG) System.out.println("  i=" + i + " ent=" + ent + " suffixLeadLabel=" +
        // suffixLeadLabel);

        if (suffixLeadLabel != lastSuffixLeadLabel) {// 记录前后后缀若发生了改变，才会进来
          int itemsInBlock = i - nextBlockStart;
          if (itemsInBlock >= minItemsInBlock && end - nextBlockStart > maxItemsInBlock) {// 这个block已经大于25了，然后剩下的也大于48 ，那么就可以拆一个block
            // The count is too large for one block, so we must break it into "floor" blocks, where
            // we record
            // the leading label of the suffix of the first term in each floor block, so at search
            // time we can
            // jump to the right floor block.  We just use a naive greedy segmenter here: make a new
            // floor
            // block as soon as we have at least minItemsInBlock.  This is not always best: it often
            // produces
            // a too-small block as the final block:
            boolean isFloor = itemsInBlock < count;// 是一个小块，并不是完整的大块
            newBlocks.add(
                writeBlock(
                    prefixLength,// 固定的，前缀长度
                    isFloor,
                    nextFloorLeadLabel,
                    nextBlockStart,// 这个子block开始位置
                    i,// 截止位置
                    hasTerms,
                    hasSubBlocks));

            hasTerms = false;
            hasSubBlocks = false;
            nextFloorLeadLabel = suffixLeadLabel;
            nextBlockStart = i;
          }

          lastSuffixLeadLabel = suffixLeadLabel;
        }

        if (ent.isTerm) {
          hasTerms = true;
        } else {
          hasSubBlocks = true;
        }
      }

      // Write last block, if any:
      if (nextBlockStart < end) {// 写入最后一个，作为一个block
        int itemsInBlock = end - nextBlockStart;// 这个block的大小
        boolean isFloor = itemsInBlock < count;
        newBlocks.add(
            writeBlock(
                prefixLength,// 固定值，相同长度
                isFloor,
                nextFloorLeadLabel,
                nextBlockStart,// 这个block起始位置
                end,// 这个block最终位置
                hasTerms,
                hasSubBlocks));
      }

      assert newBlocks.isEmpty() == false;

      PendingBlock firstBlock = newBlocks.get(0);// 确定只有一个block

      assert firstBlock.isFloor || newBlocks.size() == 1;// 要么一个block装不下，所以第一个一定是子isFloor。要么一个block全部装下了
      // 存放在tim中起始的fp和hasTerms
      firstBlock.compileIndex(newBlocks, scratchBytes, scratchIntsRef);//会将整个子block编译成一个block

      // Remove slice from the top of the pending stack, that we just wrote:
      pending.subList(pending.size() - count, pending.size()).clear();

      // Append new block
      pending.add(firstBlock);// 变成这一个了。

      newBlocks.clear();// 直接清空了，每次blocks都是空的
    }

    private boolean allEqual(byte[] b, int startOffset, int endOffset, byte value) {
      Objects.checkFromToIndex(startOffset, endOffset, b.length);
      for (int i = startOffset; i < endOffset; ++i) {
        if (b[i] != value) {
          return false;
        }
      }
      return true;
    }

    /**
     * Writes the specified slice (start is inclusive, end is exclusive) from pending stack as a new
     * block. If isFloor is true, there were too many (more than maxItemsInBlock) entries sharing
     * the same prefix, and so we broke it into multiple floor blocks where we record the starting
     * label of the suffix of each floor block.
     */
    private PendingBlock writeBlock(// 解码参考SegmentTermsEnumFrame.loadBlock()
        int prefixLength,
        boolean isFloor,// 是子，不是整个46term。并不是完整的46个term
        int floorLeadLabel,
        int start,
        int end,
        boolean hasTerms,
        boolean hasSubBlocks)
        throws IOException {

      assert end > start;

      long startFP = termsOut.getFilePointer();//这个block存储时， tim文件起始位置（termsOut存放的是每个block内部的每个term原始值）

      boolean hasFloorLeadLabel = isFloor && floorLeadLabel != -1;// 是子block

      final BytesRef prefix = new BytesRef(prefixLength + (hasFloorLeadLabel ? 1 : 0));// 前缀
      System.arraycopy(lastTerm.get().bytes, 0, prefix.bytes, 0, prefixLength);
      prefix.length = prefixLength;// 记录前缀长度

      // if (DEBUG2) System.out.println("    writeBlock field=" + fieldInfo.name + " prefix=" +
      // ToStringUtils.bytesRefToString(prefix) + " fp=" + startFP + " isFloor=" + isFloor +
      // " isLastInFloor=" + (end == pending.size()) + " floorLeadLabel=" + floorLeadLabel +
      // " start=" + start + " end=" + end + " hasTerms=" + hasTerms + " hasSubBlocks=" +
      // hasSubBlocks);

      // Write block header:
      int numEntries = end - start; // 包含了多少个字term（可以是block，也可以是term）
      int code = numEntries << 1;
      if (end == pending.size()) {
        // Last block:
        code |= 1;// 记录这个fst的block节点包含多少个term & 是否是最后一个block
      }
      termsOut.writeVInt(code);// tim： 编码存放，多少个term，是否最后一个

      /*
      if (DEBUG) {
        System.out.println("  writeBlock " + (isFloor ? "(floor) " : "") + "seg=" + segment + " pending.size()=" +
        pending.size() + " prefixLength=" + prefixLength + " indexPrefix=" + ToStringUtils.bytesRefToString(prefix) +
        " entCount=" + (end-start+1) + " startFP=" + startFP + (isFloor ? (" floorLeadLabel=" + Integer.toHexString(floorLeadLabel)) : ""));
      }
      */

      // 1st pass: pack term suffix bytes into byte[] blob
      // TODO: cutover to bulk int codec... simple64?

      // We optimize the leaf block case (block has only terms), writing a more
      // compact format in this case:
      boolean isLeafBlock = hasSubBlocks == false;

      // System.out.println("  isLeaf=" + isLeafBlock);

      final List<FST<BytesRef>> subIndices;

      boolean absolute = true;// 是否是这个block第一个term

      if (isLeafBlock) {// 若是全部都是真正的term
        // Block contains only ordinary terms:
        subIndices = null;
        StatsWriter statsWriter =
            new StatsWriter(this.statsWriter, fieldInfo.getIndexOptions() != IndexOptions.DOCS);
        for (int i = start; i < end; i++) {
          PendingEntry ent = pending.get(i);
          assert ent.isTerm : "i=" + i;

          PendingTerm term = (PendingTerm) ent;

          assert StringHelper.startsWith(term.termBytes, prefix) : term + " prefix=" + prefix;
          BlockTermState state = term.state;
          final int suffix = term.termBytes.length - prefixLength;// 这个term的后缀长度
          // if (DEBUG2) {
          //  BytesRef suffixBytes = new BytesRef(suffix);
          //  System.arraycopy(term.termBytes, prefixLength, suffixBytes.bytes, 0, suffix);
          //  suffixBytes.length = suffix;
          //  System.out.println("    write term suffix=" +
          // ToStringUtils.bytesRefToString(suffixBytes));
          // }

          // For leaf block we write suffix straight
          suffixLengthsWriter.writeVInt(suffix);// 写后缀长度
          suffixWriter.append(term.termBytes, prefixLength, suffix);//后缀内容
          assert floorLeadLabel == -1 || (term.termBytes[prefixLength] & 0xff) >= floorLeadLabel;

          // Write term stats, to separate byte[] blob:
          statsWriter.add(state.docFreq, state.totalTermFreq);

          // Write term meta data 将一个term的元数据编码到metaWriter中
          postingsWriter.encodeTerm(metaWriter, fieldInfo, state, absolute);//postingsWriter = Lucene101PostingsWriter
          absolute = false;
        }
        statsWriter.finish();
      } else {// 有部分非term的
        // Block has at least one prefix term or a sub block:
        subIndices = new ArrayList<>();
        StatsWriter statsWriter =
            new StatsWriter(this.statsWriter, fieldInfo.getIndexOptions() != IndexOptions.DOCS);
        for (int i = start; i < end; i++) {
          PendingEntry ent = pending.get(i);
          if (ent.isTerm) {
            PendingTerm term = (PendingTerm) ent;

            assert StringHelper.startsWith(term.termBytes, prefix) : term + " prefix=" + prefix;
            BlockTermState state = term.state;
            final int suffix = term.termBytes.length - prefixLength;
            // if (DEBUG2) {
            //  BytesRef suffixBytes = new BytesRef(suffix);
            //  System.arraycopy(term.termBytes, prefixLength, suffixBytes.bytes, 0, suffix);
            //  suffixBytes.length = suffix;
            //  System.out.println("      write term suffix=" +
            // ToStringUtils.bytesRefToString(suffixBytes));
            // }

            // For non-leaf block we borrow 1 bit to record
            // if entry is term or sub-block, and 1 bit to record if
            // it's a prefix term.  Terms cannot be larger than ~32 KB
            // so we won't run out of bits:

            suffixLengthsWriter.writeVInt(suffix << 1);// 是纯粹特term，末尾是0
            suffixWriter.append(term.termBytes, prefixLength, suffix);

            // Write term stats, to separate byte[] blob:
            statsWriter.add(state.docFreq, state.totalTermFreq);// 每个词的词频

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
          } else {// 针对非term节点
            PendingBlock block = (PendingBlock) ent;
            assert StringHelper.startsWith(block.prefix, prefix);
            final int suffix = block.prefix.length - prefixLength;// 子block和作为一个term的不同后缀
            assert StringHelper.startsWith(block.prefix, prefix);

            assert suffix > 0;

            // For non-leaf block we borrow 1 bit to record
            // if entry is term or sub-block:f
            suffixLengthsWriter.writeVInt((suffix << 1) | 1);// 是并非纯粹term，末尾是1
            suffixWriter.append(block.prefix.bytes, prefixLength, suffix);

            // if (DEBUG2) {
            //  BytesRef suffixBytes = new BytesRef(suffix);
            //  System.arraycopy(block.prefix.bytes, prefixLength, suffixBytes.bytes, 0, suffix);
            //  suffixBytes.length = suffix;
            //  System.out.println("      write sub-block suffix=" +
            // ToStringUtils.bytesRefToString(suffixBytes) + " subFP=" + block.fp + " subCode=" +
            // (startFP-block.fp) + " floor=" + block.isFloor);
            // }

            assert floorLeadLabel == -1
                    || (block.prefix.bytes[prefixLength] & 0xff) >= floorLeadLabel
                : "floorLeadLabel="
                    + floorLeadLabel
                    + " suffixLead="
                    + (block.prefix.bytes[prefixLength] & 0xff);
            assert block.fp < startFP;

            suffixLengthsWriter.writeVLong(startFP - block.fp);
            subIndices.add(block.index);
          }
        }
        statsWriter.finish();

        assert subIndices.size() != 0;
      }

      // Write suffixes byte[] blob to terms dict output, either uncompressed, compressed with LZ4
      // or with LowercaseAsciiCompression.
      CompressionAlgorithm compressionAlg = CompressionAlgorithm.NO_COMPRESSION;
      // If there are 2 suffix bytes or less per term, then we don't bother compressing as suffix
      // are unlikely what
      // makes the terms dictionary large, and it also tends to be frequently the case for dense IDs
      // like
      // auto-increment IDs, so not compressing in that case helps not hurt ID lookups by too much.
      // We also only start compressing when the prefix length is greater than 2 since blocks whose
      // prefix length is
      // 1 or 2 always all get visited when running a fuzzy query whose max number of edits is 2.
      if (suffixWriter.length() > 2L * numEntries && prefixLength > 2) {// 就是想压缩下每个block的内容。比如前缀压缩，block后缀总长度平均大于>2, 共同前缀>2
        // LZ4 inserts references whenever it sees duplicate strings of 4 chars or more, so only try
        // it out if the
        // average suffix length is greater than 6.
        if (suffixWriter.length() > 6L * numEntries) {
          if (compressionHashTable == null) {
            compressionHashTable = new LZ4.HighCompressionHashTable();
          }
          LZ4.compress(// 压缩算法进行压缩
              suffixWriter.bytes(), 0, suffixWriter.length(), spareWriter, compressionHashTable);
          if (spareWriter.size() < suffixWriter.length() - (suffixWriter.length() >>> 2)) {
            // LZ4 saved more than 25%, go for it
            compressionAlg = CompressionAlgorithm.LZ4;// 有必要进行压缩
          }
        }
        if (compressionAlg == CompressionAlgorithm.NO_COMPRESSION) {// 没有压缩
          spareWriter.reset();
          if (spareBytes.length < suffixWriter.length()) {
            spareBytes = new byte[ArrayUtil.oversize(suffixWriter.length(), 1)];
          }
          if (LowercaseAsciiCompression.compress(
              suffixWriter.bytes(), suffixWriter.length(), spareBytes, spareWriter)) {
            compressionAlg = CompressionAlgorithm.LOWERCASE_ASCII;
          }
        }
      }
      long token = ((long) suffixWriter.length()) << 3;// 记录了这个block的压缩前总长度
      if (isLeafBlock) {//是否全部叶子节点
        token |= 0x04;
      }
      token |= compressionAlg.code;
      termsOut.writeVLong(token);// 写入tim文件
      if (compressionAlg == CompressionAlgorithm.NO_COMPRESSION) {
        termsOut.writeBytes(suffixWriter.bytes(), suffixWriter.length());// 写入tim文件
      } else {
        spareWriter.copyTo(termsOut);// 压缩后的，放在spareWriter
      }
      suffixWriter.setLength(0);// 重置位
      spareWriter.reset();

      // Write suffix lengths
      final int numSuffixBytes = Math.toIntExact(suffixLengthsWriter.size());// 后缀总长度
      spareBytes = ArrayUtil.growNoCopy(spareBytes, numSuffixBytes);
      suffixLengthsWriter.copyTo(new ByteArrayDataOutput(spareBytes));// 每个的后缀长度
      suffixLengthsWriter.reset();
      if (allEqual(spareBytes, 1, numSuffixBytes, spareBytes[0])) {// 后缀不全完相同
        // Structured fields like IDs often have most values of the same length
        termsOut.writeVInt((numSuffixBytes << 1) | 1);
        termsOut.writeByte(spareBytes[0]);
      } else {
        termsOut.writeVInt(numSuffixBytes << 1);
        termsOut.writeBytes(spareBytes, numSuffixBytes);
      }

      // Stats
      final int numStatsBytes = Math.toIntExact(statsWriter.size());
      termsOut.writeVInt(numStatsBytes);
      statsWriter.copyTo(termsOut);
      statsWriter.reset();

      // Write term meta data byte[] blob
      termsOut.writeVInt((int) metaWriter.size());
      metaWriter.copyTo(termsOut);
      metaWriter.reset();

      // if (DEBUG) {
      //   System.out.println("      fpEnd=" + out.getFilePointer());
      // }

      if (hasFloorLeadLabel) {
        // We already allocated to length+1 above:
        prefix.bytes[prefix.length++] = (byte) floorLeadLabel;
      }
      // 都产生一个PendingBlock
      return new PendingBlock(prefix, startFP, hasTerms, isFloor, floorLeadLabel, subIndices);
    }//

    TermsWriter(FieldInfo fieldInfo) {
      this.fieldInfo = fieldInfo;
      assert fieldInfo.getIndexOptions() != IndexOptions.NONE;
      docsSeen = new FixedBitSet(maxDoc);
      postingsWriter.setField(fieldInfo);
    }
     //  处理一个字段一个term的posting
    /** Writes one term's worth of postings. */
    public void write(BytesRef text, TermsEnum termsEnum, NormsProducer norms) throws IOException {// doc文件写入
      /*
      if (DEBUG) {
        int[] tmp = new int[lastTerm.length];
        System.arraycopy(prefixStarts, 0, tmp, 0, tmp.length);
        System.out.println("BTTW: write term=" + ToStringUtils.bytesRefToString(text) + " prefixStarts=" + Arrays.toString(tmp) +
        " pending.size()=" + pending.size());
      }
      */
       // 关于处理单个文档词频，position等
      BlockTermState state = postingsWriter.writeTerm(text, termsEnum, docsSeen, norms);// 实际进入 Lucene101PostingsWriter.writeTerm
      if (state != null) {

        assert state.docFreq != 0;
        assert fieldInfo.getIndexOptions() == IndexOptions.DOCS
                || state.totalTermFreq >= state.docFreq
            : "postingsWriter=" + postingsWriter;
        pushTerm(text);// 这部分逻辑和lucene870的一样，找不一样的前缀

        PendingTerm term = new PendingTerm(text, state);// 这个text的一个
        pending.add(term);
        // if (DEBUG) System.out.println("    add pending term = " + text + " pending.size()=" +
        // pending.size());

        sumDocFreq += state.docFreq;
        sumTotalTermFreq += state.totalTermFreq;
        numTerms++; //
        if (firstPendingTerm == null) {
          firstPendingTerm = term;
        }
        lastPendingTerm = term;
      }
    }

    /** Pushes the new term to the top of the stack, and writes new blocks. */
    private void pushTerm(BytesRef text) throws IOException {
      // Find common prefix between last term and current term:
      int prefixLength =//相同前缀情况
          Arrays.mismatch(
              lastTerm.bytes(),
              0,
              lastTerm.length(),
              text.bytes,
              text.offset,
              text.offset + text.length);
      if (prefixLength == -1) { // Only happens for the first term, if it is empty
        assert lastTerm.length() == 0;
        prefixLength = 0;
      }

      // if (DEBUG) System.out.println("  shared=" + pos + "  lastTerm.length=" + lastTerm.length);

      // Close the "abandoned" suffix now:
      for (int i = lastTerm.length() - 1; i >= prefixLength; i--) {// 和迁移额term比的公共前缀

        // How many items on top of the stack share the current suffix
        // we are closing:
        int prefixTopSize = pending.size() - prefixStarts[i];// 从顶部计算，相同的前缀个数
        if (prefixTopSize >= minItemsInBlock) {// 至少达到25个不一样的term，测试阶段pending可达到了112位
          // if (DEBUG) System.out.println("pushTerm i=" + i + " prefixTopSize=" + prefixTopSize +
          // " minItemsInBlock=" + minItemsInBlock);
          writeBlocks(i + 1, prefixTopSize);//达到了至少25不一样的前缀
          prefixStarts[i] -= prefixTopSize - 1;
        }//prefixStarts[i] = prefixStarts[i]- （pending.size() - prefixStarts[i] - 1）
      }

      if (prefixStarts.length < text.length) { // prefixStarts达不到最大size的话，会不断扩容
        prefixStarts = ArrayUtil.grow(prefixStarts, text.length);
      }

      // Init new tail:
      for (int i = prefixLength; i < text.length; i++) {
        prefixStarts[i] = pending.size();
      }

      lastTerm.copyBytes(text);
    }
    // 这个fielda所有terms都处理完了，
    // Finishes all terms in this field
    public void finish() throws IOException {// 这个field的全部terms遍历结束后会进来。节点启动时就会加载
      if (numTerms > 0) {// 91个term
        // if (DEBUG) System.out.println("BTTW: finish prefixStarts=" +
        // Arrays.toString(prefixStarts));

        // Add empty term to force closing of all final blocks:
        pushTerm(new BytesRef());// 写一个空的term，开始构建fst了。并不一定会构建block

        // TODO: if pending.size() is already 1 with a non-zero prefix length
        // we can save writing a "degenerate" root block, but we have to
        // fix all the places that assume the root block's prefix is the empty string:
        pushTerm(new BytesRef()); // 再push一个empyt，有空
        writeBlocks(0, pending.size()); // 强制相同前缀为0，那么就保证，root的相同前缀，一定为""

        // We better have one final "root" block:
        assert pending.size() == 1 && !pending.get(0).isTerm
            : "pending.size()=" + pending.size() + " pending=" + pending;
        final PendingBlock root = (PendingBlock) pending.get(0);// 确定只有一个根fst
        assert root.prefix.length == 0;// 前缀为0，见writerBlocks中指定了prefixLength=0
        final BytesRef rootCode = root.index.getEmptyOutput();// 是root节点output，一定为null
        assert rootCode != null;// 一定为null

        ByteBuffersDataOutput metaOut = new ByteBuffersDataOutput();
        fields.add(metaOut);

        metaOut.writeVInt(fieldInfo.number);
        metaOut.writeVLong(numTerms);// 词典词的个数
        metaOut.writeVInt(rootCode.length);
        metaOut.writeBytes(rootCode.bytes, rootCode.offset, rootCode.length);
        assert fieldInfo.getIndexOptions() != IndexOptions.NONE;
        if (fieldInfo.getIndexOptions() != IndexOptions.DOCS) {
          metaOut.writeVLong(sumTotalTermFreq);
        }
        metaOut.writeVLong(sumDocFreq);
        metaOut.writeVInt(docsSeen.cardinality());///主要是统计这个字段(所有term)在多少个文档中出现过
        writeBytesRef(metaOut, new BytesRef(firstPendingTerm.termBytes));
        writeBytesRef(metaOut, new BytesRef(lastPendingTerm.termBytes));
        metaOut.writeVLong(indexOut.getFilePointer());// // tip文件起始位置
        // Write FST to index
        root.index.save(metaOut, indexOut);//将fst写入out
        // System.out.println("  write FST " + indexStartFP + " field=" + fieldInfo.name);

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
        assert sumTotalTermFreq == 0
            || fieldInfo.getIndexOptions() == IndexOptions.DOCS && sumTotalTermFreq == -1;
        assert sumDocFreq == 0;
        assert docsSeen.cardinality() == 0;
      }
    }

    private final ByteBuffersDataOutput suffixLengthsWriter =
        ByteBuffersDataOutput.newResettableInstance();// 每个term的后缀长度
    private final BytesRefBuilder suffixWriter = new BytesRefBuilder();// 每个term的后缀内容
    private final ByteBuffersDataOutput statsWriter = ByteBuffersDataOutput.newResettableInstance();
    private final ByteBuffersDataOutput metaWriter = ByteBuffersDataOutput.newResettableInstance();
    private final ByteBuffersDataOutput spareWriter = ByteBuffersDataOutput.newResettableInstance();// 若后缀内容可以压缩，则放这里
    private byte[] spareBytes = BytesRef.EMPTY_BYTES;
    private LZ4.HighCompressionHashTable compressionHashTable;
  }

  private boolean closed;

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
      CodecUtil.writeFooter(indexOut);//
      metaOut.writeLong(indexOut.getFilePointer());// 将tip的指针保存在tmd中
      CodecUtil.writeFooter(termsOut);
      metaOut.writeLong(termsOut.getFilePointer());
      CodecUtil.writeFooter(metaOut);
      success = true;
    } finally {
      if (success) {
        IOUtils.close(metaOut, termsOut, indexOut, postingsWriter);
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
