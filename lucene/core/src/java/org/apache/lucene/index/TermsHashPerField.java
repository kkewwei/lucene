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
// FreqProxTermsWriterPerField.termBytePool, TermVectorsConsumerPerField.termBytePool, FreqProxTermsWriter.bytesPool, FreqProxTermsWriter.termBytePool,   TermVectorsConsumer.termBytePool
// FreqProxTermsWriterPerField里面的bytesHash.pool, TermVectorsConsumerPerField.pool是同一个BytePool
import java.io.IOException;

import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash.BytesStartArray;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.IntBlockPool;

/**
 * This class stores streams of information per term without knowing
 * the size of the stream ahead of time. Each stream typically encodes one level
 * of information like term frequency per document or term proximity. Internally
 * this class allocates a linked list of slices that can be read by a {@link ByteSliceReader}
 * for each term. Terms are first deduplicated in a {@link BytesRefHash} once this is done
 * internal data-structures point to the current offset of each stream that can be written to.
 *///负责词项的索引过程，每个字段有相应的TermsHashPerField， 当索引某字段的词项时，使用对应add过程完成一个词项索引过程，并将索引内容（词项字符创/指针信息/位置信息等）存储域内存缓冲中
abstract class TermsHashPerField implements Comparable<TermsHashPerField> { // 本来就是可以对比的
  private static final int HASH_INIT_SIZE = 4;

  private final TermsHashPerField nextPerField;// TermVectorsConsumerPerField
  private final IntBlockPool intPool; //用于存储分 指向每个 Token 在 bytePool 中 freq 和 prox 信息的偏移量。如果不足时,从 DocumentsWriter 的 freeIntBlocks 分配
  final ByteBlockPool bytePool; // 写入词项位置信息, 用于存储 freq, prox 信息
  // for each term we store an integer per stream that points into the bytePool above
  // the address is updated once data is written to the stream to point to the next free offset
  // in the terms stream. The start address for the stream is stored in postingsArray.byteStarts[termId]
  // This is initialized in the #addTerm method, either to a brand new per term stream if the term is new or
  // to the addresses where the term stream was written to when we saw it the last time.
  private int[] termStreamAddressBuffer;
  private int streamAddressOffset;
  private final int streamCount;
  private final String fieldName;
  final IndexOptions indexOptions;
  /* This stores the actual term bytes for postings and offsets into the parent hash in the case that this
  * TermsHashPerField is hashing term vectors.*/
  private final BytesRefHash bytesHash;

  ParallelPostingsArray postingsArray;
  private int lastDocID; // only with assert

  /** streamCount: how many streams this field stores per term.
   * E.g. doc(+freq) is 1 stream, prox+offset is a second. */
  TermsHashPerField(int streamCount, IntBlockPool intPool, ByteBlockPool bytePool, ByteBlockPool termBytePool,
                    Counter bytesUsed, TermsHashPerField nextPerField, String fieldName, IndexOptions indexOptions) {
    this.intPool = intPool;
    this.bytePool = bytePool;// 两个是是单独分别有一个
    this.streamCount = streamCount; // 见FreqProxTermsWriterPerField，文档，词频，位置都要被索引，只能是2
    this.fieldName = fieldName;
    this.nextPerField = nextPerField;
    assert indexOptions != IndexOptions.NONE;
    this.indexOptions = indexOptions;
    PostingsBytesStartArray byteStarts = new PostingsBytesStartArray(this, bytesUsed);
    bytesHash = new BytesRefHash(termBytePool, HASH_INIT_SIZE, byteStarts); // 每个field都有一个
  }
    // TermVectorsConsumerPerField里面的这个日志会清空，但是FreqProxTermsWriterPerField里面的不会清空
  void reset() { // 仅仅清了bytesHash，别的都没有清理。
    bytesHash.clear(false);// 若写完一个文档，就会去清理nextTermsHash中的ids
    sortedTermIDs = null;
    if (nextPerField != null) {
      nextPerField.reset(); // 永远都不会进来
    }
  }
  // 在TermVectorsConsumerPerField.finish中会调用
  final void initReader(ByteSliceReader reader, int termID, int stream) {
    assert stream < streamCount;
    int streamStartOffset = postingsArray.addressOffset[termID];
    final int[] streamAddressBuffer = intPool.buffers[streamStartOffset >> IntBlockPool.INT_BLOCK_SHIFT];
    final int offsetInAddressBuffer = streamStartOffset & IntBlockPool.INT_BLOCK_MASK;
    reader.init(bytePool,
                postingsArray.byteStarts[termID]+stream*ByteBlockPool.FIRST_LEVEL_SIZE,// 第termID个term在bytePool中第stream中取值
                streamAddressBuffer[offsetInAddressBuffer+stream]);// 可用位置
  }

  private int[] sortedTermIDs;// 对termId进行排序，应该是按照字母从小向大排序的。在finishDocument时候就会调用

  /** Collapse the hash table and sort in-place; also sets
   * this.sortedTermIDs to the results
   * This method must not be called twice unless {@link #reset()}
   * or {@link #reinitHash()} was called. */
  final void sortTerms() {
    assert sortedTermIDs == null;
    sortedTermIDs = bytesHash.sort();// 主要是对ids排序，排序规则是根据termId对应的二进制大小进行排序(也即是term字母排序)
  }

  /**
   * Returns the sorted term IDs. {@link #sortTerms()} must be called before
   */
  final int[] getSortedTermIDs() {
    assert sortedTermIDs != null;
    return sortedTermIDs;
  }

  final void reinitHash() {
    sortedTermIDs = null;
    bytesHash.reinit();
  }

  private boolean doNextCall;// 在es中通过"term_vector": "with_positions_offsets"等方式开启，默认关闭，在lucene中通过ieldType.setStoreTermVectors(true)开启

  // Secondary entry point (for 2nd & subsequent TermsHash),
  // because token text has already been "interned" into
  // textStart, so we hash by textStart.  term vectors use
  // this API.
  private void add(int textStart, final int docID) throws IOException { //第二次进入，textStart代表postingsArray.textStarts中第termId起始位置
    int termID = bytesHash.addByPoolOffset(textStart); // 只是当前文档，当前域的局部字段编号
    if (termID >= 0) {      // New posting
      // First time we are seeing this token since we last
      // flushed the hash.
      initStreamSlices(termID, docID);
    } else {
      positionStreamSlice(termID, docID);
    }
  }

  private void initStreamSlices(int termID, int docID) throws IOException {
    // Init stream slices
    // TODO: figure out why this is 2*streamCount here. streamCount should be enough?
    if ((2*streamCount) + intPool.intUpto > IntBlockPool.INT_BLOCK_SIZE) {
      // can we fit all the streams in the current buffer?
      intPool.nextBuffer(); // 内存不足，申请
    }

    if (ByteBlockPool.BYTE_BLOCK_SIZE - bytePool.byteUpto < (2*streamCount) * ByteBlockPool.FIRST_LEVEL_SIZE) {//当 bytePool 不足的时候,在 freeByteBlocks 中分配新的 buffer。
      // can we fit at least one byte per stream in the current buffer, if not allocate a new one
      bytePool.nextBuffer();
    }
   //此处 streamCount 为 2,表明在 intPool 中,每两项表示一个词,一个是指向 bytePool 中 freq 信息偏移量的,一个是指向 bytePool 中prox 信息偏移量的。
    termStreamAddressBuffer = intPool.buffer;
    streamAddressOffset = intPool.intUpto; // int当前buffer内的可分配位置
    intPool.intUpto += streamCount; // advance the pool to reserve the N streams for this term// 先在intPool中申请2个位置

    postingsArray.addressOffset[termID] = streamAddressOffset + intPool.intOffset;// 第i个词在int中（为了快速找到该词的int位置）绝对起始起始位置
    //在 bytePool 中分配两个空间,一个放 freq 信息,一个放 prox 信息的。
    for (int i = 0; i < streamCount; i++) {
      // initialize each stream with a slice we start with ByteBlockPool.FIRST_LEVEL_SIZE)
      // and grow as we need more space. see ByteBlockPool.LEVEL_SIZE_ARRAY
      final int upto = bytePool.newSlice(ByteBlockPool.FIRST_LEVEL_SIZE);// 申请两个slice，返回的是该slice的相对起始位置
      termStreamAddressBuffer[streamAddressOffset + i] = upto + bytePool.byteOffset;
    }
    postingsArray.byteStarts[termID] = termStreamAddressBuffer[streamAddressOffset]; // 把该termId的使用byteBlockPool起始位置给记录下来
    newTerm(termID, docID); // 前面试把架子搭好了
  }

  private boolean assertDocId(int docId) {
    assert docId >= lastDocID : "docID must be >= " + lastDocID + " but was: " + docId;
    lastDocID = docId;
    return true;
  }
  // 见Lucene原理与代码分析完整版 P126
  /** Called once per inverted token.  This is the primary
   *  entry point (for first TermsHash); postings use this
   *  API. */// 索引过程由add()函数完成
  void add(BytesRef termBytes, final int docID) throws IOException {
    assert assertDocId(docID);
    // We are first in the chain so we must "intern" the
    // term text into textStart address
    // Get the text & hash of this term.是用来存储term内容的  1152
    int termID = bytesHash.add(termBytes); // 属于第几个不重复的term，和add()函数里面的bytesHash不是同一个对象的
    //System.out.println("add term=" + termBytesRef.utf8ToString() + " doc=" + docState.docID + " termID=" + termID);
    if (termID >= 0) { // New posting// 新的term
      // Init stream slices
      initStreamSlices(termID, docID);
    } else {// 这说明已经索引过此词项(再次在原始文档中出现)
      termID = positionStreamSlice(termID, docID);
    }//并没有再次存放termsId...
    if (doNextCall) { // 为啥会在TermVectorsConsumerPerField中重复来一次add，前面分明已经记录了
      nextPerField.add(postingsArray.textStarts[termID], docID);// 进入TermVectorsConsumerPerField.add(termId),也就是本类上面的函数
    }// postingsArray.textStarts[termID]实际等价于bytesHash里面的bytesStart[termID]
  }

  private int positionStreamSlice(int termID, final int docID) throws IOException {
    termID = (-termID) - 1;// 获得真实的termId
    int intStart = postingsArray.addressOffset[termID];
    termStreamAddressBuffer = intPool.buffers[intStart >> IntBlockPool.INT_BLOCK_SHIFT];// 是哪个buffer
    streamAddressOffset = intStart & IntBlockPool.INT_BLOCK_MASK;
    addTerm(termID, docID);
    return termID;
  }

  final void writeByte(int stream, byte b) {
    int streamAddress = streamAddressOffset + stream;
    int upto = termStreamAddressBuffer[streamAddress];// 记录的是bytePool.buffers中的某个slice的绝对起始位置
    byte[] bytes = bytePool.buffers[upto >> ByteBlockPool.BYTE_BLOCK_SHIFT]; // 获得这个存储空间
    assert bytes != null;
    int offset = upto & ByteBlockPool.BYTE_BLOCK_MASK; // budder相对位置
    if (bytes[offset] != 0) { // 不为0代表是下一level的长度，需要扩容。这里就是扩容
      // End of slice; allocate a new one
      offset = bytePool.allocSlice(bytes, offset);
      bytes = bytePool.buffer;
      termStreamAddressBuffer[streamAddress] = offset + bytePool.byteOffset;
    }
    bytes[offset] = b;
    (termStreamAddressBuffer[streamAddress])++;// 这个位置已经被写入了一条数据，存储前进一位
  }

  final void writeBytes(int stream, byte[] b, int offset, int len) {
    // TODO: optimize
    final int end = offset + len;
    for(int i=offset;i<end;i++)
      writeByte(stream, b[i]);
  }

  final void writeVInt(int stream, int i) {
    assert stream < streamCount;
    while ((i & ~0x7F) != 0) {
      writeByte(stream, (byte)((i & 0x7f) | 0x80));
      i >>>= 7;
    }
    writeByte(stream, (byte) i); // 的看下是怎么写下去的
  }

  final TermsHashPerField getNextPerField() {
    return nextPerField;
  }

  final String getFieldName() {
    return fieldName;
  }
 // PostingsBytesStartArray就是一个外壳，为了perField.postingsArray扩容
  private static final class PostingsBytesStartArray extends BytesStartArray {

    private final TermsHashPerField perField;
    private final Counter bytesUsed; //

    private PostingsBytesStartArray(
        TermsHashPerField perField, Counter bytesUsed) {
      this.perField = perField;
      this.bytesUsed = bytesUsed;
    }

    @Override
    public int[] init() {
      if (perField.postingsArray == null) {
        perField.postingsArray = perField.createPostingsArray(2);
        perField.newPostingsArray(); //  注意这里的赋值，
        bytesUsed.addAndGet(perField.postingsArray.size * perField.postingsArray.bytesPerPosting()); // 注意这里统计
      }
      return perField.postingsArray.textStarts;
    }
    // 初始化与扩容都是通过PostingsBytesStartArray实现的。
    @Override
    public int[] grow() {
      ParallelPostingsArray postingsArray = perField.postingsArray;
      final int oldSize = perField.postingsArray.size;
      postingsArray = perField.postingsArray = postingsArray.grow();
      perField.newPostingsArray();
      bytesUsed.addAndGet((postingsArray.bytesPerPosting() * (postingsArray.size - oldSize)));
      return postingsArray.textStarts;
    }

    @Override  // 在每个文档写完后，TermVectorsConsumerPerField的都会去清理
    public int[] clear() {
      if (perField.postingsArray != null) {
        bytesUsed.addAndGet(-(perField.postingsArray.size * perField.postingsArray.bytesPerPosting()));
        perField.postingsArray = null;
        perField.newPostingsArray(); // 直接将postingsArray置null
      }
      return null;
    }

    @Override
    public Counter bytesUsed() {
      return bytesUsed;
    }
  }

  @Override
  public final int compareTo(TermsHashPerField other) {// 这里有比较规则，是基于fielName
    return fieldName.compareTo(other.fieldName); // 比较是基于name
  }

  /** Finish adding all instances of this field to the
   *  current document. */
  void finish() throws IOException {
    if (nextPerField != null) {
      nextPerField.finish(); // 就是把TermVectorsConsumerPerField放到TermVectorsConsumer里面
    }
  }

  final int getNumTerms() {
    return bytesHash.size();
  }

  /** Start adding a new field instance; first is true if
   *  this is the first time this field name was seen in the
   *  document. */
  boolean start(IndexableField field, boolean first) {
    if (nextPerField != null) {// TermVectorsConsumerPerField
      doNextCall = nextPerField.start(field, first); // 对termVector进行设置
    }
    return true;
  }

  /** Called when a term is seen for the first time. */
  abstract void newTerm(int termID, final int docID) throws IOException; //这两个产生倒排索引结构

  /** Called when a previously seen term is seen again. */
  abstract void addTerm(int termID, final int docID) throws IOException;

  /** Called when the postings array is initialized or
   *  resized. */
  abstract void newPostingsArray(); // 在FreqProxTermsWriterPerField/TermVectorsConsumerPerField初始化

  /** Creates a new postings array of the specified size. */
  abstract ParallelPostingsArray createPostingsArray(int size);
}
