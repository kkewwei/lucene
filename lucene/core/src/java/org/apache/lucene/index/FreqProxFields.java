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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.FreqProxTermsWriterPerField.FreqProxPostingsArray;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

/** Implements limited (iterators only, no stats) {@link
 *  Fields} interface over the in-RAM buffered
 *  fields/terms/postings, to flush postings through the
 *  PostingsFormat. */

class FreqProxFields extends Fields {
  final Map<String,FreqProxTermsWriterPerField> fields = new LinkedHashMap<>();

  public FreqProxFields(List<FreqProxTermsWriterPerField> fieldList) {
    // NOTE: fields are already sorted by field name
    for(FreqProxTermsWriterPerField field : fieldList) {
      fields.put(field.getFieldName(), field);
    }
  }

  public Iterator<String> iterator() {
    return fields.keySet().iterator();
  }

  @Override
  public Terms terms(String field) throws IOException {
    FreqProxTermsWriterPerField perField = fields.get(field); // 4个域
    return perField == null ? null : new FreqProxTerms(perField);
  }

  @Override
  public int size() {
    throw new UnsupportedOperationException();
  }

  private static class FreqProxTerms extends Terms {
    final FreqProxTermsWriterPerField terms;

    public FreqProxTerms(FreqProxTermsWriterPerField terms) {
      this.terms = terms;
    }

    @Override
    public TermsEnum iterator() {
      FreqProxTermsEnum termsEnum = new FreqProxTermsEnum(terms); // terms=FreqProxTermsWriterPerField
      termsEnum.reset();
      return termsEnum;
    }

    @Override
    public long size() {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getSumTotalTermFreq() {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getSumDocFreq() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getDocCount() {
      throw new UnsupportedOperationException();
    }
  
    @Override
    public boolean hasFreqs() {
      return terms.indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
    }

    @Override
    public boolean hasOffsets() {
      // NOTE: the in-memory buffer may have indexed offsets
      // because that's what FieldInfo said when we started,
      // but during indexing this may have been downgraded:
      return terms.indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
    }
  
    @Override
    public boolean hasPositions() {
      // NOTE: the in-memory buffer may have indexed positions
      // because that's what FieldInfo said when we started,
      // but during indexing this may have been downgraded:
      return terms.indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
    }
  
    @Override
    public boolean hasPayloads() {
      return terms.sawPayloads;
    }
  }

  private static class FreqProxTermsEnum extends BaseTermsEnum {
    final FreqProxTermsWriterPerField terms;
    final int[] sortedTermIDs;
    final FreqProxPostingsArray postingsArray;
    final BytesRef scratch = new BytesRef();
    final int numTerms; // 该域总共多少个词，ord从0开始，不能超过numTerms个数
    int ord; // 目前是开始读取的该域所有文档所有term的第几个词，term已经排好序了

    FreqProxTermsEnum(FreqProxTermsWriterPerField terms) {
      this.terms = terms;
      this.numTerms = terms.getNumTerms();// 分词后的
      sortedTermIDs = terms.getSortedTermIDs();
      assert sortedTermIDs != null;
      postingsArray = (FreqProxPostingsArray) terms.postingsArray;
    }

    public void reset() {
      ord = -1;
    }

    public SeekStatus seekCeil(BytesRef text) {
      // TODO: we could instead keep the BytesRefHash
      // intact so this is a hash lookup

      // binary search:
      int lo = 0;
      int hi = numTerms - 1;
      while (hi >= lo) {
        int mid = (lo + hi) >>> 1;
        int textStart = postingsArray.textStarts[sortedTermIDs[mid]];
        terms.bytePool.setBytesRef(scratch, textStart);
        int cmp = scratch.compareTo(text);
        if (cmp < 0) {
          lo = mid + 1;
        } else if (cmp > 0) {
          hi = mid - 1;
        } else {
          // found:
          ord = mid;
          assert term().compareTo(text) == 0;
          return SeekStatus.FOUND;
        }
      }

      // not found:
      ord = lo;
      if (ord >= numTerms) {
        return SeekStatus.END;
      } else {
        int textStart = postingsArray.textStarts[sortedTermIDs[ord]];
        terms.bytePool.setBytesRef(scratch, textStart);
        assert term().compareTo(text) > 0;
        return SeekStatus.NOT_FOUND;
      }
    }

    public void seekExact(long ord) {
      this.ord = (int) ord;
      int textStart = postingsArray.textStarts[sortedTermIDs[this.ord]];
      terms.bytePool.setBytesRef(scratch, textStart);
    }

    @Override
    public BytesRef next() { // 按照sort顺序读取
      ord++;
      if (ord >= numTerms) { // 总共词的个数
        return null;
      } else {
        int textStart = postingsArray.textStarts[sortedTermIDs[ord]];
        terms.bytePool.setBytesRef(scratch, textStart); // 读取这个termID的具体byte
        return scratch;
      }
    }

    @Override
    public BytesRef term() {
      return scratch;
    }

    @Override
    public long ord() {
      return ord;
    }

    @Override
    public int docFreq() {
      // We do not store this per-term, and we cannot
      // implement this at merge time w/o an added pass
      // through the postings:
      throw new UnsupportedOperationException();
    }

    @Override
    public long totalTermFreq() {
      // We do not store this per-term, and we cannot
      // implement this at merge time w/o an added pass
      // through the postings:
      throw new UnsupportedOperationException();
    }

    @Override
    public PostingsEnum postings(PostingsEnum reuse, int flags) {
      if (PostingsEnum.featureRequested(flags, PostingsEnum.POSITIONS)) { // 进来了
        FreqProxPostingsEnum posEnum; // 先去读取一个term
       // 检查freq是否需要
        if (!terms.hasProx) {
          // Caller wants positions but we didn't index them;
          // don't lie:
          throw new IllegalArgumentException("did not index positions");
        }

        if (!terms.hasOffsets && PostingsEnum.featureRequested(flags, PostingsEnum.OFFSETS)) {
          // Caller wants offsets but we didn't index them;
          // don't lie:
          throw new IllegalArgumentException("did not index offsets");
        }

        if (reuse instanceof FreqProxPostingsEnum) {
          posEnum = (FreqProxPostingsEnum) reuse;
          if (posEnum.postingsArray != postingsArray) {
            posEnum = new FreqProxPostingsEnum(terms, postingsArray);
          }
        } else { // 跑到这里
          posEnum = new FreqProxPostingsEnum(terms, postingsArray);
        }
        posEnum.reset(sortedTermIDs[ord]); // 需要进来看下
        return posEnum; // 就直接返回了
      }

      FreqProxDocsEnum docsEnum;

      if (!terms.hasFreq && PostingsEnum.featureRequested(flags, PostingsEnum.FREQS)) {
        // Caller wants freqs but we didn't index them;
        // don't lie:
        throw new IllegalArgumentException("did not index freq");
      }

      if (reuse instanceof FreqProxDocsEnum) {
        docsEnum = (FreqProxDocsEnum) reuse;
        if (docsEnum.postingsArray != postingsArray) {
          docsEnum = new FreqProxDocsEnum(terms, postingsArray);
        }
      } else {
        docsEnum = new FreqProxDocsEnum(terms, postingsArray);
      }
      docsEnum.reset(sortedTermIDs[ord]);
      return docsEnum;
    }

    @Override
    public ImpactsEnum impacts(int flags) throws IOException {
      throw new UnsupportedOperationException();
    }

    /**
     * Expert: Returns the TermsEnums internal state to position the TermsEnum
     * without re-seeking the term dictionary.
     * <p>
     * NOTE: A seek by {@link TermState} might not capture the
     * {@link AttributeSource}'s state. Callers must maintain the
     * {@link AttributeSource} states separately
     * 
     * @see TermState
     * @see #seekExact(BytesRef, TermState)
     */
    public TermState termState() throws IOException {
      return new TermState() {
        @Override
        public void copyFrom(TermState other) {
          throw new UnsupportedOperationException();
        }
      };
    }
  }

  private static class FreqProxDocsEnum extends PostingsEnum {

    final FreqProxTermsWriterPerField terms;
    final FreqProxPostingsArray postingsArray;
    final ByteSliceReader reader = new ByteSliceReader();
    final boolean readTermFreq;
    int docID = -1;
    int freq;
    boolean ended;
    int termID;

    public FreqProxDocsEnum(FreqProxTermsWriterPerField terms, FreqProxPostingsArray postingsArray) {
      this.terms = terms;
      this.postingsArray = postingsArray;
      this.readTermFreq = terms.hasFreq;
    }

    public void reset(int termID) {
      this.termID = termID;
      terms.initReader(reader, termID, 0);
      ended = false;
      docID = -1;
    }

    @Override
    public int docID() {
      return docID;
    }

    @Override
    public int freq() {
      // Don't lie here ... don't want codecs writings lots
      // of wasted 1s into the index:
      if (!readTermFreq) {
        throw new IllegalStateException("freq was not indexed");
      } else {
        return freq;
      }
    }

    @Override
    public int nextPosition() throws IOException {
      return -1;
    }

    @Override
    public int startOffset() throws IOException {
      return -1;
    }

    @Override
    public int endOffset() throws IOException {
      return -1;
    }

    @Override
    public BytesRef getPayload() throws IOException {
      return null;
    }

    @Override
    public int nextDoc() throws IOException {
      if (docID == -1) {
        docID = 0;
      }
      if (reader.eof()) {
        if (ended) {
          return NO_MORE_DOCS;
        } else {
          ended = true;
          docID = postingsArray.lastDocIDs[termID];
          if (readTermFreq) {
            freq = postingsArray.termFreqs[termID];
          }
        }
      } else {
        int code = reader.readVInt();
        if (!readTermFreq) {
          docID += code;
        } else {
          docID += code >>> 1;
          if ((code & 1) != 0) {
            freq = 1;
          } else {
            freq = reader.readVInt();
          }
        }

        assert docID != postingsArray.lastDocIDs[termID];
      }

      return docID;
    }

    @Override
    public int advance(int target) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long cost() {
      throw new UnsupportedOperationException();
    }
  }

  private static class FreqProxPostingsEnum extends PostingsEnum {

    final FreqProxTermsWriterPerField terms;
    final FreqProxPostingsArray postingsArray;
    final ByteSliceReader reader = new ByteSliceReader(); // 读取单个term的freq等信息(stream0)
    final ByteSliceReader posReader = new ByteSliceReader(); // 读取单个term的offset等信息(stream1)
    final boolean readOffsets;
    int docID = -1;
    int freq;
    int pos;
    int startOffset;
    int endOffset;
    int posLeft; // freqency
    int termID;
    boolean ended;
    boolean hasPayload;
    BytesRefBuilder payload = new BytesRefBuilder();

    public FreqProxPostingsEnum(FreqProxTermsWriterPerField terms, FreqProxPostingsArray postingsArray) {
      this.terms = terms;
      this.postingsArray = postingsArray;
      this.readOffsets = terms.hasOffsets; // true
      assert terms.hasProx;
      assert terms.hasFreq;
    }

    public void reset(int termID) {
      this.termID = termID;
      terms.initReader(reader, termID, 0); // FreqProxTermsWriterPerField, 把词频等信息从PoolBuffer0中读取出来
      terms.initReader(posReader, termID, 1); // FreqProxTermsWriterPerField, 把offset、position等信息从PoolBuffer1中读取出来
      ended = false;
      docID = -1;
      posLeft = 0;
    }

    @Override
    public int docID() {
      return docID;
    }

    @Override
    public int freq() {
      return freq;
    }

    @Override
    public int nextDoc() throws IOException { // 读取一个词，将docId&freq读取出来
      if (docID == -1) {
        docID = 0;
      }
      while (posLeft != 0) {
        nextPosition();
      }

      if (reader.eof()) { // 是否到达了该词的末尾
        if (ended) {
          return NO_MORE_DOCS;
        } else {
          ended = true;
          docID = postingsArray.lastDocIDs[termID];
          freq = postingsArray.termFreqs[termID];
        }
      } else {// 跑到这里
        int code = reader.readVInt(); // 可以看下FreqProxTermsWriterPerField.addTerm()里面存储doc的过程L168，就是
        docID += code >>> 1; // doc就是一直累加的
        if ((code & 1) != 0) { // 低位为1，就说明freq为1
          freq = 1;
        } else {
          freq = reader.readVInt();
        }

        assert docID != postingsArray.lastDocIDs[termID];
      }

      posLeft = freq;
      pos = 0;
      startOffset = 0;
      return docID;
    }

    @Override
    public int advance(int target) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long cost() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int nextPosition() throws IOException {
      assert posLeft > 0;
      posLeft--; // 词的频率
      int code = posReader.readVInt(); // 可参考建立索引时候的过程：FreqProxTermsWriterPerField.addTerm(), position-lastPosition   L184
      pos += code >>> 1; // 当前第几个position
      if ((code & 1) != 0) { //
        hasPayload = true;
        // has a payload
        payload.setLength(posReader.readVInt());
        payload.grow(payload.length());
        posReader.readBytes(payload.bytes(), 0, payload.length());
      } else {
        hasPayload = false; // 跑这里
      }

      if (readOffsets) {
        startOffset += posReader.readVInt(); // 可参考建立索引时候的过程：FreqProxTermsWriterPerField.writeOffsets(), offset-lastOffset
        endOffset = startOffset + posReader.readVInt();
      }

      return pos;
    }

    @Override
    public int startOffset() {
      if (!readOffsets) {
        throw new IllegalStateException("offsets were not indexed");
      }
      return startOffset;
    }

    @Override
    public int endOffset() {
      if (!readOffsets) {
        throw new IllegalStateException("offsets were not indexed");
      }
      return endOffset;
    }

    @Override
    public BytesRef getPayload() {
      if (hasPayload) {
        return payload.get();
      } else {
        return null;
      }
    }
  }
}
