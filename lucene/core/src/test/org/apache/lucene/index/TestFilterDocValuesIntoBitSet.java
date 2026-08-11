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
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;

/**
 * Unit tests for the {@code intoBitSet} / {@code docIDRunEnd} forwarding behavior in:
 *
 * <ul>
 *   <li>{@link FilterNumericDocValues}
 *   <li>{@link FilterBinaryDocValues}
 *   <li>{@link FilterSortedDocValues}
 *   <li>{@link FilterSortedNumericDocValues}
 *   <li>{@link FilterSortedSetDocValues}
 * </ul>
 *
 * <p>Each filter unconditionally forwards {@code intoBitSet} and {@code docIDRunEnd} to its wrapped
 * instance so that callers (e.g. NumericComparator's competitive iterator pushdown) keep the
 * batched fast paths provided by the underlying codec instead of falling back to the per-doc
 * default {@link DocIdSetIterator#intoBitSet} / {@code docID()+1} loop.
 *
 * <p>The tests use spy implementations that record whether the wrapped {@code intoBitSet}/{@code
 * docIDRunEnd} was actually invoked, plus a recognizable side-effect (setting a sentinel doc id)
 * so we can detect default-impl regressions, not just functional equivalence.
 */
public class TestFilterDocValuesIntoBitSet extends LuceneTestCase {

  // ---------------------------------------------------------------------------
  // FilterNumericDocValues
  // ---------------------------------------------------------------------------
  public void testFilterNumericDocValuesForwardsIntoBitSet() throws IOException {
    SpyNumericDocValues raw = new SpyNumericDocValues();
    FilterNumericDocValues filtered = new FilterNumericDocValues(raw) {};

    FixedBitSet dst = new FixedBitSet(64);
    filtered.intoBitSet(64, dst, 0);

    assertTrue("intoBitSet should be forwarded to wrapped NumericDocValues", raw.intoBitSetCalled);
    assertTrue("sentinel doc 7 set by underlying intoBitSet", dst.get(7));
  }

  public void testFilterNumericDocValuesForwardsDocIDRunEnd() throws IOException {
    SpyNumericDocValues raw = new SpyNumericDocValues();
    FilterNumericDocValues filtered = new FilterNumericDocValues(raw) {};

    assertEquals(42, filtered.docIDRunEnd());
    assertTrue(raw.docIDRunEndCalled);
  }

  // ---------------------------------------------------------------------------
  // FilterBinaryDocValues
  // ---------------------------------------------------------------------------
  public void testFilterBinaryDocValuesForwardsIntoBitSet() throws IOException {
    SpyBinaryDocValues raw = new SpyBinaryDocValues();
    FilterBinaryDocValues filtered = new FilterBinaryDocValues(raw) {};

    FixedBitSet dst = new FixedBitSet(64);
    filtered.intoBitSet(64, dst, 0);

    assertTrue(raw.intoBitSetCalled);
    assertTrue(dst.get(11));
  }

  public void testFilterBinaryDocValuesForwardsDocIDRunEnd() throws IOException {
    SpyBinaryDocValues raw = new SpyBinaryDocValues();
    FilterBinaryDocValues filtered = new FilterBinaryDocValues(raw) {};

    assertEquals(33, filtered.docIDRunEnd());
    assertTrue(raw.docIDRunEndCalled);
  }

  // ---------------------------------------------------------------------------
  // FilterSortedDocValues
  // ---------------------------------------------------------------------------
  public void testFilterSortedDocValuesForwardsIntoBitSet() throws IOException {
    SpySortedDocValues raw = new SpySortedDocValues();
    FilterSortedDocValues filtered = new FilterSortedDocValues(raw) {};

    FixedBitSet dst = new FixedBitSet(64);
    filtered.intoBitSet(64, dst, 0);

    assertTrue(raw.intoBitSetCalled);
    assertTrue(dst.get(13));
  }

  public void testFilterSortedDocValuesForwardsDocIDRunEnd() throws IOException {
    SpySortedDocValues raw = new SpySortedDocValues();
    FilterSortedDocValues filtered = new FilterSortedDocValues(raw) {};

    assertEquals(55, filtered.docIDRunEnd());
    assertTrue(raw.docIDRunEndCalled);
  }

  // ---------------------------------------------------------------------------
  // FilterSortedNumericDocValues
  // ---------------------------------------------------------------------------
  public void testFilterSortedNumericDocValuesForwardsIntoBitSet() throws IOException {
    SpySortedNumericDocValues raw = new SpySortedNumericDocValues();
    FilterSortedNumericDocValues filtered = new FilterSortedNumericDocValues(raw) {};

    FixedBitSet dst = new FixedBitSet(64);
    filtered.intoBitSet(64, dst, 0);

    assertTrue(raw.intoBitSetCalled);
    assertTrue(dst.get(17));
  }

  public void testFilterSortedNumericDocValuesForwardsDocIDRunEnd() throws IOException {
    SpySortedNumericDocValues raw = new SpySortedNumericDocValues();
    FilterSortedNumericDocValues filtered = new FilterSortedNumericDocValues(raw) {};

    assertEquals(77, filtered.docIDRunEnd());
    assertTrue(raw.docIDRunEndCalled);
  }

  // ---------------------------------------------------------------------------
  // FilterSortedSetDocValues
  // ---------------------------------------------------------------------------
  public void testFilterSortedSetDocValuesForwardsIntoBitSet() throws IOException {
    SpySortedSetDocValues raw = new SpySortedSetDocValues();
    FilterSortedSetDocValues filtered = new FilterSortedSetDocValues(raw);

    FixedBitSet dst = new FixedBitSet(64);
    filtered.intoBitSet(64, dst, 0);

    assertTrue(raw.intoBitSetCalled);
    assertTrue(dst.get(19));
  }

  public void testFilterSortedSetDocValuesForwardsDocIDRunEnd() throws IOException {
    SpySortedSetDocValues raw = new SpySortedSetDocValues();
    FilterSortedSetDocValues filtered = new FilterSortedSetDocValues(raw);

    assertEquals(99, filtered.docIDRunEnd());
    assertTrue(raw.docIDRunEndCalled);
  }

  // ===========================================================================
  // Spy implementations: minimal abstract-method stubs + recordable hot methods.
  // ===========================================================================

  private static class SpyNumericDocValues extends NumericDocValues {
    boolean intoBitSetCalled;
    boolean docIDRunEndCalled;
    int doc = -1;

    @Override
    public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) {
      intoBitSetCalled = true;
      bitSet.set(7);
    }

    @Override
    public int docIDRunEnd() {
      docIDRunEndCalled = true;
      return 42;
    }

    @Override
    public long longValue() {
      return 0L;
    }

    @Override
    public boolean advanceExact(int target) {
      return false;
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() {
      return doc = DocIdSetIterator.NO_MORE_DOCS;
    }

    @Override
    public int advance(int target) {
      return doc = DocIdSetIterator.NO_MORE_DOCS;
    }

    @Override
    public long cost() {
      return 0L;
    }
  }

  private static class SpyBinaryDocValues extends BinaryDocValues {
    boolean intoBitSetCalled;
    boolean docIDRunEndCalled;
    int doc = -1;

    @Override
    public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) {
      intoBitSetCalled = true;
      bitSet.set(11);
    }

    @Override
    public int docIDRunEnd() {
      docIDRunEndCalled = true;
      return 33;
    }

    @Override
    public BytesRef binaryValue() {
      return new BytesRef();
    }

    @Override
    public boolean advanceExact(int target) {
      return false;
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() {
      return doc = DocIdSetIterator.NO_MORE_DOCS;
    }

    @Override
    public int advance(int target) {
      return doc = DocIdSetIterator.NO_MORE_DOCS;
    }

    @Override
    public long cost() {
      return 0L;
    }
  }

  private static class SpySortedDocValues extends SortedDocValues {
    boolean intoBitSetCalled;
    boolean docIDRunEndCalled;
    int doc = -1;

    @Override
    public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) {
      intoBitSetCalled = true;
      bitSet.set(13);
    }

    @Override
    public int docIDRunEnd() {
      docIDRunEndCalled = true;
      return 55;
    }

    @Override
    public int ordValue() {
      return 0;
    }

    @Override
    public BytesRef lookupOrd(int ord) {
      return new BytesRef();
    }

    @Override
    public int getValueCount() {
      return 0;
    }

    @Override
    public boolean advanceExact(int target) {
      return false;
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() {
      return doc = DocIdSetIterator.NO_MORE_DOCS;
    }

    @Override
    public int advance(int target) {
      return doc = DocIdSetIterator.NO_MORE_DOCS;
    }

    @Override
    public long cost() {
      return 0L;
    }
  }

  private static class SpySortedNumericDocValues extends SortedNumericDocValues {
    boolean intoBitSetCalled;
    boolean docIDRunEndCalled;
    int doc = -1;

    @Override
    public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) {
      intoBitSetCalled = true;
      bitSet.set(17);
    }

    @Override
    public int docIDRunEnd() {
      docIDRunEndCalled = true;
      return 77;
    }

    @Override
    public long nextValue() {
      return 0L;
    }

    @Override
    public int docValueCount() {
      return 0;
    }

    @Override
    public boolean advanceExact(int target) {
      return false;
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() {
      return doc = DocIdSetIterator.NO_MORE_DOCS;
    }

    @Override
    public int advance(int target) {
      return doc = DocIdSetIterator.NO_MORE_DOCS;
    }

    @Override
    public long cost() {
      return 0L;
    }
  }

  private static class SpySortedSetDocValues extends SortedSetDocValues {
    boolean intoBitSetCalled;
    boolean docIDRunEndCalled;
    int doc = -1;

    @Override
    public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) {
      intoBitSetCalled = true;
      bitSet.set(19);
    }

    @Override
    public int docIDRunEnd() {
      docIDRunEndCalled = true;
      return 99;
    }

    @Override
    public long nextOrd() {
      return 0L;
    }

    @Override
    public int docValueCount() {
      return 0;
    }

    @Override
    public BytesRef lookupOrd(long ord) {
      return new BytesRef();
    }

    @Override
    public long getValueCount() {
      return 0L;
    }

    @Override
    public boolean advanceExact(int target) {
      return false;
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() {
      return doc = DocIdSetIterator.NO_MORE_DOCS;
    }

    @Override
    public int advance(int target) {
      return doc = DocIdSetIterator.NO_MORE_DOCS;
    }

    @Override
    public long cost() {
      return 0L;
    }
  }

  // ---------------------------------------------------------------------------
  // Real integration test: drive FilterNumericDocValues.intoBitSet against a real
  // Lucene90 codec-backed index, accessed via a FilterLeafReader wrap.
  // ---------------------------------------------------------------------------

  /**
   * End-to-end IT for {@link FilterNumericDocValues#intoBitSet}. Builds a real index with a
   * dense NumericDocValues field, opens it, wraps the leaf reader with a {@link FilterLeafReader}
   * (whose {@code getNumericDocValues} returns the wrapped {@link FilterNumericDocValues}), and
   * drives {@code intoBitSet} on the filtered values.
   *
   * <p>Trigger path:
   *
   * <pre>
   *  IndexWriter (default codec) -&gt; SegmentReader.getNumericDocValues("nv")
   *    returns Lucene90DocValuesProducer$NumericDocValuesAccessor (concrete codec impl).
   *  FilterLeafReader.FilterNumericDocValues wraps it.
   *  filtered.intoBitSet(maxDoc, dst, 0)
   *    -&gt; in.intoBitSet(...)                                              // FilterNumericDocValues:68
   *    -&gt; concrete codec NumericDocValues.intoBitSet
   *         -&gt; disi.intoBitSet(...)                                        // Lucene90DocValuesProducer:635/972/1742
   *         -&gt; FixedBitSet.orRange or per-doc set, depending on density
   * </pre>
   *
   * Without the forwarding override, FilterNumericDocValues would inherit the per-doc default
   * implementation from DocIdSetIterator and the codec's batched fast path would be lost.
   */
  public void testFilterNumericDocValuesIntoBitSetIT_RealIndex() throws IOException {
    try (Directory dir = newDirectory()) {
      try (IndexWriter w =
          new IndexWriter(dir, newIndexWriterConfig().setMergePolicy(newLogMergePolicy()))) {
        // Index 4096 docs with NumericDocValues set on every doc => dense iterator.
        for (int i = 0; i < 4096; i++) {
          Document d = new Document();
          d.add(new NumericDocValuesField("nv", i));
          w.addDocument(d);
        }
        w.forceMerge(1);
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertEquals(1, reader.leaves().size());
        LeafReader leaf = reader.leaves().get(0).reader();
        int maxDoc = leaf.maxDoc();

        NumericDocValues raw = leaf.getNumericDocValues("nv");
        assertNotNull("test setup: nv field must exist", raw);

        // Wrap with a "transparent" FilterNumericDocValues subclass that does not override
        // anything. Forwarding kicks in via FilterNumericDocValues.intoBitSet.
        FilterNumericDocValues filtered = new FilterNumericDocValues(raw) {};

        // Position the iterator and forward intoBitSet through the filter.
        assertEquals(0, filtered.nextDoc());
        FixedBitSet collected = new FixedBitSet(maxDoc);
        filtered.intoBitSet(maxDoc, collected, 0);

        // Verify: the filter's intoBitSet must reach every doc that has a value, matching the
        // codec's authoritative iteration via a fresh NumericDocValues.
        FixedBitSet reference = new FixedBitSet(maxDoc);
        NumericDocValues fresh = leaf.getNumericDocValues("nv");
        for (int doc = fresh.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = fresh.nextDoc()) {
          reference.set(doc);
        }
        assertEquals(
            "FilterNumericDocValues.intoBitSet must collect the same docs as the underlying codec",
            reference.cardinality(),
            collected.cardinality());
        // Sanity: dense across the whole segment.
        assertEquals(maxDoc, collected.cardinality());

        // docIDRunEnd must also forward to the codec-backed value.
        NumericDocValues fresh2 = leaf.getNumericDocValues("nv");
        FilterNumericDocValues filtered2 = new FilterNumericDocValues(fresh2) {};
        assertEquals(0, filtered2.nextDoc());
        assertEquals(
            "FilterNumericDocValues.docIDRunEnd should forward to wrapped codec impl",
            fresh2.docIDRunEnd(),
            filtered2.docIDRunEnd());
      }
    }
  }
}
