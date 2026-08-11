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

import static org.apache.lucene.search.BooleanClause.Occur;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;

public class TestDocIdSetIterator extends LuceneTestCase {

  public void testEmpty() throws IOException {
    DocIdSetIterator disi = DocIdSetIterator.empty();
    assertEquals(-1, disi.docID());
    assertEquals(NO_MORE_DOCS, disi.nextDoc());
    assertEquals(NO_MORE_DOCS, disi.docID());

    disi = DocIdSetIterator.empty();
    assertEquals(-1, disi.docID());
    assertEquals(NO_MORE_DOCS, disi.advance(42));
    assertEquals(NO_MORE_DOCS, disi.docID());
  }

  public void testRangeBasic() throws Exception {
    DocIdSetIterator disi = DocIdSetIterator.range(5, 8);
    assertEquals(-1, disi.docID());
    assertEquals(5, disi.nextDoc());
    assertEquals(6, disi.nextDoc());
    assertEquals(7, disi.nextDoc());
    assertEquals(NO_MORE_DOCS, disi.nextDoc());
  }

  public void testInvalidRange() throws Exception {
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          DocIdSetIterator.range(5, 4);
        });
  }

  public void testInvalidRangeMin() throws Exception {
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          DocIdSetIterator.range(-1, 4);
        });
  }

  public void testEmptyRange() throws Exception {
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          DocIdSetIterator.range(7, 7);
        });
  }

  public void testRangeAdvance() throws Exception {
    DocIdSetIterator disi = DocIdSetIterator.range(5, 20);
    assertEquals(-1, disi.docID());
    assertEquals(5, disi.nextDoc());
    assertEquals(17, disi.advance(17));
    assertEquals(18, disi.nextDoc());
    assertEquals(19, disi.nextDoc());
    assertEquals(NO_MORE_DOCS, disi.nextDoc());
  }

  public void testIntoBitset() throws Exception {
    for (int i = 0; i < 10; i++) {
      int max = 1 + random().nextInt(500);
      DocIdSetIterator expectedDisi;
      DocIdSetIterator actualDisi;
      if ((i & 1) == 0) {
        int min = random().nextInt(max);
        expectedDisi = DocIdSetIterator.range(min, max);
        actualDisi = DocIdSetIterator.range(min, max);
      } else {
        expectedDisi = DocIdSetIterator.all(max);
        actualDisi = DocIdSetIterator.all(max);
      }
      FixedBitSet expected = new FixedBitSet(max * 2);
      FixedBitSet actual = new FixedBitSet(max * 2);
      int doc = -1;
      expectedDisi.nextDoc();
      actualDisi.nextDoc();
      while (doc != NO_MORE_DOCS) {
        int r = random().nextInt(3);
        switch (r) {
          case 0 -> {
            expectedDisi.nextDoc();
            actualDisi.nextDoc();
          }
          case 1 -> {
            int jump = expectedDisi.docID() + random().nextInt(5);
            expectedDisi.advance(jump);
            actualDisi.advance(jump);
          }
          case 2 -> {
            expected.clear();
            actual.clear();
            int upTo =
                random().nextBoolean()
                    ? expectedDisi.docID() - 1
                    : expectedDisi.docID() + random().nextInt(5);
            int offset = expectedDisi.docID() - random().nextInt(max);
            // use the default impl of intoBitSet
            DocIdSetIterator defaultIntoBitSet =
                new DocIdSetIterator() {
                  @Override
                  public int docID() {
                    return expectedDisi.docID();
                  }

                  @Override
                  public int nextDoc() throws IOException {
                    return expectedDisi.nextDoc();
                  }

                  @Override
                  public int advance(int target) throws IOException {
                    return expectedDisi.advance(target);
                  }

                  @Override
                  public long cost() {
                    return expectedDisi.cost();
                  }
                };
            defaultIntoBitSet.intoBitSet(upTo, expected, offset);
            actualDisi.intoBitSet(upTo, actual, offset);
            assertArrayEquals(expected.getBits(), actual.getBits());
          }
        }
        assertEquals(expectedDisi.docID(), actualDisi.docID());
        doc = expectedDisi.docID();
      }
    }
  }

  public void testIntoArray() throws Exception {
    DocIdSetIterator disi = DocIdSetIterator.range(5, 20);
    assertEquals(5, disi.nextDoc());

    // Stops when the array is full and leaves the iterator on the first doc that was not copied.
    int[] docs = new int[4];
    assertEquals(4, disi.intoArray(20, docs));
    assertArrayEquals(new int[] {5, 6, 7, 8}, docs);
    assertEquals(9, disi.docID());

    // Stops on upTo, which is exclusive.
    assertEquals(2, disi.intoArray(11, docs));
    assertEquals(9, docs[0]);
    assertEquals(10, docs[1]);
    assertEquals(11, disi.docID());

    // No doc left below upTo.
    assertEquals(0, disi.intoArray(11, docs));
    assertEquals(11, disi.docID());

    docs = new int[16];
    assertEquals(9, disi.intoArray(NO_MORE_DOCS, docs));
    assertEquals(NO_MORE_DOCS, disi.docID());
    assertEquals(0, disi.intoArray(NO_MORE_DOCS, docs));
  }

  public void testDocIDRunEndAll() throws IOException {
    DocIdSetIterator it = DocIdSetIterator.all(13);
    assertEquals(0, it.nextDoc());
    assertEquals(13, it.docIDRunEnd());
    assertEquals(10, it.advance(10));
    assertEquals(13, it.docIDRunEnd());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, it.advance(13));
  }

  public void testDocIDRunEndRange() throws IOException {
    DocIdSetIterator it = DocIdSetIterator.range(4, 13);
    assertEquals(4, it.nextDoc());
    assertEquals(13, it.docIDRunEnd());
    assertEquals(10, it.advance(10));
    assertEquals(13, it.docIDRunEnd());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, it.advance(13));
  }

  /**
   * When the runtime class is exactly {@link FilterDocIdSetIterator}, {@code intoBitSet} forwards
   * directly to the wrapped iterator. Verified by observing that the side-effect of the underlying
   * BitSetIterator.intoBitSet (advance to NO_MORE_DOCS) is visible through the filter wrapper.
   */
  public void testFilterDocIdSetIteratorIntoBitSetForwarded() throws IOException {
    FixedBitSet src = new FixedBitSet(512);
    src.set(0, 300);
    DocIdSetIterator inner = new BitSetIterator(src, 300);

    FilterDocIdSetIterator wrapped = new FilterDocIdSetIterator(inner);
    assertEquals(0, wrapped.nextDoc());

    FixedBitSet dst = new FixedBitSet(512);
    wrapped.intoBitSet(512, dst, 0);

    for (int d = 0; d < 300; d++) {
      assertTrue("doc " + d + " should be set", dst.get(d));
    }
    for (int d = 300; d < 512; d++) {
      assertFalse(dst.get(d));
    }
    // Forwarded path: wrapped.docID tracks inner.docID after intoBitSet.
    assertEquals(inner.docID(), wrapped.docID());
  }

  /**
   * When a subclass overrides FilterDocIdSetIterator (i.e. {@code getClass() != FilterDocIdSetIterator.class}),
   * {@code intoBitSet} must NOT short-circuit to {@code in.intoBitSet} — the subclass may have changed
   * traversal semantics. The guard falls back to {@code super.intoBitSet} which iterates one doc at a time.
   */
  public void testFilterDocIdSetIteratorIntoBitSetGuardForSubclass() throws IOException {
    DocIdSetIterator inner = DocIdSetIterator.range(0, 10);
    // Subclass: skip even docs in nextDoc.
    FilterDocIdSetIterator subclass =
        new FilterDocIdSetIterator(inner) {
          @Override
          public int nextDoc() throws IOException {
            int d;
            do {
              d = in.nextDoc();
            } while (d != NO_MORE_DOCS && (d & 1) == 0);
            return d;
          }
        };
    assertEquals(1, subclass.nextDoc());

    FixedBitSet dst = new FixedBitSet(16);
    subclass.intoBitSet(10, dst, 0);

    // Default super.intoBitSet honored subclass nextDoc -> only odd docs are set.
    for (int d = 0; d < 10; d++) {
      assertEquals("doc " + d, (d & 1) == 1, dst.get(d));
    }
  }

  /**
   * Verifies that FilterDocIdSetIterator.docIDRunEnd is forwarded to the wrapped iterator, so a
   * dense run from the underlying BitSetIterator is visible to callers like DenseConjunctionBulkScorer.
   */
  public void testFilterDocIdSetIteratorDocIDRunEndForwarded() throws IOException {
    FixedBitSet bs = new FixedBitSet(256);
    bs.set(10, 200);
    BitSetIterator inner = new BitSetIterator(bs, 190);
    inner.nextDoc();

    FilterDocIdSetIterator wrap = new FilterDocIdSetIterator(inner);
    assertEquals(200, wrap.docIDRunEnd());
  }

  /** When subclassed, {@code docIDRunEnd} must fall back to {@code docID() + 1}. */
  public void testFilterDocIdSetIteratorDocIDRunEndGuardForSubclass() throws IOException {
    FixedBitSet bs = new FixedBitSet(256);
    bs.set(10, 200);
    BitSetIterator inner = new BitSetIterator(bs, 190);
    inner.nextDoc();

    FilterDocIdSetIterator subclass = new FilterDocIdSetIterator(inner) {};
    assertEquals(subclass.docID() + 1, subclass.docIDRunEnd());
  }

  /**
   * Real integration test that drives {@link FilterDocIdSetIterator#intoBitSet} through end-to-end
   * query execution.
   *
   * <p>Trigger path:
   *
   * <pre>
   *  IndexSearcher.search(BooleanQuery[FILTER:TermQuery("a"), FILTER:TermQuery("b")])
   *    -&gt; BooleanScorerSupplier.bulkScorer()
   *    -&gt; DenseConjunctionBulkScorer.of(requiredNoScoring=2 clauses, ...)            // line 410 of BooleanScorerSupplier
   *    -&gt; for each Scorer: ScorerUtil.likelyImpactsEnum(scorer.iterator())
   *         -&gt; new FilterDocIdSetIterator(impactsEnum)                                // ScorerUtil:68
   *    -&gt; scoreWindowUsingBitSet(...) calls lead.intoBitSet(...)                       // DenseConjunctionBulkScorer:233
   *    -&gt; FilterDocIdSetIterator.intoBitSet (getClass()==FilterDocIdSetIterator.class) -&gt; in.intoBitSet
   * </pre>
   *
   * <p>To activate the dense path we need:
   *
   * <ul>
   *   <li>{@code maxDoc &gt;= DenseConjunctionBulkScorer.WINDOW_SIZE} (= 4096) — index 4500 docs
   *   <li>{@code leadCost &gt;= maxDoc / DENSITY_THRESHOLD_INVERSE} (= 4500 / 32 ≈ 141) — both terms
   *       appear in &gt;=4000 docs
   *   <li>{@code scoreMode != TOP_SCORES} — use {@link ScoreMode#COMPLETE_NO_SCORES}
   * </ul>
   *
   * Result count is asserted; the test will reach the optimized {@code intoBitSet} path naturally.
   */
  public void testFilterDocIdSetIteratorIntoBitSetIT_DenseConjunction() throws IOException {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = newIndexWriterConfig().setMergePolicy(newLogMergePolicy());
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        // 4500 docs; "a" appears in 4000 docs, "b" in 4000 docs, intersection in 3500 docs.
        for (int i = 0; i < 4500; i++) {
          Document d = new Document();
          if (i < 4000) {
            d.add(new StringField("f", "a", Field.Store.NO));
          }
          if (i >= 500 && i < 4500) {
            d.add(new StringField("f", "b", Field.Store.NO));
          }
          w.addDocument(d);
        }
        w.forceMerge(1);
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = newSearcher(reader);

        BooleanQuery bq =
            new BooleanQuery.Builder()
                .add(new TermQuery(new Term("f", "a")), Occur.FILTER)
                .add(new TermQuery(new Term("f", "b")), Occur.FILTER)
                .build();

        // COMPLETE_NO_SCORES + 2 FILTER clauses + dense + maxDoc>=4096 selects DenseConjunctionBulkScorer.
        TotalHitCountCollectorManager cm =
            new TotalHitCountCollectorManager(searcher.getSlices());
        Integer hits = searcher.search(bq, cm);
        // intersection [500, 4000) = 3500 docs.
        assertEquals(3500, hits.intValue());
      }
    }
  }
}
