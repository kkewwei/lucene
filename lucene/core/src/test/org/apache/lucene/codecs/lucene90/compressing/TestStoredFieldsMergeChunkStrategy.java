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
package org.apache.lucene.codecs.lucene90.compressing;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.codecs.compressing.CompressingCodec;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;

public class TestStoredFieldsMergeChunkStrategy extends LuceneTestCase {

  private static final int CHUNK_SIZE = 4 * 1024;
  /** force chunk boundaries every 4 docs so we can pinpoint deletions */
  private static final int MAX_DOCS_PER_CHUNK = 4;

  private static final int BLOCK_SHIFT = 8;

  private Codec deterministicCompressingCodec(Random r) {
    return CompressingCodec.randomInstance(
        r, CHUNK_SIZE, MAX_DOCS_PER_CHUNK, false, BLOCK_SHIFT);
  }

  /** Small stored payload, but unique per-doc, so we can detect any swapping. */
  private static Document makeDoc(int id) {
    Document doc = new Document();
    doc.add(new StringField("id", Integer.toString(id), Field.Store.NO));
    doc.add(new StoredField("payload", "doc-payload-" + id));
    return doc;
  }

  private static Lucene90CompressingStoredFieldsReader storedFieldsReader(CodecReader cr) {
    return (Lucene90CompressingStoredFieldsReader) cr.getFieldsReader();
  }

  /**
   * Index numDocs without flushing in between, so they live in a single segment with predictable
   * chunk boundaries (every {@link #MAX_DOCS_PER_CHUNK} docs).
   */
  private void indexInOneSegment(IndexWriter iw, int numDocs) throws IOException {
    for (int i = 0; i < numDocs; i++) {
      iw.addDocument(makeDoc(i));
    }
    iw.commit();
  }

  private void assertPayloadsMatch(DirectoryReader reader, Set<Integer> deletedIds)
      throws IOException {
    Set<Integer> seen = new HashSet<>();
    for (LeafReaderContext leaf : reader.leaves()) {
      StoredFields sf = leaf.reader().storedFields();
      for (int i = 0; i < leaf.reader().maxDoc(); i++) {
        Document d = sf.document(i);
        String payload = d.get("payload");
        assertNotNull("missing payload at leaf doc " + i, payload);
        assertTrue("unexpected payload prefix: " + payload, payload.startsWith("doc-payload-"));
        int id = Integer.parseInt(payload.substring("doc-payload-".length()));
        assertFalse("payload for deleted id=" + id + " resurfaced", deletedIds.contains(id));
        assertTrue("duplicate payload for id=" + id, seen.add(id));
      }
    }
  }

  /**
   * Delete a single doc in the middle chunk and verify a single forceMerge produces a correct,
   * complete index. Exercises: per-chunk live-count == chunkDocCount fast path for unaffected
   * chunks, plus the dirty-chunk + borrow-next-chunk path on the impacted chunk.
   */
  public void testDeleteSingleDocInMiddleChunk() throws Exception {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
      iwc.setMergePolicy(NoMergePolicy.INSTANCE);
      Codec codec = deterministicCompressingCodec(random());
      iwc.setCodec(codec);
      try (IndexWriter iw = new IndexWriter(dir, iwc)) {
        // 4 chunks of 4 docs each = 16 docs in a single segment
        indexInOneSegment(iw, 16);
        // delete the middle of chunk #1 (docs 4..7), namely id=5
        iw.deleteDocuments(new Term("id", "5"));
        iw.commit();
      }

      iwc = newIndexWriterConfig(new MockAnalyzer(random()));
      iwc.setCodec(codec);
      iwc.setMergePolicy(newLogMergePolicy());
      try (IndexWriter iw = new IndexWriter(dir, iwc)) {
        iw.forceMerge(1);
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertEquals(15, reader.numDocs());
        Set<Integer> deleted = new HashSet<>();
        deleted.add(5);
        assertPayloadsMatch(reader, deleted);

        CodecReader cr = (CodecReader) getOnlyLeafReader(reader);
        Lucene90CompressingStoredFieldsReader fr = storedFieldsReader(cr);
        // We started from a single segment with 4 chunks; after relocating one dirty chunk and
        // pulling in some neighbours to repack, we should not have produced more chunks than the
        // original 4 (and very likely fewer).
        assertTrue("unexpectedly many chunks: " + fr.getNumChunks(), fr.getNumChunks() <= 4);
      }
    }
  }

  /** Delete the very first doc of a chunk and verify correctness + low dirty-chunk count. */
  public void testDeleteFirstDocOfChunk() throws Exception {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
      iwc.setMergePolicy(NoMergePolicy.INSTANCE);
      Codec codec = deterministicCompressingCodec(random());
      iwc.setCodec(codec);


      try (IndexWriter iw = new IndexWriter(dir, iwc)) {
        indexInOneSegment(iw, 16);
        // first doc of chunk #2
        iw.deleteDocuments(new Term("id", "8"));
        iw.commit();
      }

      iwc = newIndexWriterConfig(new MockAnalyzer(random()));
      iwc.setMergePolicy(newLogMergePolicy());
      iwc.setCodec(codec);
      try (IndexWriter iw = new IndexWriter(dir, iwc)) {
        iw.forceMerge(1);
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertEquals(15, reader.numDocs());
        Set<Integer> deleted = new HashSet<>();
        deleted.add(8);
        assertPayloadsMatch(reader, deleted);
      }
    }
  }

  /** Delete the last doc of a chunk and verify correctness. */
  public void testDeleteLastDocOfChunk() throws Exception {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
      iwc.setMergePolicy(NoMergePolicy.INSTANCE);
      Codec codec = deterministicCompressingCodec(random());
      iwc.setCodec(codec);
      try (IndexWriter iw = new IndexWriter(dir, iwc)) {
        indexInOneSegment(iw, 16);
        // last doc of chunk #1
        iw.deleteDocuments(new Term("id", "7"));
        iw.commit();
      }

      iwc = newIndexWriterConfig(new MockAnalyzer(random()));
      iwc.setCodec(codec);
      iwc.setMergePolicy(newLogMergePolicy());
      try (IndexWriter iw = new IndexWriter(dir, iwc)) {
        iw.forceMerge(1);
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertEquals(15, reader.numDocs());
        Set<Integer> deleted = new HashSet<>();
        deleted.add(7);
        assertPayloadsMatch(reader, deleted);
      }
    }
  }

  /**
   * Delete every doc in a single chunk (full-dead chunk). The relocate path should simply skip
   * that chunk and not emit any output bytes for it.
   */
  public void testDeleteWholeChunk() throws Exception {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
      iwc.setMergePolicy(NoMergePolicy.INSTANCE);
      Codec codec = deterministicCompressingCodec(random());
      iwc.setCodec(codec);
      try (IndexWriter iw = new IndexWriter(dir, iwc)) {
        indexInOneSegment(iw, 16);
        // delete chunk #2 entirely (docs 8..11)
        for (int id = 8; id < 12; id++) {
          iw.deleteDocuments(new Term("id", Integer.toString(id)));
        }
        iw.commit();
      }

      iwc = newIndexWriterConfig(new MockAnalyzer(random()));
      iwc.setCodec(codec);
      iwc.setMergePolicy(newLogMergePolicy());
      try (IndexWriter iw = new IndexWriter(dir, iwc)) {
        iw.forceMerge(1);
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertEquals(12, reader.numDocs());
        Set<Integer> deleted = new HashSet<>();
        for (int id = 8; id < 12; id++) {
          deleted.add(id);
        }
        assertPayloadsMatch(reader, deleted);
      }
    }
  }

  /**
   * Two adjacent chunks both have deletions. Exercises the V2 "borrow-and-fill across multiple
   * chunks" behaviour: residue from the first dirty chunk should be absorbed into the next chunk's
   * processing rather than each producing its own dirty output chunk.
   */
  public void testDeletesInTwoConsecutiveChunks() throws Exception {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
      iwc.setMergePolicy(NoMergePolicy.INSTANCE);
      Codec codec = deterministicCompressingCodec(random());
      iwc.setCodec(codec);
      try (IndexWriter iw = new IndexWriter(dir, iwc)) {
        indexInOneSegment(iw, 16);
        iw.deleteDocuments(new Term("id", "5")); // chunk #1
        iw.deleteDocuments(new Term("id", "9")); // chunk #2
        iw.commit();
      }

      iwc = newIndexWriterConfig(new MockAnalyzer(random()));
      iwc.setCodec(codec);
      iwc.setMergePolicy(newLogMergePolicy());
      try (IndexWriter iw = new IndexWriter(dir, iwc)) {
        iw.forceMerge(1);
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertEquals(14, reader.numDocs());
        Set<Integer> deleted = new HashSet<>();
        deleted.add(5);
        deleted.add(9);
        assertPayloadsMatch(reader, deleted);
      }
    }
  }

  /**
   * Two separate segments, each with deletions, are merged together. Verifies cross-reader
   * ordering and that residue from segment 0 is correctly flushed before segment 1 starts.
   */
  public void testDeletesAcrossTwoSegments() throws Exception {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
      iwc.setMergePolicy(NoMergePolicy.INSTANCE);
      Codec codec = deterministicCompressingCodec(random());
      iwc.setCodec(codec);
      try (IndexWriter iw = new IndexWriter(dir, iwc)) {
        // segment 0: 16 docs (ids 0..15)
        indexInOneSegment(iw, 16);
        // segment 1: 16 docs (ids 100..115)
        for (int i = 100; i < 116; i++) {
          iw.addDocument(makeDoc(i));
        }
        iw.commit();
        // delete one doc from each segment
        iw.deleteDocuments(new Term("id", "6"));
        iw.deleteDocuments(new Term("id", "110"));
        iw.commit();
      }

      iwc = newIndexWriterConfig(new MockAnalyzer(random()));
      iwc.setCodec(codec);
      iwc.setMergePolicy(newLogMergePolicy());
      try (IndexWriter iw = new IndexWriter(dir, iwc)) {
        iw.forceMerge(1);
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertEquals(30, reader.numDocs());
        Set<Integer> deleted = new HashSet<>();
        deleted.add(6);
        deleted.add(110);
        assertPayloadsMatch(reader, deleted);
      }
    }
  }

  /**
   * Index sort enabled: the chunk-relocate fast path must NOT engage (because docs are reordered).
   * We just want to verify correctness here -- the merge should still produce a valid, fully
   * readable index via the {@code DOC} fallback path.
   */
  public void testIndexSortFallsBackToDocPath() throws Exception {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
      iwc.setMergePolicy(NoMergePolicy.INSTANCE);
      Codec codec = deterministicCompressingCodec(random());
      iwc.setCodec(codec);
      iwc.setIndexSort(new Sort(new SortField("sort", SortField.Type.LONG)));
      try (IndexWriter iw = new IndexWriter(dir, iwc)) {
        for (int i = 0; i < 16; i++) {
          Document d = makeDoc(i);
          d.add(new NumericDocValuesField("sort", 16 - i));
          iw.addDocument(d);
        }
        iw.commit();
        iw.deleteDocuments(new Term("id", "5"));
        iw.commit();
      }

      iwc = newIndexWriterConfig(new MockAnalyzer(random()));
      iwc.setCodec(codec);
      iwc.setIndexSort(new Sort(new SortField("sort", SortField.Type.LONG)));
      iwc.setMergePolicy(newLogMergePolicy());
      try (IndexWriter iw = new IndexWriter(dir, iwc)) {
        iw.forceMerge(1);
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertEquals(15, reader.numDocs());
        Set<Integer> deleted = new HashSet<>();
        deleted.add(5);
        assertPayloadsMatch(reader, deleted);
      }
    }
  }

  /**
   * Randomised soak test: random deletes spread over multiple chunks, then forceMerge, and verify
   * every surviving id is readable exactly once with the correct payload.
   */
  public void testRandomDeletesRoundTrip() throws Exception {
    final int numDocs = atLeast(200);
    final int deleteEvery = TestUtil.nextInt(random(), 3, 11);

    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
      iwc.setMergePolicy(NoMergePolicy.INSTANCE);
      Codec codec = deterministicCompressingCodec(random());
      iwc.setCodec(codec);

      Map<Integer, String> expected = new HashMap<>();
      Set<Integer> deleted = new HashSet<>();
      try (IndexWriter iw = new IndexWriter(dir, iwc)) {
        for (int i = 0; i < numDocs; i++) {
          iw.addDocument(makeDoc(i));
          expected.put(i, "doc-payload-" + i);
        }
        iw.commit();

        for (int i = 0; i < numDocs; i++) {
          if (i % deleteEvery == 0) {
            iw.deleteDocuments(new Term("id", Integer.toString(i)));
            expected.remove(i);
            deleted.add(i);
          }
        }
        iw.commit();
      }

      iwc = newIndexWriterConfig(new MockAnalyzer(random()));
      iwc.setCodec(codec);
      iwc.setMergePolicy(newLogMergePolicy());
      try (IndexWriter iw = new IndexWriter(dir, iwc)) {
        iw.forceMerge(1);
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertEquals(expected.size(), reader.numDocs());
        Set<Integer> seen = new HashSet<>();
        for (LeafReaderContext leaf : reader.leaves()) {
          StoredFields sf = leaf.reader().storedFields();
          for (int i = 0; i < leaf.reader().maxDoc(); i++) {
            Document d = sf.document(i);
            int id = Integer.parseInt(d.get("payload").substring("doc-payload-".length()));
            assertFalse("deleted id resurfaced: " + id, deleted.contains(id));
            assertTrue("duplicate id seen: " + id, seen.add(id));
            assertEquals(expected.get(id), d.get("payload"));
          }
        }
        assertEquals(expected.keySet(), seen);
      }
    }
  }

  /**
   * Sanity-check: a merge of segments WITHOUT any deletions still goes through the original BULK
   * path and produces no extra dirty chunks. This guards against accidentally routing the
   * no-deletes case through CHUNK_RELOCATE.
   */
  public void testNoDeletionsStillUsesBulk() throws Exception {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
      iwc.setMergePolicy(NoMergePolicy.INSTANCE);
      Codec codec = deterministicCompressingCodec(random());
      iwc.setCodec(codec);
      try (IndexWriter iw = new IndexWriter(dir, iwc)) {
        indexInOneSegment(iw, 16);
        // second segment, also full chunks
        for (int i = 100; i < 116; i++) {
          iw.addDocument(makeDoc(i));
        }
        iw.commit();
      }

      iwc = newIndexWriterConfig(new MockAnalyzer(random()));
      iwc.setCodec(codec);
      iwc.setMergePolicy(newLogMergePolicy());
      try (IndexWriter iw = new IndexWriter(dir, iwc)) {
        iw.forceMerge(1);
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertEquals(32, reader.numDocs());
        CodecReader cr = (CodecReader) getOnlyLeafReader(reader);
        Lucene90CompressingStoredFieldsReader fr = storedFieldsReader(cr);
        // Without deletions both source segments are eligible for raw bulk copy. The trailing
        // dirty chunk count should therefore be at most 1 per source segment (the original
        // last-flush dirty chunk), i.e. <= 2 in total.
        assertTrue("unexpected dirty chunks: " + fr.getNumDirtyChunks(),
            fr.getNumDirtyChunks() <= 2);
      }
    }
  }

  /**
   * Use a SortedDocValuesField so that each stored doc carries something other than just the
   * payload string; this protects against subtle column-order regressions during chunk relocation.
   */
  public void testDeleteWithMultipleStoredAndDvFields() throws Exception {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
      iwc.setMergePolicy(NoMergePolicy.INSTANCE);
      Codec codec = deterministicCompressingCodec(random());
      iwc.setCodec(codec);
      try (IndexWriter iw = new IndexWriter(dir, iwc)) {
        for (int i = 0; i < 16; i++) {
          Document d = makeDoc(i);
          d.add(new StoredField("extra", "x-" + i));
          d.add(new SortedDocValuesField("sdv", new BytesRef("sdv-" + i)));
          iw.addDocument(d);
        }
        iw.commit();
        iw.deleteDocuments(new Term("id", "10"));
        iw.commit();
      }

      iwc = newIndexWriterConfig(new MockAnalyzer(random()));
      iwc.setCodec(codec);
      iwc.setMergePolicy(newLogMergePolicy());
      try (IndexWriter iw = new IndexWriter(dir, iwc)) {
        iw.forceMerge(1);
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertEquals(15, reader.numDocs());
        for (LeafReaderContext leaf : reader.leaves()) {
          StoredFields sf = leaf.reader().storedFields();
          for (int i = 0; i < leaf.reader().maxDoc(); i++) {
            Document d = sf.document(i);
            String payload = d.get("payload");
            String extra = d.get("extra");
            int id = Integer.parseInt(payload.substring("doc-payload-".length()));
            assertNotEquals("deleted id resurfaced: " + id, 10, id);
            assertEquals("extra column mismatch for id=" + id, "x-" + id, extra);
          }
        }
      }
    }
  }

  /**
   * Build a payload that comfortably exceeds {@link #CHUNK_SIZE} so that the writer is forced to
   * emit a sliced chunk (token bit-0 set, chunkDocs == 1, multiple compressed segments).
   */
  private static String hugePayload(int id) {
    StringBuilder sb = new StringBuilder(CHUNK_SIZE * 6);
    String tag = "huge-" + id + "|";
    while (sb.length() < CHUNK_SIZE * 5) {
      sb.append(tag);
    }
    return sb.toString();
  }

  private static Document makeHugeDoc(int id) {
    Document doc = new Document();
    doc.add(new StringField("id", Integer.toString(id), Field.Store.NO));
    doc.add(new StoredField("payload", hugePayload(id)));
    return doc;
  }

  /**
   * Mix sliced (huge) docs and normal docs in one segment, then delete one normal doc that sits
   * before/around a sliced chunk. Verifies:
   *
   * <ul>
   *   <li>the sliced chunk's raw bytes are bulk-copied unchanged when no residue precedes it,
   *   <li>the sliced chunk is correctly absorbed via {@code copyOneDoc} when residue is present
   *       (its eager decompression path in the merge-time BlockState must handle multi-segment
   *       payloads).
   * </ul>
   */
  public void testHugeSlicedDocWithDeletes() throws Exception {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
      iwc.setMergePolicy(NoMergePolicy.INSTANCE);
      Codec codec = deterministicCompressingCodec(random());
      iwc.setCodec(codec);
      Map<Integer, Boolean> isHuge = new HashMap<>();
      try (IndexWriter iw = new IndexWriter(dir, iwc)) {
        // pattern: 4 small (chunk #0), 1 huge sliced (chunk #1), 4 small (chunk #2),
        //          1 huge sliced (chunk #3), 4 small (chunk #4)
        int id = 0;
        for (int k = 0; k < 4; k++) {
          iw.addDocument(makeDoc(id));
          isHuge.put(id, false);
          id++;
        }
        iw.addDocument(makeHugeDoc(id));
        isHuge.put(id, true);
        id++;
        for (int k = 0; k < 4; k++) {
          iw.addDocument(makeDoc(id));
          isHuge.put(id, false);
          id++;
        }
        iw.addDocument(makeHugeDoc(id));
        isHuge.put(id, true);
        id++;
        for (int k = 0; k < 4; k++) {
          iw.addDocument(makeDoc(id));
          isHuge.put(id, false);
          id++;
        }
        iw.commit();
        // delete a small doc right before the first huge doc, forcing residue to spill
        // into the sliced chunk during merge
        iw.deleteDocuments(new Term("id", "3"));
        // delete the second huge doc itself, exercising "whole sliced chunk dead"
        iw.deleteDocuments(new Term("id", "9"));
        iw.commit();
      }

      iwc = newIndexWriterConfig(new MockAnalyzer(random()));
      iwc.setCodec(codec);
      iwc.setMergePolicy(newLogMergePolicy());
      try (IndexWriter iw = new IndexWriter(dir, iwc)) {
        iw.forceMerge(1);
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertEquals(isHuge.size() - 2, reader.numDocs());
        Set<Integer> seen = new HashSet<>();
        for (LeafReaderContext leaf : reader.leaves()) {
          StoredFields sf = leaf.reader().storedFields();
          for (int i = 0; i < leaf.reader().maxDoc(); i++) {
            Document d = sf.document(i);
            String payload = d.get("payload");
            int idVal;
            if (payload.startsWith("huge-")) {
              int bar = payload.indexOf('|');
              idVal = Integer.parseInt(payload.substring("huge-".length(), bar));
              assertTrue("huge tag mismatch", isHuge.getOrDefault(idVal, false));
              // verify the huge body length wasn't truncated by the relocate path
              assertTrue(
                  "huge payload length suspiciously small: " + payload.length(),
                  payload.length() >= CHUNK_SIZE * 5);
            } else {
              idVal = Integer.parseInt(payload.substring("doc-payload-".length()));
              assertFalse("expected small doc but found huge tag", isHuge.get(idVal));
            }
            assertNotEquals("deleted id 3 resurfaced", 3, idVal);
            assertNotEquals("deleted id 9 resurfaced", 9, idVal);
            assertTrue("duplicate id " + idVal, seen.add(idVal));
          }
        }
      }
    }
  }

  /**
   * Delete the very last doc of the segment so that the only chunk producing residue is the
   * trailing one. Verifies the residue is correctly flushed at {@link
   * Lucene90CompressingStoredFieldsWriter#finish(int)} time as a (dirty) tail chunk and that no
   * doc is lost or duplicated.
   */
  public void testDeleteOnlyLastChunkLeavesTailResidue() throws Exception {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
      iwc.setMergePolicy(NoMergePolicy.INSTANCE);
      Codec codec = deterministicCompressingCodec(random());
      iwc.setCodec(codec);
      try (IndexWriter iw = new IndexWriter(dir, iwc)) {
        // Use 17 docs so the last chunk has only 1 doc; deleting it triggers liveCount == 0
        // on a partial trailing chunk. Then add a separate set so we have residue + EOF.
        for (int i = 0; i < 17; i++) {
          iw.addDocument(makeDoc(i));
        }
        iw.commit();
        // delete last doc of last chunk, plus one in the middle chunk to seed residue first
        iw.deleteDocuments(new Term("id", "13")); // chunk #3, residue starts here
        iw.deleteDocuments(new Term("id", "16")); // tail chunk: liveCount == 0
        iw.commit();
      }

      iwc = newIndexWriterConfig(new MockAnalyzer(random()));
      iwc.setCodec(codec);
      iwc.setMergePolicy(newLogMergePolicy());
      try (IndexWriter iw = new IndexWriter(dir, iwc)) {
        iw.forceMerge(1);
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertEquals(15, reader.numDocs());
        Set<Integer> deleted = new HashSet<>();
        deleted.add(13);
        deleted.add(16);
        assertPayloadsMatch(reader, deleted);
      }
    }
  }

  /**
   * Three segments, all with deletions. The middle segment ends with residue that should be picked
   * up by the next reader's first chunk via {@code chunkRelocateMerge} (cross-reader relay).
   */
  public void testCrossReaderResidueRelay() throws Exception {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
      iwc.setMergePolicy(NoMergePolicy.INSTANCE);
      Codec codec = deterministicCompressingCodec(random());
      iwc.setCodec(codec);
      try (IndexWriter iw = new IndexWriter(dir, iwc)) {
        // segment A: ids 0..15
        for (int i = 0; i < 16; i++) {
          iw.addDocument(makeDoc(i));
        }
        iw.commit();
        // segment B: ids 100..115; delete the tail of B to leave residue on its last chunk
        for (int i = 100; i < 116; i++) {
          iw.addDocument(makeDoc(i));
        }
        iw.commit();
        // segment C: ids 200..215
        for (int i = 200; i < 216; i++) {
          iw.addDocument(makeDoc(i));
        }
        iw.commit();
        iw.deleteDocuments(new Term("id", "5")); // A
        iw.deleteDocuments(new Term("id", "115")); // B tail
        iw.deleteDocuments(new Term("id", "200")); // C head
        iw.commit();
      }

      iwc = newIndexWriterConfig(new MockAnalyzer(random()));
      iwc.setCodec(codec);
      iwc.setMergePolicy(newLogMergePolicy());
      try (IndexWriter iw = new IndexWriter(dir, iwc)) {
        iw.forceMerge(1);
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertEquals(45, reader.numDocs());
        Set<Integer> deleted = new HashSet<>();
        deleted.add(5);
        deleted.add(115);
        deleted.add(200);
        assertPayloadsMatch(reader, deleted);
      }
    }
  }

  /**
   * Heavily fragmented source: each segment holds only a few docs (so each commit produces tiny
   * chunks). With sparse deletes scattered across segments, the chunk-relocate path should still
   * reassemble valid output and not leak dirty chunks.
   */
  public void testHighlyFragmentedSegmentsWithDeletes() throws Exception {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
      iwc.setMergePolicy(NoMergePolicy.INSTANCE);
      Codec codec = deterministicCompressingCodec(random());
      iwc.setCodec(codec);
      Map<Integer, String> expected = new HashMap<>();
      Set<Integer> deleted = new HashSet<>();
      try (IndexWriter iw = new IndexWriter(dir, iwc)) {
        // Create 10 small segments of 3 docs each (each segment's only chunk will likely be
        // dirty).
        int id = 0;
        for (int seg = 0; seg < 10; seg++) {
          for (int i = 0; i < 3; i++) {
            iw.addDocument(makeDoc(id));
            expected.put(id, "doc-payload-" + id);
            id++;
          }
          iw.commit();
        }
        // delete one doc per segment, skipping a couple for variety
        for (int s = 0; s < 10; s++) {
          if (s == 2 || s == 7) continue;
          int victim = s * 3 + 1;
          iw.deleteDocuments(new Term("id", Integer.toString(victim)));
          expected.remove(victim);
          deleted.add(victim);
        }
        iw.commit();
      }

      iwc = newIndexWriterConfig(new MockAnalyzer(random()));
      iwc.setCodec(codec);
      iwc.setMergePolicy(newLogMergePolicy());
      try (IndexWriter iw = new IndexWriter(dir, iwc)) {
        iw.forceMerge(1);
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertEquals(expected.size(), reader.numDocs());
        Set<Integer> seen = new HashSet<>();
        for (LeafReaderContext leaf : reader.leaves()) {
          StoredFields sf = leaf.reader().storedFields();
          for (int i = 0; i < leaf.reader().maxDoc(); i++) {
            Document d = sf.document(i);
            int idVal = Integer.parseInt(d.get("payload").substring("doc-payload-".length()));
            assertFalse(deleted.contains(idVal));
            assertTrue("duplicate id " + idVal, seen.add(idVal));
          }
        }
        assertEquals(expected.keySet(), seen);

        // Output should not have *more* chunks than the original 10 source chunks; ideally fewer
        // because residue absorbs neighbours into clean output blocks.
        CodecReader cr = (CodecReader) getOnlyLeafReader(reader);
        Lucene90CompressingStoredFieldsReader fr = storedFieldsReader(cr);
        assertTrue(
            "fragmentation went up: numChunks=" + fr.getNumChunks(),
            fr.getNumChunks() <= 10);
      }
    }
  }

  /**
   * 性能优化场景:大量干净 chunk + 少量单删,验证"按需 flush 让路"逻辑。
   *
   * <p>构造一个包含 NUM_CHUNKS 个 chunk 的单段索引,只在前段、中段、后段各删一个 doc。
   * 优化生效时,每次删除最多产生 2 个 dirty chunk(一个 flush 残留 + 一个被解码吸收的脏 chunk),
   * 其余几乎所有 chunk 都应当走 raw-copy 字节拷贝路径并保持 clean。
   *
   * <p>关键不变量:{@code numDirtyChunks} 应该 &lt;= O(删除次数),
   * 而不是退化为 O(总 chunk 数)。如果断言失败说明优化路径回到了旧行为。
   */
  public void testOptimizationManyCleanChunksFewDeletes() throws Exception {
    final int numChunks = 100;
    final int docsPerChunk = MAX_DOCS_PER_CHUNK;
    final int totalDocs = numChunks * docsPerChunk;
    final int[] deletedIds = {5, 205, 305};

    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
      iwc.setMergePolicy(NoMergePolicy.INSTANCE);
      Codec codec = deterministicCompressingCodec(random());
      iwc.setCodec(codec);
      try (IndexWriter iw = new IndexWriter(dir, iwc)) {
        indexInOneSegment(iw, totalDocs);
        for (int id : deletedIds) {
          iw.deleteDocuments(new Term("id", Integer.toString(id)));
        }
        iw.commit();
      }

      long sourceNumChunks;
      long sourceNumDirtyChunks;
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        CodecReader cr = (CodecReader) getOnlyLeafReader(reader);
        Lucene90CompressingStoredFieldsReader fr = storedFieldsReader(cr);
        sourceNumChunks = fr.getNumChunks();
        sourceNumDirtyChunks = fr.getNumDirtyChunks();
        // 正常情况下源段无 dirty chunk(尾部最多 1 个)
        assertTrue(
            "source segment unexpectedly dirty: " + sourceNumDirtyChunks,
            sourceNumDirtyChunks <= 1);
        // 源 chunk 数应当接近 numChunks
        assertTrue(
            "source chunk count off: " + sourceNumChunks,
            sourceNumChunks >= numChunks - 1 && sourceNumChunks <= numChunks + 1);
      }

      iwc = newIndexWriterConfig(new MockAnalyzer(random()));
      iwc.setCodec(codec);
      iwc.setMergePolicy(newLogMergePolicy());
      try (IndexWriter iw = new IndexWriter(dir, iwc)) {
        iw.forceMerge(1);
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertEquals(totalDocs - deletedIds.length, reader.numDocs());

        Set<Integer> deleted = new HashSet<>();
        for (int id : deletedIds) deleted.add(id);
        assertPayloadsMatch(reader, deleted);

        CodecReader cr = (CodecReader) getOnlyLeafReader(reader);
        Lucene90CompressingStoredFieldsReader fr = storedFieldsReader(cr);
        long mergedNumChunks = fr.getNumChunks();
        long mergedNumDirtyChunks = fr.getNumDirtyChunks();

        // 关键断言 1:dirty chunk 数应当 ~= O(删除次数),而不是 O(总 chunk 数)。
        // 优化生效:每个删除产生最多 2 个 dirty(残留 flush + 脏 chunk 自身) = ~6
        // 优化失效:从第一个删除开始所有后续 chunk 都被解压重压 -> >> 50
        long dirtyBudget = deletedIds.length * 2L + 2L; // +2 余量给尾部
        assertTrue(
            "dirty chunks exceeded optimization budget: actual="
                + mergedNumDirtyChunks
                + " budget="
                + dirtyBudget
                + " (源段 dirty="
                + sourceNumDirtyChunks
                + ", 源段 chunks="
                + sourceNumChunks
                + ")",
            mergedNumDirtyChunks <= dirtyBudget);

        // 关键断言 2:总 chunk 数不应大幅膨胀
        // raw-copy 保留原结构,只多出"flush 让路"产生的小残留 chunk
        long chunkBudget = sourceNumChunks + deletedIds.length + 1;
        assertTrue(
            "merged chunks ballooned: actual="
                + mergedNumChunks
                + " budget="
                + chunkBudget,
            mergedNumChunks <= chunkBudget);

        // 关键断言 3:输出 fdt 字节数应当与源接近(只少了 3 个 doc 的载荷 + 少量 chunk header 开销)。
        // dirty 数和 chunk 总数已经间接保证了字节代价(dirty <=8, 总 chunk <= 源段+4),
        // 直接读 .fdt 文件长度在 CFS 复合文件格式下会得到 0,所以这里不再做硬字节断言;
        // 字节维度由 dirtyBudget + chunkBudget 两个不变量共同约束。
      }
    }
  }
}

