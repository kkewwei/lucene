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
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.TermsEnum.SeekStatus;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.codecs.compressing.CompressingCodec;
import org.apache.lucene.tests.index.BaseTermVectorsFormatTestCase;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;

public class TestCompressingTermVectorsFormat extends BaseTermVectorsFormatTestCase {

  @Override
  protected Codec getCodec() {
    if (TEST_NIGHTLY) {
      return CompressingCodec.randomInstance(random());
    } else {
      return CompressingCodec.reasonableInstance(random());
    }
  }

  private static Document makeDoc(int id) {
    Document doc = new Document();
    doc.add(new StringField("id", Integer.toString(id), Field.Store.NO));
    doc.add(new StoredField("payload", "doc-payload-" + id));
    return doc;
  }

  private void indexInOneSegment(IndexWriter iw, int numDocs) throws IOException {
    for (int i = 0; i < numDocs; i++) {
      iw.addDocument(makeDoc(i));
    }
    iw.commit();
  }
  public static final String SOFT_DELETES_FIELD = "__soft_deletes";
  protected final NumericDocValuesField softDeletesField = new NumericDocValuesField(SOFT_DELETES_FIELD, 1);
  private static final int CHUNK_SIZE = 4 * 1024;
  /** force chunk boundaries every 4 docs so we can pinpoint deletions */
  private static final int MAX_DOCS_PER_CHUNK = 4;

  private static final int BLOCK_SHIFT = 8;
  private Codec deterministicCompressingCodec(Random r) {
    return CompressingCodec.randomInstance(
      r, CHUNK_SIZE, MAX_DOCS_PER_CHUNK, false, BLOCK_SHIFT);
  }

  public void testDeleteFirstDocOfChunk() throws Exception {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
      iwc.setMergePolicy(NoMergePolicy.INSTANCE);
      iwc.setCodec(deterministicCompressingCodec(random()));
      iwc.setSoftDeletesField("SOFT_DELETES_FIELD");

      try (IndexWriter iw = new IndexWriter(dir, iwc)) {
        indexInOneSegment(iw, 16);
        // first doc of chunk #2
        iw.deleteDocuments(new Term("id", "8"));

        Document v2 = new Document();
        v2.add(new StringField("id", "7", Field.Store.YES));
        v2.add(softDeletesField);
        iw.softUpdateDocument(new Term("id", "7"), v2, softDeletesField);
        iw.commit();
      }

      iwc = newIndexWriterConfig(new MockAnalyzer(random()));
      iwc.setCodec(deterministicCompressingCodec(random()));
      iwc.setMergePolicy(newLogMergePolicy());
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


  // https://issues.apache.org/jira/browse/LUCENE-5156
  public void testNoOrds() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
    ft.setStoreTermVectors(true);
    doc.add(new Field("foo", "this is a test", ft));
    iw.addDocument(doc);
    LeafReader ir = getOnlyLeafReader(iw.getReader());
    Terms terms = ir.termVectors().get(0, "foo");
    assertNotNull(terms);
    TermsEnum termsEnum = terms.iterator();
    assertEquals(SeekStatus.FOUND, termsEnum.seekCeil(new BytesRef("this")));

    expectThrows(UnsupportedOperationException.class, termsEnum::ord);
    expectThrows(UnsupportedOperationException.class, () -> termsEnum.seekExact(0));

    ir.close();
    iw.close();
    dir.close();
  }

  /**
   * writes some tiny segments with incomplete compressed blocks, and ensures merge recompresses
   * them.
   */
  public void testChunkCleanup() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwConf = newIndexWriterConfig(new MockAnalyzer(random()));
    iwConf.setMergePolicy(NoMergePolicy.INSTANCE);

    // we have to enforce certain things like maxDocsPerChunk to cause dirty chunks to be created
    // by this test.
    iwConf.setCodec(CompressingCodec.randomInstance(random(), 4 * 1024, 4, false, 8));
    IndexWriter iw = new IndexWriter(dir, iwConf);
    DirectoryReader ir = DirectoryReader.open(iw);
    for (int i = 0; i < 5; i++) {
      Document doc = new Document();
      FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
      ft.setStoreTermVectors(true);
      doc.add(new Field("text", "not very long at all", ft));
      iw.addDocument(doc);
      // force flush
      DirectoryReader ir2 = DirectoryReader.openIfChanged(ir);
      assertNotNull(ir2);
      ir.close();
      ir = ir2;
      // examine dirty counts:
      for (LeafReaderContext leaf : ir2.leaves()) {
        CodecReader sr = (CodecReader) leaf.reader();
        Lucene90CompressingTermVectorsReader reader =
            (Lucene90CompressingTermVectorsReader) sr.getTermVectorsReader();
        assertTrue(reader.getNumDirtyDocs() > 0);
        assertEquals(1, reader.getNumDirtyChunks());
      }
    }
    iw.getConfig().setMergePolicy(newLogMergePolicy());
    iw.forceMerge(1);
    // add one more doc and merge again
    Document doc = new Document();
    FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
    ft.setStoreTermVectors(true);
    doc.add(new Field("text", "not very long at all", ft));
    iw.addDocument(doc);
    iw.forceMerge(1);
    DirectoryReader ir2 = DirectoryReader.openIfChanged(ir);
    assertNotNull(ir2);
    ir.close();
    ir = ir2;
    CodecReader sr = (CodecReader) getOnlyLeafReader(ir);
    Lucene90CompressingTermVectorsReader reader =
        (Lucene90CompressingTermVectorsReader) sr.getTermVectorsReader();
    // at most 2: the 5 chunks from 5 doc segment will be collapsed into a single chunk
    assertTrue(reader.getNumDirtyChunks() <= 2);
    ir.close();
    iw.close();
    dir.close();
  }
}
