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
package org.apache.lucene.search.comparators;

import java.io.IOException;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

public class TestTermOrdValComparator extends LuceneTestCase {

  private static final String FIELD = "field";
  private static final int MISSING_DOC = 0;
  private static final int ANT_DOC = 1;
  private static final int BEE_DOC = 2;
  private static final int CAT_DOC = 3;
  private static final int DOG_DOC = 4;
  private static final int EEL_DOC = 5;

  private Directory directory;
  private DirectoryReader reader;
  private LeafReaderContext leafContext;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    directory = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig();
    try (IndexWriter writer = new IndexWriter(directory, config)) {
      writer.addDocument(new Document());
      addDocument(writer, "ant");
      addDocument(writer, "bee");
      addDocument(writer, "cat");
      addDocument(writer, "dog");
      addDocument(writer, "eel");
      writer.forceMerge(1);
    }
    reader = DirectoryReader.open(directory);
    assertEquals(1, reader.leaves().size());
    leafContext = reader.leaves().get(0);
  }

  @Override
  public void tearDown() throws Exception {
    IOUtils.close(reader, directory);
    super.tearDown();
  }

  public void testSkippingDisabledWhenMissingValuesSortBestWithTieBreaker() throws Exception {
    TermOrdValComparator comparator =
        new TermOrdValComparator(1, FIELD, false, false, Pruning.GREATER_THAN);

    assertNull(comparator.getLeafComparator(leafContext).competitiveIterator());
  }

  public void testPagingAfterMissingDoesNotEnableSkipping() throws Exception {
    TermOrdValComparator comparator =
        new TermOrdValComparator(1, FIELD, false, false, Pruning.GREATER_THAN_OR_EQUAL_TO);
    comparator.setTopValue(null);

    assertNull(comparator.getLeafComparator(leafContext).competitiveIterator());
  }

  public void testMissingValuesStopCompetingWithoutTieBreaker() throws Exception {
    TermOrdValComparator comparator =
        new TermOrdValComparator(1, FIELD, false, false, Pruning.GREATER_THAN_OR_EQUAL_TO);
    LeafFieldComparator leafComparator = comparator.getLeafComparator(leafContext);

    setBottomAndThreshold(leafComparator, MISSING_DOC);

    assertCompetitiveDocs(leafComparator, ANT_DOC, BEE_DOC, CAT_DOC, DOG_DOC, EEL_DOC);
  }

  public void testAscendingRangeWithNonExactTop() throws Exception {
    TermOrdValComparator comparator =
        new TermOrdValComparator(1, FIELD, false, false, Pruning.GREATER_THAN);
    comparator.setTopValue(new BytesRef("bat"));
    LeafFieldComparator leafComparator = comparator.getLeafComparator(leafContext);

    setBottomAndThreshold(leafComparator, DOG_DOC);

    assertCompetitiveDocs(leafComparator, BEE_DOC, CAT_DOC, DOG_DOC);
  }

  public void testDescendingRangeWithNonExactTop() throws Exception {
    TermOrdValComparator comparator =
        new TermOrdValComparator(1, FIELD, false, true, Pruning.GREATER_THAN);
    comparator.setTopValue(new BytesRef("cow"));
    LeafFieldComparator leafComparator = comparator.getLeafComparator(leafContext);

    setBottomAndThreshold(leafComparator, BEE_DOC);

    assertCompetitiveDocs(leafComparator, BEE_DOC, CAT_DOC);
  }

  public void testBottomEqualityIsPrunedWithoutTieBreaker() throws Exception {
    TermOrdValComparator comparator =
        new TermOrdValComparator(1, FIELD, true, false, Pruning.GREATER_THAN_OR_EQUAL_TO);
    LeafFieldComparator leafComparator = comparator.getLeafComparator(leafContext);

    setBottomAndThreshold(leafComparator, DOG_DOC);

    assertCompetitiveDocs(leafComparator, ANT_DOC, BEE_DOC, CAT_DOC);
  }

  private static void addDocument(IndexWriter writer, String value) throws IOException {
    Document document = new Document();
    document.add(new StringField(FIELD, value, Field.Store.NO));
    document.add(new SortedDocValuesField(FIELD, new BytesRef(value)));
    writer.addDocument(document);
  }

  private static void setBottomAndThreshold(LeafFieldComparator comparator, int bottomDoc)
      throws IOException {
    comparator.copy(0, bottomDoc);
    comparator.setBottom(0);
    comparator.setHitsThresholdReached();
  }

  private static void assertCompetitiveDocs(LeafFieldComparator comparator, int... expectedDocs)
      throws IOException {
    DocIdSetIterator iterator = comparator.competitiveIterator();
    assertNotNull(iterator);
    for (int expectedDoc : expectedDocs) {
      assertEquals(expectedDoc, iterator.nextDoc());
    }
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, iterator.nextDoc());
  }
}
