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
package org.apache.lucene.benchmark.jmh;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/** Benchmarks string sort pruning at different search-after depths. */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 10, time = 2)
@Fork(value = 1, warmups = 1)
public class StringSearchAfterBenchmark {

  private static final String FIELD = "sort";
  private static final int DOC_COUNT = 500_000;
  private static final int CARDINALITY = 512;
  private static final int TOP_N = 10;
  private static final String INDEX_ROOT_ENV = "STRING_SEARCH_AFTER_INDEX_ROOT";

  @Param({"POSTINGS", "DV_SKIPPER"})
  String implementation;

  @Param({"1", "25", "50", "75", "90", "99.9"})
  double afterPercent;

  private Path path;
  private Directory directory;
  private IndexReader reader;
  private IndexSearcher searcher;
  private Query query;
  private Sort sort;
  private FieldDoc after;
  private boolean deleteIndex;

  @Setup(Level.Trial)
  public void setup() throws Exception {
    String indexRoot = System.getenv(INDEX_ROOT_ENV);
    if (indexRoot == null) {
      path = Files.createTempDirectory("stringSearchAfter");
      deleteIndex = true;
    } else {
      path = Path.of(indexRoot, implementation.toLowerCase(Locale.ROOT));
      Files.createDirectories(path);
    }
    directory = MMapDirectory.open(path);

    if (DirectoryReader.indexExists(directory) == false) {
      try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
        for (int docID = 0; docID < DOC_COUNT; docID++) {
          BytesRef value = value(docID);
          Document document = new Document();
          if (implementation.equals("POSTINGS")) {
            document.add(new KeywordField(FIELD, value, Field.Store.NO));
          } else {
            document.add(SortedDocValuesField.indexedField(FIELD, value));
          }
          writer.addDocument(document);
        }
        writer.forceMerge(1);
      }
    }
    reader = DirectoryReader.open(directory);

    searcher = new IndexSearcher(reader);
    searcher.setQueryCache(null);
    query = new MatchAllDocsQuery();
    sort =
        new Sort(
            KeywordField.newSortField(
                FIELD, false, SortedSetSelector.Type.MIN, SortField.STRING_FIRST));

    int afterDoc = Math.max(0, (int) Math.floor(DOC_COUNT * afterPercent / 100d) - 1);
    after = new FieldDoc(afterDoc, Float.NaN, new Object[] {value(afterDoc)});

    TopDocs hits = searcher.searchAfter(after, query, TOP_N, sort);
    if (hits.scoreDocs.length != TOP_N) {
      throw new IllegalStateException(
          "expected " + TOP_N + " hits but got " + hits.scoreDocs.length);
    }
  }

  @Benchmark
  public TopDocs searchAfter() throws IOException {
    return searcher.searchAfter(after, query, TOP_N, sort);
  }

  private static BytesRef value(int docID) {
    int ord = (int) ((long) docID * CARDINALITY / DOC_COUNT);
    return new BytesRef(String.format(Locale.ROOT, "%04d", ord));
  }

  @TearDown(Level.Trial)
  public void tearDown() throws Exception {
    reader.close();
    directory.close();
    if (deleteIndex) {
      try (Stream<Path> files = Files.walk(path)) {
        files.sorted(Comparator.reverseOrder())
            .forEach(
                file -> {
                  try {
                    Files.delete(file);
                  } catch (IOException ioe) {
                    throw new RuntimeException(ioe);
                  }
                });
      }
    }
  }
}
