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
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Locale;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.lucene.codecs.lucene90.LZ4WithPresetDictCompressionMode;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.SuppressForbidden;
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
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.runner.IterationType;

@State(Scope.Thread)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 0)
@Measurement(iterations = 1)
@Fork(
    value = 1,
    jvmArgsAppend = {
      "-Xmx2g",
      "-Xms2g",
      "-XX:+AlwaysPreTouch",
      "-Dlucene.lz4.presetDict.metrics=true"
    })
public class StoredFieldsReuseBenchmark {

  private static final int NUM_DOCS = 1_000_000;

  @Param({"100", "200", "1000", "5000", "10000"})
  public int numDocsToLoad;

  private Path path;
  private Directory dir;
  private DirectoryReader reader;
  private LeafReader leaf;
  private int[] docs;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    path = Files.createTempDirectory("storedFieldsReuse");
    dir = MMapDirectory.open(path);

    byte[] source = new byte[1024];
    new Random(0).nextBytes(source);
    IndexWriterConfig config = new IndexWriterConfig();
    config.setMergePolicy(NoMergePolicy.INSTANCE);
    config.setRAMBufferSizeMB(2048);
    try (IndexWriter writer = new IndexWriter(dir, config)) {
      Document document = new Document();
      document.add(new StoredField("_source", source));
      for (int doc = 0; doc < NUM_DOCS; doc++) {
        writer.addDocument(document);
      }
    }

    reader = DirectoryReader.open(dir);
    if (reader.leaves().size() != 1) {
      throw new IllegalStateException("expected one segment, got " + reader.leaves().size());
    }
    leaf = reader.leaves().get(0).reader();
    docs = createDocs(numDocsToLoad);
  }

  @Setup(Level.Iteration)
  public void resetMetrics() {
    LZ4WithPresetDictCompressionMode.resetDictionaryReuseMetrics();
  }

  @TearDown(Level.Iteration)
  @SuppressForbidden(reason = "System.out required for benchmark metrics")
  public void printMetrics(IterationParams iterationParams) {
    if (iterationParams.getType() == IterationType.MEASUREMENT) {
      var metrics = LZ4WithPresetDictCompressionMode.getDictionaryReuseMetrics();
      long total = metrics.loads() + metrics.reuses();
      System.out.printf(
          Locale.ROOT,
          "LZ4 preset dictionary metrics: total=%d, reused=%d, loaded=%d, reuseRatio=%.2f%%,"
              + " compressedDictionaryBytesSkipped=%d%n",
          total,
          metrics.reuses(),
          metrics.loads(),
          100d * metrics.reuses() / total,
          metrics.compressedBytesSkipped());
    }
  }

  @TearDown(Level.Trial)
  public void tearDown() throws IOException {
    reader.close();
    dir.close();
    try (Stream<Path> walk = Files.walk(path)) {
      walk.sorted(Comparator.reverseOrder())
          .forEach(
              p -> {
                try {
                  Files.delete(p);
                } catch (IOException _) {
                }
              });
    }
  }

  @Benchmark
  public long fetchTopDocs() throws IOException {
    StoredFields storedFields = leaf.storedFields();
    long bytes = 0;
    for (int doc : docs) {
      bytes += storedFields.document(doc).getBinaryValue("_source").length;
    }
    return bytes;
  }

  private static int[] createDocs(int numDocsToLoad) {
    Random random = new Random(0);
    Set<Integer> selected = new HashSet<>();
    int[] docs = new int[numDocsToLoad];
    int count = 0;
    while (count < docs.length) {
      int doc = random.nextInt(NUM_DOCS);
      if (selected.add(doc)) {
        docs[count++] = doc;
      }
    }
    Arrays.sort(docs);
    return docs;
  }
}
