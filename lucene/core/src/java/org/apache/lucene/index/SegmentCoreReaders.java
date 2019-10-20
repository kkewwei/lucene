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


import java.io.Closeable;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CompoundDirectory;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.index.IndexReader.CacheKey;
import org.apache.lucene.index.IndexReader.ClosedListener;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.CloseableThreadLocal;
import org.apache.lucene.util.IOUtils;

/** Holds core readers that are shared (unchanged) when
 * SegmentReader is cloned or reopened */
final class SegmentCoreReaders {

  // Counts how many other readers share the core objects
  // (freqStream, proxStream, tis, etc.) of this reader;
  // when coreRef drops to 0, these core objects may be
  // closed.  A given instance of SegmentReader may be
  // closed, even though it shares core objects with other
  // SegmentReaders:
  private final AtomicInteger ref = new AtomicInteger(1); // 多少个其余Reader共享这一个
  
  final FieldsProducer fields;  // PerFieldPostingsFormat.$FieldsReader  里面有每个字段的BlockTreeTermsReader
  final NormsProducer normsProducer;

  final StoredFieldsReader fieldsReaderOrig;  // CompressingStoredFieldsReader 映射的fdt
  final TermVectorsReader termVectorsReaderOrig;
  final PointsReader pointsReader;// Lucene60PointsReader
  final CompoundDirectory cfsReader; // Lucene50CompoundReader，只是检查了文件头，并没有读完
  final String segment;// _7
  /** 
   * fieldinfos for this core: means gen=-1.
   * this is the exact fieldinfos these codec components saw at write.
   * in the case of DV updates, SR may hold a newer version. */
  final FieldInfos coreFieldInfos;  // 每个字段的属性，就是从fnm中读取的

  // TODO: make a single thread local w/ a
  // Thingy class holding fieldsReader, termVectorsReader,
  // normsProducer
  // 这里使用了缓存，为啥需要使用缓存呢
  final CloseableThreadLocal<StoredFieldsReader> fieldsReaderLocal = new CloseableThreadLocal<StoredFieldsReader>() {
    @Override
    protected StoredFieldsReader initialValue() {
      return fieldsReaderOrig.clone(); // CompressingStoredFieldsReader，是上面那个对象的克隆。
    }
  };
  
  final CloseableThreadLocal<TermVectorsReader> termVectorsLocal = new CloseableThreadLocal<TermVectorsReader>() {
    @Override
    protected TermVectorsReader initialValue() {
      return (termVectorsReaderOrig == null) ? null : termVectorsReaderOrig.clone();
    }
  };

  private final Set<IndexReader.ClosedListener> coreClosedListeners = // 当资源关闭时的回调函数 
      Collections.synchronizedSet(new LinkedHashSet<IndexReader.ClosedListener>());
  
  SegmentCoreReaders(Directory dir, SegmentCommitInfo si, IOContext context) throws IOException {
    // codec=Lucene86Codec
    final Codec codec = si.info.getCodec();  // PerFieldMappingPostingFormatCodec(Lucene86)
    final Directory cfsDir; // confusing name: if (cfs) it's the cfsdir, otherwise it's the segment's directory.
    boolean success = false;
    
    try {
      if (si.info.getUseCompoundFile()) { // 是否是复合文件
        cfsDir = cfsReader = codec.compoundFormat().getCompoundReader(dir, si.info, context); // Lucene50CompoundReader,部分文件使用mmap打开了
      } else {
        cfsReader = null;
        cfsDir = dir;
      }

      segment = si.info.name;  //_7
      //首先从.fnm获取每个字段的属性。   复合文件：_Lucene50_0.tip  .nvm  .fnm   .fdt   _Lucene50_0.pos  .nvd   _Lucene50_0.tim   .fdx   _Lucene50_0.doc
      coreFieldInfos = codec.fieldInfosFormat().read(cfsDir, si.info, "", context);
      
      final SegmentReadState segmentReadState = new SegmentReadState(cfsDir, si.info, coreFieldInfos, context);
      final PostingsFormat format = codec.postingsFormat();
      // Ask codec for its Fields 会去读每个字段的fst结构
      fields = format.fieldsProducer(segmentReadState); // PerFieldPostingsFormat.$FieldsReader，给每个字段分配BlockTreeTermsReader
      assert fields != null;
      // ask codec for its Norms: 
      // TODO: since we don't write any norms file if there are no norms,
      // kinda jaky to assume the codec handles the case of no norms file at all gracefully?!

      if (coreFieldInfos.hasNorms()) { //
        normsProducer = codec.normsFormat().normsProducer(segmentReadState);
        assert normsProducer != null; // 获取的是_7.nvd文件每个字段的属性。
      } else {
        normsProducer = null;
      }
      // CompressingStoredFieldsFormatReader: 获取的是fdx和fdt文件的数据
      fieldsReaderOrig = si.info.getCodec().storedFieldsFormat().fieldsReader(cfsDir, si.info, coreFieldInfos, context);

      if (coreFieldInfos.hasVectors()) { // open term vector files only as needed
        termVectorsReaderOrig = si.info.getCodec().termVectorsFormat().vectorsReader(cfsDir, si.info, coreFieldInfos, context);
      } else {
        termVectorsReaderOrig = null;
      }

      if (coreFieldInfos.hasPointValues()) {
        pointsReader = codec.pointsFormat().fieldsReader(segmentReadState); // 对所有的Point类型数据进行读取
      } else {
        pointsReader = null;
      }
      success = true;
    } catch (EOFException | FileNotFoundException e) {
      throw new CorruptIndexException("Problem reading index from " + dir, dir.toString(), e);
    } catch (NoSuchFileException e) {
      throw new CorruptIndexException("Problem reading index.", e.getFile(), e);
    } finally {
      if (!success) {
        decRef();
      }
    }
  }
  
  int getRefCount() {
    return ref.get();
  }
  
  void incRef() {
    int count;
    while ((count = ref.get()) > 0) {
      if (ref.compareAndSet(count, count+1)) {
        return;
      }
    }
    throw new AlreadyClosedException("SegmentCoreReaders is already closed");
  }

  @SuppressWarnings("try")
  void decRef() throws IOException {
    if (ref.decrementAndGet() == 0) {
      Throwable th = null;
      try (Closeable finalizer = this::notifyCoreClosedListeners){
        IOUtils.close(termVectorsLocal, fieldsReaderLocal, fields, termVectorsReaderOrig, fieldsReaderOrig,
                      cfsReader, normsProducer, pointsReader);
      }
    }
  }
  // 分别都cache的啥
  private final IndexReader.CacheHelper cacheHelper = new IndexReader.CacheHelper() {
    private final IndexReader.CacheKey cacheKey = new IndexReader.CacheKey();

    @Override
    public CacheKey getKey() {
      return cacheKey;
    }

    @Override
    public void addClosedListener(ClosedListener listener) {
      coreClosedListeners.add(listener);
    }
  };

  IndexReader.CacheHelper getCacheHelper() {
    return cacheHelper;
  }

  private void notifyCoreClosedListeners() throws IOException {
    synchronized(coreClosedListeners) {
      IOUtils.applyToAll(coreClosedListeners, l -> l.onClose(cacheHelper.getKey()));
    }
  }

  @Override
  public String toString() {
    return "SegmentCoreReader(" + segment + ")";
  }
}
