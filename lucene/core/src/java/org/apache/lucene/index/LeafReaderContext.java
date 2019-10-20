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


import java.util.Collections;
import java.util.List;

/**
 * {@link IndexReaderContext} for {@link LeafReader} instances.
 */ //LeafReaderContext 即用来描述某一个段的信息，并且通过它能获得一个 LeafCollector 对象
public final class LeafReaderContext extends IndexReaderContext {
  /** The reader's ord in the top-level's leaves array */
  public final int ord; //
  /** The reader's absolute doc base */
  public final int docBase; // 这个segment的文档起点，为啥需要对同一个shard的所有segment都要分配一个起始绝对Id呢，是为了保证唯一性，分片节点向协调节点返回的是docId， 这个DocId是Lucene上里面每个semgment的Id,并不是ES层面的Id
  // 当协调节点向shard请求这些Id时，绝对Id就是为了区分这个id是哪个Semgent的
  private final LeafReader reader;// shard粒度的，es查询时，可以是ExitableLeafReader。es别的地方可以使:ElasticsearchLeafReader
  private final List<LeafReaderContext> leaves;
  
  /**
   * Creates a new {@link LeafReaderContext} 
   */    
  LeafReaderContext(CompositeReaderContext parent, LeafReader reader,
                    int ord, int docBase, int leafOrd, int leafDocBase) {
    super(parent, ord, docBase);
    this.ord = leafOrd;
    this.docBase = leafDocBase; // 这个segment的文档起点,在CompositeReaderContext中构建IndexReaderContext时，对每个segment都分配一个起始docId
    this.reader = reader;
    this.leaves = isTopLevel ? Collections.singletonList(this) : null;
  }
  
  LeafReaderContext(LeafReader leafReader) {
    this(null, leafReader, 0, 0, 0, 0); // order是0
  }
  
  @Override
  public List<LeafReaderContext> leaves() {
    if (!isTopLevel) {
      throw new UnsupportedOperationException("This is not a top-level context.");
    }
    assert leaves != null;
    return leaves;
  }
  
  @Override
  public List<IndexReaderContext> children() {
    return null;
  }
  
  @Override
  public LeafReader reader() {
    return reader; // SegmentReader，查询时可以是ExitableLeafReader，refresh时候可以是ElasticsearchLeafReader
  }

  @Override
  public String toString() {
    return "LeafReaderContext(" + reader + " docBase=" + docBase + " ord=" + ord + ")";
  }
}
