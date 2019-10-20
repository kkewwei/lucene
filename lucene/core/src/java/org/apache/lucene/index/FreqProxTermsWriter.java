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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;
// 写_X.tim,_X.tip, _X.doc, _X.pos文件。
final class FreqProxTermsWriter extends TermsHash {
 // segment内固定
  public FreqProxTermsWriter(DocumentsWriterPerThread docWriter, TermsHash termVectors) {
    super(docWriter, true, termVectors);
  }

  private void applyDeletes(SegmentWriteState state, Fields fields) throws IOException {
    // Process any pending Term deletes for this newly
    // flushed segment:
    if (state.segUpdates != null && state.segUpdates.deleteTerms.size() > 0) { //对于term类的删除
      Map<Term,Integer> segDeletes = state.segUpdates.deleteTerms;
      List<Term> deleteTerms = new ArrayList<>(segDeletes.keySet());
      Collections.sort(deleteTerms);
      FrozenBufferedUpdates.TermDocsIterator iterator = new FrozenBufferedUpdates.TermDocsIterator(fields, true);
      for(Term deleteTerm : deleteTerms) { // 遍历每一个deleteTerm
        DocIdSetIterator postings = iterator.nextTerm(deleteTerm.field(), deleteTerm.bytes());
        if (postings != null ) { // 找到了这个
          int delDocLimit = segDeletes.get(deleteTerm); // 第一次删除时，内存中多少delete没有来得及处理，(之后来的都已经被处理了?)
          assert delDocLimit < PostingsEnum.NO_MORE_DOCS;
          int doc; // delDocLimit之后的文件不用再考虑删除。
          while ((doc = postings.nextDoc()) < delDocLimit) { // 主要是统计有多少文档需要删除+把不删除的文档仍然保存在liveDocs中
            if (state.liveDocs == null) { // 第一次置位
              state.liveDocs = new FixedBitSet(state.segmentInfo.maxDoc());
              state.liveDocs.set(0, state.segmentInfo.maxDoc()); // 置位0-maxDoc位
            }
            if (state.liveDocs.get(doc)) {
              state.delCountOnFlush++;
              state.liveDocs.clear(doc); // 仅仅是把需要删除的文档从live中去掉
            }
          }
        }
      }
    }
  }
  // 写tvd、tvm文件，然后在写tip、tim文件
  @Override
  public void flush(Map<String,TermsHashPerField> fieldsToFlush, final SegmentWriteState state,
      Sorter.DocMap sortMap, NormsProducer norms) throws IOException {
    super.flush(fieldsToFlush, state, sortMap, norms); // tvd、tvm文件

    // Gather all fields that saw any postings:
    List<FreqProxTermsWriterPerField> allFields = new ArrayList<>(); // 就四个域

    for (TermsHashPerField f : fieldsToFlush.values()) { // 就4个域
      final FreqProxTermsWriterPerField perField = (FreqProxTermsWriterPerField) f;
      if (perField.getNumTerms() > 0) {// FreqProxTermsWriterPerField里面存储的是所有文档该域所有distinct(term)
        perField.sortTerms(); // 该域所有文档所有的termId根据二进制进行排序
        assert perField.indexOptions != IndexOptions.NONE;
        allFields.add(perField);
      }
    }

    // Sort by field name   基于fieldName进行排序。里面的termId已经排好序了
    CollectionUtil.introSort(allFields);

    Fields fields = new FreqProxFields(allFields);
    applyDeletes(state, fields); //会进行term删除操作
    if (sortMap != null) { // 为null
      fields = new SortingLeafReader.SortingFields(fields, state.fieldInfos, sortMap);
    }

    FieldsConsumer consumer = state.segmentInfo.getCodec().postingsFormat().fieldsConsumer(state); // 返回FieldsWriter
    boolean success = false;
    try {
      consumer.write(fields, norms); // 需要进去看下。创建字典结构，有tim tip、doc文件文件
      success = true;
    } finally {
      if (success) {
        IOUtils.close(consumer);
      } else {
        IOUtils.closeWhileHandlingException(consumer);
      }
    }

  }

  @Override  // segment内共享，segment完成后就清空
  public TermsHashPerField addField(FieldInvertState invertState, FieldInfo fieldInfo) {
    return new FreqProxTermsWriterPerField(invertState, this, fieldInfo, nextTermsHash.addField(invertState, fieldInfo));
  }
}
