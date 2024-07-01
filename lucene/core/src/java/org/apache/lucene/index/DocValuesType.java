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

/**
 * DocValues types. Note that DocValues is strongly typed, so a field cannot have different types
 * across different documents.
 */
public enum DocValuesType {
  /** No doc values for this field. */
  NONE, // 不开启docvalue时的状态，默认
  /** A per-document Number */   // 数值或日期或枚举字段+单值
  NUMERIC, /// 每个文档只能有个一个value，比如seq_no就说这个
  /**
   * A per-document byte[]. Values may be larger than 32766 bytes, but different codecs may enforce
   * their own limits.
   */
  BINARY, // 二进制类型值对应不同的codes最大值可能超过32766字节，一个只能单值
  /**
   * A pre-sorted byte[]. Fields with this type only store distinct byte values and store an
   * additional offset pointer per document to dereference the shared byte[]. The stored byte[] is
   * presorted and allows access via document id, ordinal and by-value. Values must be {@code <=
   * 32766} bytes.
   */
  SORTED, //值必须小于等于32766字节，一个只能单值，
  /**
   * A pre-sorted Number[]. Fields with this type store numeric values in sorted order according to
   * {@link Long#compare(long, long)}.
   */
  SORTED_NUMERIC, // int，long，float，double，date，geo_point字段都选择的这个,一个field可以有多个value
  /**
   * A pre-sorted Set&lt;byte[]&gt;. Fields with this type only store distinct byte values and store
   * additional offset pointers per document to dereference the shared byte[]s. The stored byte[] is
   * presorted and allows access via document id, ordinal and by-value. Values must be {@code <=
   * 32766} bytes.
   */
  SORTED_SET; // keyword选择的这个，单值长度有限制，不能超过32766
}
