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
 * DocValues types. Note that DocValues is strongly typed, so a
 * field cannot have different types across different documents.
 */
public enum DocValuesType {
  /**
   * No doc values for this field.
   */
  NONE, // 不开启docvalue时的状态，默认
  /** 
   * A per-document Number
   */  // 数值或日期或枚举字段+单值
  NUMERIC,  ///  单个数值类型的docvalue主要包括（int，long，float，double）
  /**
   * A per-document byte[].  Values may be larger than
   * 32766 bytes, but different codecs may enforce their own limits.
   */
  BINARY,   // 二进制类型值对应不同的codes最大值可能超过32766字节，
  /** 
   * A pre-sorted byte[]. Fields with this type only store distinct byte values 
   * and store an additional offset pointer per document to dereference the shared 
   * byte[]. The stored byte[] is presorted and allows access via document id, 
   * ordinal and by-value.  Values must be {@code <= 32766} bytes.
   */
  SORTED,  // 有序增量字节存储，仅仅存储不同部分的值和偏移量指针，值必须小于等于32766字节   字符串+单值 会选择
  /**
   * A pre-sorted Number[]. Fields with this type store numeric values in sorted
   * order according to {@link Long#compare(long, long)}.
   */
  SORTED_NUMERIC,  // 存储数值类型的有序数组列表   数值或日期或枚举字段+多值
  /**
   * A pre-sorted Set&lt;byte[]&gt;. Fields with this type only store distinct byte values 
   * and store additional offset pointers per document to dereference the shared 
   * byte[]s. The stored byte[] is presorted and allows access via document id, 
   * ordinal and by-value.  Values must be {@code <= 32766} bytes.
   */ // es针对不分词字段，默认是这个设置
  SORTED_SET,   /// 可以存储多值域的docvalue值，但返回时，仅仅只能返回多值域的第一个docvalue
} // 多字段使用。会预先对值字节进行排序、去重存储。字符串+多值 会选择

