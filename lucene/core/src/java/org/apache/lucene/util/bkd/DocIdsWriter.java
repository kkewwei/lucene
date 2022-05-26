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
package org.apache.lucene.util.bkd;

import java.io.IOException;

import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;

class DocIdsWriter {

  private DocIdsWriter() {}
 // Id有序无序都可以下载
  static void writeDocIds(int[] docIds, int start, int count, DataOutput out) throws IOException {
    // docs can be sorted either when all docs in a block have the same value
    // or when a segment is sorted
    boolean sorted = true;
    for (int i = 1; i < count; ++i) { // 自带检查，若后面的数小于前面的数,是递增数列
      if (docIds[start + i - 1] > docIds[start + i]) {
        sorted = false;
        break;
      }
    }
    if (sorted) { // 若排好序，增增量写入
      out.writeByte((byte) 0);// 有序
      int previous = 0;
      for (int i = 0; i < count; ++i) {
        int doc = docIds[start + i];
        out.writeVInt(doc - previous);
        previous = doc;
      }
    } else {
      long max = 0;
      for (int i = 0; i < count; ++i) { //找最大值
        max |= Integer.toUnsignedLong(docIds[start + i]);
      }
      if (max <= 0xffffff) {
        out.writeByte((byte) 24); // 每个文档使用24位，无序
        for (int i = 0; i < count; ++i) {
          out.writeShort((short) (docIds[start + i] >>> 8));
          out.writeByte((byte) docIds[start + i]);
        }
      } else {
        out.writeByte((byte) 32); // 每个文档使用32位，无序
        for (int i = 0; i < count; ++i) {
          out.writeInt(docIds[start + i]);
        }
      }
    }
  }
   // 参考BKDWriter.build()中叶子存储过程：writeLeafBlockDocs函数,先存储的docCount，
  /** Read {@code count} integers into {@code docIDs}. */
  static void readInts(IndexInput in, int count, int[] docIDs) throws IOException {
    final int bpv = in.readByte();
    switch (bpv) {// 有序
      case 0:
        readDeltaVInts(in, count, docIDs);
        break;
      case 32: //无序，32表示
        readInts32(in, count, docIDs);
        break;
      case 24: // 无序，24位表示
        readInts24(in, count, docIDs);
        break;
      default:
        throw new IOException("Unsupported number of bits per value: " + bpv);
    }
  }

  private static void readDeltaVInts(IndexInput in, int count, int[] docIDs) throws IOException {
    int doc = 0;
    for (int i = 0; i < count; i++) {
      doc += in.readVInt();
      docIDs[i] = doc;
    }
  }

  static <T> void readInts32(IndexInput in, int count, int[] docIDs) throws IOException {
    for (int i = 0; i < count; i++) {
      docIDs[i] = in.readInt();
    }
  }

  private static void readInts24(IndexInput in, int count, int[] docIDs) throws IOException {
    int i;
    for (i = 0; i < count - 7; i += 8) {
      long l1 = in.readLong();
      long l2 = in.readLong();
      long l3 = in.readLong();
      docIDs[i] =  (int) (l1 >>> 40);
      docIDs[i+1] = (int) (l1 >>> 16) & 0xffffff;
      docIDs[i+2] = (int) (((l1 & 0xffff) << 8) | (l2 >>> 56));
      docIDs[i+3] = (int) (l2 >>> 32) & 0xffffff;
      docIDs[i+4] = (int) (l2 >>> 8) & 0xffffff;
      docIDs[i+5] = (int) (((l2 & 0xff) << 16) | (l3 >>> 48));
      docIDs[i+6] = (int) (l3 >>> 24) & 0xffffff;
      docIDs[i+7] = (int) l3 & 0xffffff;
    }
    for (; i < count; ++i) {
      docIDs[i] = (Short.toUnsignedInt(in.readShort()) << 8) | Byte.toUnsignedInt(in.readByte());
    }
  }

  /** Read {@code count} integers and feed the result directly to {@link IntersectVisitor#visit(int)}. */
  static void readInts(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
    final int bpv = in.readByte();
    switch (bpv) {
      case 0:
        readDeltaVInts(in, count, visitor);
        break;
      case 32:
        readInts32(in, count, visitor);
        break;
      case 24:
        readInts24(in, count, visitor);
        break;
      default:
        throw new IOException("Unsupported number of bits per value: " + bpv);
    }
  }

  private static void readDeltaVInts(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
    int doc = 0;
    for (int i = 0; i < count; i++) {
      doc += in.readVInt();
      visitor.visit(doc);
    }
  }

  private static void readInts32(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
    for (int i = 0; i < count; i++) {
      visitor.visit(in.readInt());
    }
  }

  private static void readInts24(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
    int i;
    for (i = 0; i < count - 7; i += 8) {
      long l1 = in.readLong();
      long l2 = in.readLong();
      long l3 = in.readLong();
      visitor.visit((int) (l1 >>> 40));
      visitor.visit((int) (l1 >>> 16) & 0xffffff);
      visitor.visit((int) (((l1 & 0xffff) << 8) | (l2 >>> 56)));
      visitor.visit((int) (l2 >>> 32) & 0xffffff);
      visitor.visit((int) (l2 >>> 8) & 0xffffff);
      visitor.visit((int) (((l2 & 0xff) << 16) | (l3 >>> 48)));
      visitor.visit((int) (l3 >>> 24) & 0xffffff);
      visitor.visit((int) l3 & 0xffffff);
    }
    for (; i < count; ++i) {
      visitor.visit((Short.toUnsignedInt(in.readShort()) << 8) | Byte.toUnsignedInt(in.readByte()));
    }
  }
}
