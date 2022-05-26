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
package org.apache.lucene.search;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import org.apache.lucene.document.IntPoint;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.FutureArrays;

/** 
 * Abstract class for range queries against single or multidimensional points such as
 * {@link IntPoint}.
 * <p>
 * This is for subclasses and works on the underlying binary encoding: to
 * create range queries for lucene's standard {@code Point} types, refer to factory
 * methods on those classes, e.g. {@link IntPoint#newRangeQuery IntPoint.newRangeQuery()} for 
 * fields indexed with {@link IntPoint}.
 * <p>
 * For a single-dimensional field this query is a simple range query; in a multi-dimensional field it's a box shape.
 * @see PointValues
 * @lucene.experimental
 */
public abstract class PointRangeQuery extends Query {
  final String field; // 确定了读取那个域的索引
  final int numDims; // 单个元素由几阶构成
  final int bytesPerDim;
  final byte[] lowerPoint; // 当前查找范围 的上下界
  final byte[] upperPoint;

  /** 
   * Expert: create a multidimensional range query for point values.
   *
   * @param field field name. must not be {@code null}.
   * @param lowerPoint lower portion of the range (inclusive).
   * @param upperPoint upper portion of the range (inclusive).
   * @param numDims number of dimensions.
   * @throws IllegalArgumentException if {@code field} is null, or if {@code lowerValue.length != upperValue.length}
   */
  protected PointRangeQuery(String field, byte[] lowerPoint, byte[] upperPoint, int numDims) {
    checkArgs(field, lowerPoint, upperPoint);
    this.field = field;
    if (numDims <= 0) {
      throw new IllegalArgumentException("numDims must be positive, got " + numDims);
    }
    if (lowerPoint.length == 0) {
      throw new IllegalArgumentException("lowerPoint has length of zero");
    }
    if (lowerPoint.length % numDims != 0) {
      throw new IllegalArgumentException("lowerPoint is not a fixed multiple of numDims");
    }
    if (lowerPoint.length != upperPoint.length) {
      throw new IllegalArgumentException("lowerPoint has length=" + lowerPoint.length + " but upperPoint has different length=" + upperPoint.length);
    }
    this.numDims = numDims;
    this.bytesPerDim = lowerPoint.length / numDims;

    this.lowerPoint = lowerPoint;
    this.upperPoint = upperPoint;
  }

  /** 
   * Check preconditions for all factory methods
   * @throws IllegalArgumentException if {@code field}, {@code lowerPoint} or {@code upperPoint} are null.
   */
  public static void checkArgs(String field, Object lowerPoint, Object upperPoint) {
    if (field == null) {
      throw new IllegalArgumentException("field must not be null");
    }
    if (lowerPoint == null) {
      throw new IllegalArgumentException("lowerPoint must not be null");
    }
    if (upperPoint == null) {
      throw new IllegalArgumentException("upperPoint must not be null");
    }
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(field)) {
      visitor.visitLeaf(this);
    }
  }
  //精确匹配的Weight都是常量得分匹配。（匹配与否比较明确）
  @Override
  public final Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {

    // We don't use RandomAccessWeight here: it's no good to approximate with "match all docs".
    // This is an inverted structure and should be used in the first pass:

    return new ConstantScoreWeight(this, boost) {

      private boolean matches(byte[] packedValue) {
        for(int dim=0;dim<numDims;dim++) { //是否匹配上下界
          int offset = dim*bytesPerDim;
          if (FutureArrays.compareUnsigned(packedValue, offset, offset + bytesPerDim, lowerPoint, offset, offset + bytesPerDim) < 0) {
            // Doc's value is too low, in this dimension
            return false;
          }
          if (FutureArrays.compareUnsigned(packedValue, offset, offset + bytesPerDim, upperPoint, offset, offset + bytesPerDim) > 0) {
            // Doc's value is too high, in this dimension
            return false;
          }
        }
        return true;
      }
      // 查询与数据范围的关系,  minPackedValue: 该segment中最大/最小的那个值
      private Relation relate(byte[] minPackedValue, byte[] maxPackedValue) {
        // 是否相交
        boolean crosses = false;

        for(int dim=0;dim<numDims;dim++) {
          int offset = dim*bytesPerDim;
          // 对比每一阶，只要有一阶大于，就outside
          if (FutureArrays.compareUnsigned(minPackedValue, offset, offset + bytesPerDim, upperPoint, offset, offset + bytesPerDim) > 0 ||
              FutureArrays.compareUnsigned(maxPackedValue, offset, offset + bytesPerDim, lowerPoint, offset, offset + bytesPerDim) < 0) {
            return Relation.CELL_OUTSIDE_QUERY; // 查询不在数据范围之内
          }
          
          crosses |= FutureArrays.compareUnsigned(minPackedValue, offset, offset + bytesPerDim, lowerPoint, offset, offset + bytesPerDim) < 0 ||
              FutureArrays.compareUnsigned(maxPackedValue, offset, offset + bytesPerDim, upperPoint, offset, offset + bytesPerDim) > 0;
        }

        if (crosses) {
          return Relation.CELL_CROSSES_QUERY;
        } else {
          return Relation.CELL_INSIDE_QUERY;
        }
      }
       // 为了遍历匹配使用的，下面还有个getInverseIntersectVisitor。结果中间保持者
      private IntersectVisitor getIntersectVisitor(DocIdSetBuilder result) {
        return new IntersectVisitor() {

          DocIdSetBuilder.BulkAdder adder;// 匹配的docID放这里

          @Override
          public void grow(int count) {
            adder = result.grow(count); // 先留好保存空间的结构，等待后面存储docId
          }

          @Override
          public void visit(int docID) {
            adder.add(docID); // 存放的是匹配的docId
          }

          @Override
          public void visit(int docID, byte[] packedValue) {
            if (matches(packedValue)) { // 是否匹配上下界限,
              visit(docID); // 就是上面这个函数,将docId存放起来
            }
          }

          @Override
          public void visit(DocIdSetIterator iterator, byte[] packedValue) throws IOException {
            if (matches(packedValue)) {
              int docID;
              while ((docID = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                visit(docID);
              }
            }
          }

          @Override
          public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
            return relate(minPackedValue, maxPackedValue);
          }
        };
      }

      /**
       * Create a visitor that clears documents that do NOT match the range.
       */ //相反visit，不match的会被置0
      private IntersectVisitor getInverseIntersectVisitor(FixedBitSet result, int[] cost) {
        return new IntersectVisitor() {

          @Override
          public void visit(int docID) {
            result.clear(docID); // 对应位置标记0
            cost[0]--;
          }

          @Override
          public void visit(int docID, byte[] packedValue) {
            if (matches(packedValue) == false) { // 不mathc的，会被置0
              visit(docID);
            }
          }

          @Override
          public void visit(DocIdSetIterator iterator, byte[] packedValue) throws IOException {
            if (matches(packedValue) == false) {
              int docID;
              while ((docID = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                visit(docID);
              }
            }
          }
          //反着来
          @Override
          public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
            Relation relation = relate(minPackedValue, maxPackedValue);
            switch (relation) {//也是反着来的
              case CELL_INSIDE_QUERY:
                // all points match, skip this subtree
                return Relation.CELL_OUTSIDE_QUERY;
              case CELL_OUTSIDE_QUERY: // 是找哪些没匹配的，对这些docId进行置0操作
                // none of the points match, clear all documents
                return Relation.CELL_INSIDE_QUERY;
              default:
                return relation;
            }
          }
        };
      }

      @Override
      public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        LeafReader reader = context.reader(); // 开始时：ExitableDirectoryReader$ExitableLeafReader，后来是SegmentReader

        PointValues values = reader.getPointValues(field); //开始是：ExitableDirectoryReader$ExitablePointValues，后来是BKDReader
        if (values == null) {
          // No docs in this segment/field indexed any points
          return null;
        }

        if (values.getNumIndexDimensions() != numDims) {
          throw new IllegalArgumentException("field=\"" + field + "\" was indexed with numIndexDimensions=" + values.getNumIndexDimensions() + " but this query has numDims=" + numDims);
        }
        if (bytesPerDim != values.getBytesPerDimension()) {
          throw new IllegalArgumentException("field=\"" + field + "\" was indexed with bytesPerDim=" + values.getBytesPerDimension() + " but this query has bytesPerDim=" + bytesPerDim);
        }

        boolean allDocsMatch;
        if (values.getDocCount() == reader.maxDoc()) { // 每个文档都包含的有这个字段
          final byte[] fieldPackedLower = values.getMinPackedValue(); // 这个字段的最小值
          final byte[] fieldPackedUpper = values.getMaxPackedValue(); // 获取最大值和最小值
          allDocsMatch = true;
          for (int i = 0; i < numDims; ++i) {
            int offset = i * bytesPerDim; // 查询范围
            if (FutureArrays.compareUnsigned(lowerPoint, offset, offset + bytesPerDim, fieldPackedLower, offset, offset + bytesPerDim) > 0
                || FutureArrays.compareUnsigned(upperPoint, offset, offset + bytesPerDim, fieldPackedUpper, offset, offset + bytesPerDim) < 0) {
              allDocsMatch = false; // 需要查询的范围并不能把存储的范围完全包含中。
              break;
            }
          }
        } else {//是否所有文档都包含该字段，并且查询范围包含在内
          allDocsMatch = false;
        }

        final Weight weight = this; // PointRangeQuery$IntersectVisitor
        if (allDocsMatch) {
          // all docs have a value and all points are within bounds, so everything matches
          return new ScorerSupplier() {
            @Override
            public Scorer get(long leadCost) {
              return new ConstantScoreScorer(weight, score(), scoreMode, DocIdSetIterator.all(reader.maxDoc()));
            }
            
            @Override
            public long cost() {
              return reader.maxDoc();
            }
          };
        } else {
          return new ScorerSupplier() {
            // result中记录了在BKD树中查找文档id的记录
            final DocIdSetBuilder result = new DocIdSetBuilder(reader.maxDoc(), values, field);// 仅仅申明文档收集器
            final IntersectVisitor visitor = getIntersectVisitor(result);
            long cost = -1;
            // 真正对比每条数据，判断是否是在范围内的
            @Override
            public Scorer get(long leadCost) throws IOException { // 参数没用
              if (values.getDocCount() == reader.maxDoc() // 判断segment包含的point和最大文档编号是否相等
                  && values.getDocCount() == values.size() // 每个文档都有一个值
                  && cost() > reader.maxDoc() / 2) { // 这里有个预估，若大部分匹配上了，那么就找未匹配的
                // If all docs have exactly one value and the cost is greater
                // than half the leaf size then maybe we can make things faster
                // by computing the set of documents that do NOT match the range
                final FixedBitSet result = new FixedBitSet(reader.maxDoc());
                result.set(0, reader.maxDoc()); // 全部置位1了
                int[] cost = new int[] { reader.maxDoc() };
                values.intersect(getInverseIntersectVisitor(result, cost)); // 相反查找
                final DocIdSetIterator iterator = new BitSetIterator(result, cost[0]);
                return new ConstantScoreScorer(weight, score(), scoreMode, iterator);
              }

              values.intersect(visitor); // 会进行具体的比较value,缓存docId
              DocIdSetIterator iterator = result.build().iterator(); // 哪些文档符合预期，结果保存在了iterator  BitSetIterator
              return new ConstantScoreScorer(weight, score(), scoreMode, iterator);
            }
            // 在bkd数中若叶子节点有一个满足，那么就认为预估节点为512/2个数据符合要求。
            @Override
            public long cost() {
              if (cost == -1) {
                // Computing the cost may be expensive, so only do it if necessary
                cost = values.estimateDocCount(visitor);//会去遍历bkd索引，预计匹配的分片树：在bkd数中根据上下限对比，只要有相交，那么就认为预估节点为512/2个数据符合要求。
                assert cost >= 0;
              }
              return cost;
            }
          };
        }
      }
      // 对一个segment操作
      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        ScorerSupplier scorerSupplier = scorerSupplier(context); // 这里的Scorer直接跑到了scorerSupplier
        if (scorerSupplier == null) {
          return null;
        }
        return scorerSupplier.get(Long.MAX_VALUE); // PointRangeQuery， 会有BKD树中查找匹配的docId
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return true;
      }

    };
  }

  public String getField() {
    return field;
  }

  public int getNumDims() {
    return numDims;
  }

  public int getBytesPerDim() {
    return bytesPerDim;
  }
  // 当前查询条件下限
  public byte[] getLowerPoint() {
    return lowerPoint.clone();
  }

  public byte[] getUpperPoint() {
    return upperPoint.clone();
  }

  @Override
  public final int hashCode() {
    int hash = classHash();
    hash = 31 * hash + field.hashCode();
    hash = 31 * hash + Arrays.hashCode(lowerPoint);
    hash = 31 * hash + Arrays.hashCode(upperPoint);
    hash = 31 * hash + numDims;
    hash = 31 * hash + Objects.hashCode(bytesPerDim);
    return hash;
  }

  @Override
  public final boolean equals(Object o) {
    return sameClassAs(o) &&
           equalsTo(getClass().cast(o));
  }

  private boolean equalsTo(PointRangeQuery other) {
    return Objects.equals(field, other.field) &&
           numDims == other.numDims &&
           bytesPerDim == other.bytesPerDim &&
           Arrays.equals(lowerPoint, other.lowerPoint) &&
           Arrays.equals(upperPoint, other.upperPoint);
  }

  @Override
  public final String toString(String field) {
    final StringBuilder sb = new StringBuilder();
    if (this.field.equals(field) == false) {
      sb.append(this.field);
      sb.append(':');
    }

    // print ourselves as "range per dimension"
    for (int i = 0; i < numDims; i++) {
      if (i > 0) {
        sb.append(',');
      }
      
      int startOffset = bytesPerDim * i;

      sb.append('[');
      sb.append(toString(i, ArrayUtil.copyOfSubArray(lowerPoint, startOffset, startOffset + bytesPerDim)));
      sb.append(" TO ");
      sb.append(toString(i, ArrayUtil.copyOfSubArray(upperPoint, startOffset, startOffset + bytesPerDim)));
      sb.append(']');
    }

    return sb.toString();
  }

  /**
   * Returns a string of a single value in a human-readable format for debugging.
   * This is used by {@link #toString()}.
   *
   * @param dimension dimension of the particular value
   * @param value single value, never null
   * @return human readable value for debugging
   */
  protected abstract String toString(int dimension, byte[] value);
}
