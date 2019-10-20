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
package org.apache.lucene.codecs;


import java.io.IOException;

import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.util.MathUtil;

/**
 * This abstract class writes skip lists with multiple levels.
 * 
 * <pre>
 *
 * Example for skipInterval = 3:
 *                                                     c            (skip level 2)
 *                 c                 c                 c            (skip level 1) 
 *     x     x     x     x     x     x     x     x     x     x      (skip level 0)
 * d d d d d d d d d d d d d d d d d d d d d d d d d d d d d d d d  (posting list)
 *     3     6     9     12    15    18    21    24    27    30     (df)
 * 
 * d - document
 * x - skip data
 * c - skip data with child pointer
 * 
 * Skip level i contains every skipInterval-th entry from skip level i-1.
 * Therefore the number of entries on level i is: floor(df / ((skipInterval ^ (i + 1))).
 * 
 * Each skip entry on a level {@code i>0} contains a pointer to the corresponding skip entry in list i-1.
 * This guarantees a logarithmic amount of skips to find the target document.
 * 
 * While this class takes care of writing the different skip levels,
 * subclasses must define the actual format of the skip data.
 * </pre>
 * @lucene.experimental
 */

public abstract class MultiLevelSkipListWriter {
  /** number of levels in this skip list */
  protected final int numberOfSkipLevels; // 最大10层
   // 就是一个block的大小
  /** the skip interval in the list with level = 0 */
  private final int skipInterval; // skipInterval是level 0的跳跃间距，skipMultiplier是level > 0的跳跃间距。初始跳跃为128

  /** skipInterval used for level &gt; 0 */
  private final int skipMultiplier; // 是level > 0的跳跃间距。 skipInterval*skipMultiplier^n，可以算出当前节点跳跃level=n+1，为8
  
  /** for every skip level a different buffer is used  */
  private RAMOutputStream[] skipBuffer; // 是个RAMOutputStream的数组，用来存跳表，相当于一个二维字节数组。

  /** Creates a {@code MultiLevelSkipListWriter}. */ //  df 该segment的文档总数
  protected MultiLevelSkipListWriter(int skipInterval, int skipMultiplier, int maxSkipLevels, int df) {
    this.skipInterval = skipInterval; // 128
    this.skipMultiplier = skipMultiplier;
    
    int numberOfSkipLevels;
    // calculate the maximum number of skip levels for this document frequency
    if (df <= skipInterval) { // 本segment的文档数
      numberOfSkipLevels = 1;
    } else {
      numberOfSkipLevels = 1+MathUtil.log(df/skipInterval, skipMultiplier); // 第一次128 ，第二层8  第三层8
    }
    
    // make sure it does not exceed maxSkipLevels
    if (numberOfSkipLevels > maxSkipLevels) {
      numberOfSkipLevels = maxSkipLevels;
    }
    this.numberOfSkipLevels = numberOfSkipLevels;
  }
  
  /** Creates a {@code MultiLevelSkipListWriter}, where
   *  {@code skipInterval} and {@code skipMultiplier} are
   *  the same. */
  protected MultiLevelSkipListWriter(int skipInterval, int maxSkipLevels, int df) {
    this(skipInterval, skipInterval, maxSkipLevels, df);
  }

  /** Allocates internal skip buffers. */
  protected void init() {
    skipBuffer = new RAMOutputStream[numberOfSkipLevels];
    for (int i = 0; i < numberOfSkipLevels; i++) {
      skipBuffer[i] = new RAMOutputStream();
    }
  }

  /** Creates new buffers or empties the existing ones */
  protected void resetSkip() { // 初始化skipBuffer
    if (skipBuffer == null) { // 跑到这里
      init();
    } else {
      for (int i = 0; i < skipBuffer.length; i++) {
        skipBuffer[i].reset();
      }
    }      
  }

  /**
   * Subclasses must implement the actual skip data encoding in this method.
   *  
   * @param level the level skip data shall be writing for
   * @param skipBuffer the skip buffer to write to
   */
  protected abstract void writeSkipData(int level, IndexOutput skipBuffer) throws IOException;

  /**
   * Writes the current skip data to the buffers. The current document frequency determines
   * the max level is skip data is to be written to. 
   * 
   * @param df the current document frequency 
   * @throws IOException If an I/O error occurs
   */
  public void bufferSkip(int df) throws IOException { // df：第多少个文档建立跳表点

    assert df % skipInterval == 0;
    int numLevels = 1; //df = skipInterval*skipMultiplier^n   -> numLevels= n+1
    df /= skipInterval;  // 这里表示 skip point一定会写入到 level 0
   
    // determine max level
    while ((df % skipMultiplier) == 0 && numLevels < numberOfSkipLevels) {
      numLevels++;
      df /= skipMultiplier;
    }
    
    long childPointer = 0;
    // level值从小到大，在每层 level的字节数组末尾写入skip point
    for (int level = 0; level < numLevels; level++) { // 一个节点是几个level上的跳点
      writeSkipData(level, skipBuffer[level]);// 将 skip point写入 skipBuffer[level]中
      
      long newChildPointer = skipBuffer[level].getFilePointer(); //
      
      if (level != 0) { // 缓存第几级别
        // store child pointers for all levels except the lowest
        skipBuffer[level].writeVLong(childPointer); // 后一个level记录前一个level使用的缓存大小
      }
      
      //remember the childPointer for the next level
      childPointer = newChildPointer;
    }
  }

  /**
   * Writes the buffered skip lists to the given output.
   * 
   * @param output the IndexOutput the skip lists shall be written to 
   * @return the pointer the skip list starts
   */
  public long writeSkip(IndexOutput output) throws IOException {
    long skipPointer = output.getFilePointer();
    //System.out.println("skipper.writeSkip fp=" + skipPointer);
    if (skipBuffer == null || skipBuffer.length == 0) return skipPointer;
    
    for (int level = numberOfSkipLevels - 1; level > 0; level--) { // 跳跃表从高阶到低阶排序的
      long length = skipBuffer[level].getFilePointer(); // 文档长度
      if (length > 0) {
        output.writeVLong(length);
        skipBuffer[level].writeTo(output);
      }
    }
    skipBuffer[0].writeTo(output); // 第0个跳跃表单独拿出来
    
    return skipPointer;
  }
}
