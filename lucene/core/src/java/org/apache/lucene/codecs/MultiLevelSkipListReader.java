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


import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.MathUtil;

/**
 * This abstract class reads skip lists with multiple levels.
 * 
 * See {@link MultiLevelSkipListWriter} for the information about the encoding 
 * of the multi level skip lists. 
 * 
 * Subclasses must implement the abstract method {@link #readSkipData(int, IndexInput)}
 * which defines the actual format of the skip data.
 * @lucene.experimental
 */

public abstract class MultiLevelSkipListReader implements Closeable {
  /** the maximum number of skip levels possible for this index */
  protected int maxNumberOfSkipLevels; 
  
  /** number of levels in this skip list */
  protected int numberOfSkipLevels;// 这个跳表的level
  
  // Expert: defines the number of top skip levels to buffer in memory.
  // Reducing this number results in less memory usage, but possibly
  // slower performance due to more random I/Os.
  // Please notice that the space each level occupies is limited by
  // the skipInterval. The top level can not contain more than
  // skipLevel entries, the second top level can not contain more
  // than skipLevel^2 entries and so forth.
  private int numberOfLevelsToBuffer = 1;
  // 这个词在多少个文档中出现过的
  private int docCount;

  /** skipStream for each level. */
  private IndexInput[] skipStream;  // 存放的是每级别跳表全部value(),skipStream[0]是在 MultiLevelSkipListReader 构造器中初始化成功的

  /** The start pointer of each skip level. */
  private long skipPointer[];//每级别跳表value在doc文件中的绝对起始位置（初始化时是跳表在doc文件中的起始位置）

  /**  skipInterval of each level. */
  private int skipInterval[];// 每一级别跳表元素记录的相邻跳表的doc 间距

  /** Number of docs skipped per level.
   * It's possible for some values to overflow a signed int, but this has been accounted for.
   */// ，每level之间间隔多少文档数
  private int[] numSkipped;

  /** Doc id of current skip entry per level. */
  protected int[] skipDoc; // 每级别的doc文档数

  /** Doc id of last read skip entry with docId &lt;= target. */
  private int lastDoc;

  /** Child pointer of current skip entry per level. */
  private long[] childPointer; // 每一级别childPointer在doc文件中的绝对位置

  /** childPointer of last read skip entry with docId &lt;=
   *  target. */
  private long lastChildPointer;
  
  private boolean inputIsBuffered;
  private final int skipMultiplier;
  // 会从 Lucene84SkipReader 对象初始化跑进来
  /** Creates a {@code MultiLevelSkipListReader}. */
  protected MultiLevelSkipListReader(IndexInput skipStream, int maxSkipLevels, int skipInterval, int skipMultiplier) {
    this.skipStream = new IndexInput[maxSkipLevels];
    this.skipPointer = new long[maxSkipLevels];
    this.childPointer = new long[maxSkipLevels];
    this.numSkipped = new int[maxSkipLevels];
    this.maxNumberOfSkipLevels = maxSkipLevels;
    this.skipInterval = new int[maxSkipLevels];// 每level跳表最开始对应的文档号：128，128*8，128*8*8，128*8*8*8，128*8*8*8*8，......
    this.skipMultiplier = skipMultiplier;// 每次跳表的间距，8
    this.skipStream [0]= skipStream;// doc文件赋值，可参考 Lucene84PostingsReader$BlockImpactsDocsEnum
    this.inputIsBuffered = (skipStream instanceof BufferedIndexInput);//使用的mmap方式加载的，默认为false
    this.skipInterval[0] = skipInterval;// 128
    for (int i = 1; i < maxSkipLevels; i++) {
      // cache skip intervals
      this.skipInterval[i] = this.skipInterval[i - 1] * skipMultiplier;
    }
    skipDoc = new int[maxSkipLevels];
  }

  /** Creates a {@code MultiLevelSkipListReader}, where
   *  {@code skipInterval} and {@code skipMultiplier} are
   *  the same. */
  protected MultiLevelSkipListReader(IndexInput skipStream, int maxSkipLevels, int skipInterval) {
    this(skipStream, maxSkipLevels, skipInterval, skipInterval);
  }
  
  /** Returns the id of the doc to which the last call of {@link #skipTo(int)}
   *  has skipped.  */
  public int getDoc() {
    return lastDoc;
  }
  
   // 跳过文档号大于target的第一条文档，返回该文档的docId
  /** Skips entries to the first beyond the current whose document number is
   *  greater than or equal to <i>target</i>. Returns the current doc count. 
   */
  public int skipTo(int target) throws IOException {

    // walk up the levels until highest level is found that has a skip
    // for this target
    int level = 0;  // 是为了补充skipDoc
    while (level < numberOfSkipLevels - 1 && target > skipDoc[level + 1]) {// 知道满足下一级别的起始文档id高于target
      level++;
    }    

    while (level >= 0) {
      if (target > skipDoc[level]) { // 高了，就是还没找到(也并没有加载出来)
        if (!loadNextSkip(level)) {
          continue;// 很神奇，这里没有-1，加载完loadNextSkip后，就知道循环跑到else中了
        }
      } else { // 不用再继续找levele了
        // no more skips on this level, go down one level
        if (level > 0 && lastChildPointer > skipStream[level - 1].getFilePointer()) {
          seekChild(level - 1);
        } 
        level--;
      }
    }
    
    return numSkipped[0] - skipInterval[0] - 1;
  }
  // 可以参考 MultiLevelSkipListWriter.bufferSkip
  private boolean loadNextSkip(int level) throws IOException {
    // we have to skip, the target document is greater than the current
    // skip list entry        
    setLastSkipData(level); // 上次访问的level的信息，跳到 Lucene84SkipReader.setLastSkipData
    // 每level之间间隔多少文档数
    numSkipped[level] += skipInterval[level];

    // numSkipped may overflow a signed int, so compare as unsigned.
    if (Integer.compareUnsigned(numSkipped[level], docCount) > 0) {// 跳表号码起始位置小于总文档数，一般不会成立
      // this skip list is exhausted
      skipDoc[level] = Integer.MAX_VALUE;
      if (numberOfSkipLevels > level) numberOfSkipLevels = level; 
      return false;
    }

    // read next skip entry 那么就读取该级别起始值，就说明该级别跳表读完了。
    skipDoc[level] += readSkipData(level, skipStream[level]);
    
    if (level != 0) {
      // read the child pointer if we are not on the leaf level
      childPointer[level] = skipStream[level].readVLong() + skipPointer[level - 1]; // 上一级别跳表在doc的的绝对位置
    } // skipStream[level].readVLong()读取的是上一级别跳表相对的位置，而skipPointer[level - 1]是doc中的绝对位置
    
    return true;

  }
  
  /** Seeks the skip entry on the given level */
  protected void seekChild(int level) throws IOException {
    skipStream[level].seek(lastChildPointer);// 跳转到
    numSkipped[level] = numSkipped[level + 1] - skipInterval[level + 1];
    skipDoc[level] = lastDoc;
    if (level > 0) {
      childPointer[level] = skipStream[level].readVLong() + skipPointer[level - 1];
    }
  }

  @Override
  public void close() throws IOException {
    for (int i = 1; i < skipStream.length; i++) {
      if (skipStream[i] != null) {
        skipStream[i].close();
      }
    }
  }

  /** Initializes the reader, for reuse on a new term. */
  public void init(long skipPointer, int df) throws IOException {
    this.skipPointer[0] = skipPointer;//跳表在doc文件中的绝对起始位置
    this.docCount = df;
    assert skipPointer >= 0 && skipPointer <= skipStream[0].length() 
    : "invalid skip pointer: " + skipPointer + ", length=" + skipStream[0].length();
    Arrays.fill(skipDoc, 0);
    Arrays.fill(numSkipped, 0);
    Arrays.fill(childPointer, 0);
    
    for (int i = 1; i < numberOfSkipLevels; i++) {
      skipStream[i] = null;
    }
    loadSkipLevels();
  }
  // 加载每级别的value[]，但是还没有解析出来
  /** Loads the skip levels  */
  private void loadSkipLevels() throws IOException {
    if (docCount <= skipInterval[0]) {
      numberOfSkipLevels = 1;
    } else {
      numberOfSkipLevels = 1+MathUtil.log(docCount/skipInterval[0], skipMultiplier);// 跳表的level
    }

    if (numberOfSkipLevels > maxNumberOfSkipLevels) {
      numberOfSkipLevels = maxNumberOfSkipLevels;
    }
    // 跑到跳表的起始位置了
    skipStream[0].seek(skipPointer[0]);
    
    int toBuffer = numberOfLevelsToBuffer;
    
    for (int i = numberOfSkipLevels - 1; i > 0; i--) {// 倒叙读取每级跳表，可参考 MultiLevelSkipListWriter.writeSkip()
      // the length of the current level
      long length = skipStream[0].readVLong();// 从这个文件首先读取长度
      
      // the start pointer of the current level
      skipPointer[i] = skipStream[0].getFilePointer();
      if (toBuffer > 0) {
        // buffer this level
        skipStream[i] = new SkipBuffer(skipStream[0], (int) length);// 为了读取对应level跳表的值出来
        toBuffer--;
      } else {
        // clone this stream, it is already at the start of the current level
        skipStream[i] = skipStream[0].clone();
        if (inputIsBuffered && length < BufferedIndexInput.BUFFER_SIZE) {
          ((BufferedIndexInput) skipStream[i]).setBufferSize(Math.max(BufferedIndexInput.MIN_BUFFER_SIZE, (int) length));
        }
        
        // move base stream beyond the current level
        skipStream[0].seek(skipStream[0].getFilePointer() + length);
      }
    }
   
    // use base stream for the lowest level
    skipPointer[0] = skipStream[0].getFilePointer();// 把
  }
  
  /**
   * Subclasses must implement the actual skip data encoding in this method.
   *  
   * @param level the level skip data shall be read from
   * @param skipStream the skip stream to read from
   */  
  protected abstract int readSkipData(int level, IndexInput skipStream) throws IOException;
  
  /** Copies the values of the last read skip entry on this level */
  protected void setLastSkipData(int level) {
    lastDoc = skipDoc[level];
    lastChildPointer = childPointer[level];
  }

  
  /** used to buffer the top skip levels */
  private final static class SkipBuffer extends IndexInput {
    private byte[] data;
    private long pointer;
    private int pos;
    
    SkipBuffer(IndexInput input, int length) throws IOException {
      super("SkipBuffer on " + input);
      data = new byte[length];
      pointer = input.getFilePointer();
      input.readBytes(data, 0, length);// 每级别跳表的内容，
    }
    
    @Override
    public void close() {
      data = null;
    }

    @Override
    public long getFilePointer() {
      return pointer + pos;
    }

    @Override
    public long length() {
      return data.length;
    }

    @Override
    public byte readByte() {
      return data[pos++];
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) {
      System.arraycopy(data, pos, b, offset, len);
      pos += len;
    }

    @Override
    public void seek(long pos) {
      this.pos =  (int) (pos - pointer);
    }
    
    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
      throw new UnsupportedOperationException();
    }
  }
}
