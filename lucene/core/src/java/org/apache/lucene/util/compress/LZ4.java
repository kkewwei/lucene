/*
 * LZ4 Library
 * Copyright (c) 2011-2016, Yann Collet
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice, this
 *   list of conditions and the following disclaimer.
 *
 * * Redistributions in binary form must reproduce the above copyright notice, this
 *   list of conditions and the following disclaimer in the documentation and/or
 *   other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
 * ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.apache.lucene.util.compress;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.BitUtil;

/**
 * LZ4 compression and decompression routines.
 *
 * <p>https://github.com/lz4/lz4/tree/dev/lib http://fastcompression.blogspot.fr/p/lz4.html
 *
 * <p>The high-compression option is a simpler version of the one of the original algorithm, and
 * only retains a better hash table that remembers about more occurrences of a previous 4-bytes
 * sequence, and removes all the logic about handling of the case when overlapping matches are
 * found.
 */
public final class LZ4 {

  private LZ4() {}

  /**
   * Window size: this is the maximum supported distance between two strings so that LZ4 can replace
   * the second one by a reference to the first one.
   */
  public static final int MAX_DISTANCE = 1 << 16; // maximum distance of a reference

  static final int MEMORY_USAGE = 14;
  static final int MIN_MATCH = 4; // minimum length of a match 匹配的最小长度，就是一个int 4位长度
  static final int LAST_LITERALS = 5; // the last 5 bytes must be encoded as literals最后5个byte必须编码为literals
  static final int HASH_LOG_HC = 15; // log size of the dictionary for compressHC
  static final int HASH_TABLE_SIZE_HC = 1 << HASH_LOG_HC;
  //在LZ4中，用到了魔数2654435761L，这个数是2到2^32间黄金分割的素数， 2654435761 / 4294967296 = 0.618033987， 2654435761L用 signed int表示就是-1640531535.
  private static int hash(int i, int hashBits) {
    return (i * -1640531535) >>> (32 - hashBits);
  }

  private static int hashHC(int i) {
    return hash(i, HASH_LOG_HC);
  }

  private static int readInt(byte[] buf, int i) {
    // According to LZ4's algorithm the endianness does not matter at all:
    return (int) BitUtil.VH_NATIVE_INT.get(buf, i);
  }

  private static int commonBytes(byte[] b, int o1, int o2, int limit) {
    assert o1 < o2;
    // never -1 because lengths always differ
    return Arrays.mismatch(b, o1, limit, b, o2, limit);
  }

  /**
   * Decompress at least {@code decompressedLen} bytes into {@code dest[dOff:]}. Please note that
   * {@code dest} must be large enough to be able to hold <b>all</b> decompressed data (meaning that
   * you need to know the total decompressed length). If the given bytes were compressed using a
   * preset dictionary then the same dictionary must be provided in {@code dest[dOff-dictLen:dOff]}.
   */
  public static int decompress(DataInput compressed, int decompressedLen, byte[] dest, int dOff)// 返回的是压缩前的长度
      throws IOException {// 词典解压
    final int destEnd = dOff + decompressedLen;

    do {
      // literals
      final int token = compressed.readByte() & 0xFF;
      int literalLen = token >>> 4;

      if (literalLen != 0) {//从上次开始存储，到这次存储之间的长度
        if (literalLen == 0x0F) {
          byte len;
          while ((len = compressed.readByte()) == (byte) 0xFF) {
            literalLen += 0xFF;
          }
          literalLen += len & 0xFF;
        }
        compressed.readBytes(dest, dOff, literalLen);// 读取上次到这次未重复的字符串
        dOff += literalLen;
      }

      if (dOff >= destEnd) {
        break;
      }

      // matchs
      final int matchDec = compressed.readShort() & 0xFFFF;// 两个匹配offset点增量长度
      if (matchDec == 0) {
        throw new IOException("offset 0 is invalid");
      }

      int matchLen = token & 0x0F;
      if (matchLen == 0x0F) {
        int len;
        while ((len = compressed.readByte()) == (byte) 0xFF) {
          matchLen += 0xFF;
        }
        matchLen += len & 0xFF;
      }
      matchLen += MIN_MATCH;// 匹配的长度

      // copying a multiple of 8 bytes can make decompression from 5% to 10% faster
      final int fastLen = (matchLen + 7) & 0xFFFFFFF8;
      if (matchDec < matchLen || dOff + fastLen > destEnd) {
        // overlap -> naive incremental copy
        for (int ref = dOff - matchDec, end = dOff + matchLen; dOff < end; ++ref, ++dOff) {
          dest[dOff] = dest[ref];
        }
      } else {
        // no overlap -> arraycopy
        System.arraycopy(dest, dOff - matchDec, dest, dOff, fastLen);//从上次重复的地方，开始继续读取fastLen个重复的字符
        dOff += matchLen;
      }
    } while (dOff < destEnd);

    return dOff;
  }

  private static void encodeLen(int l, DataOutput out) throws IOException {
    while (l >= 0xFF) {
      out.writeByte((byte) 0xFF);
      l -= 0xFF;
    }
    out.writeByte((byte) l);
  }

  private static void encodeLiterals(
      byte[] bytes, int token, int anchor, int literalLen, DataOutput out) throws IOException {
    out.writeByte((byte) token);// 存放了长度（小于16的部分）

    // encode literal length
    if (literalLen >= 0x0F) {   // 若长度大于16，存放剩余长度
      encodeLen(literalLen - 0x0F, out);
    }

    // encode literals
    out.writeBytes(bytes, anchor, literalLen); // 存储还未重复的string
  }

  private static void encodeLastLiterals(byte[] bytes, int anchor, int literalLen, DataOutput out)
      throws IOException {
    final int token = Math.min(literalLen, 0x0F) << 4;
    encodeLiterals(bytes, token, anchor, literalLen, out);
  }

  private static void encodeSequence(//  anchor: 上次存储的起始位置；matchRef: hashId
      byte[] bytes, int anchor, int matchRef, int matchOff, int matchLen, DataOutput out)
      throws IOException {
    final int literalLen = matchOff - anchor;// 从未开始编码-现在长度
    assert matchLen >= 4;
    // encode token  编码literalLen
    final int token = (Math.min(literalLen, 0x0F) << 4) | Math.min(matchLen - 4, 0x0F);
    encodeLiterals(bytes, token, anchor, literalLen, out); // 存储还未重复的string

    // encode match dec
    final int matchDec = matchOff - matchRef;// 两个匹配offset增量长度
    assert matchDec > 0 && matchDec < 1 << 16;
    out.writeShort((short) matchDec);// 存放差值长度

    // encode match len
    if (matchLen >= MIN_MATCH + 0x0F) {// 若match长度大于19
      encodeLen(matchLen - 0x0F - MIN_MATCH, out);// 长度编码-19
    }
  }// 存储还未重复的string长度+ 存储还未重复的string长度 + 存储还未重复的string + 匹配的字符串长度

  /** A record of previous occurrences of sequences of 4 bytes. */
  abstract static class HashTable {

    /** Reset this hash table in order to compress the given content. */
    abstract void reset(byte[] b, int off, int len);

    /** Init {@code dictLen} bytes to be used as a dictionary. */
    abstract void initDictionary(int dictLen);

    /**
     * Advance the cursor to {@code off} and return an index that stored the same 4 bytes as {@code
     * b[o:o+4)}. This may only be called on strictly increasing sequences of offsets. A return
     * value of {@code -1} indicates that no other index could be found.
     */
    abstract int get(int off);

    /**
     * Return an index that less than {@code off} and stores the same 4 bytes. Unlike {@link #get},
     * it doesn't need to be called on increasing offsets. A return value of {@code -1} indicates
     * that no other index could be found.
     */
    abstract int previous(int off);

    // For testing
    abstract boolean assertReset();
  }

  private abstract static class Table {

    abstract void set(int offset, int value);

    abstract int getAndSet(int offset, int value);

    abstract int getBitsPerValue();

    abstract int size();
  }

  /**
   * 16 bits per offset. This is by far the most commonly used table since it gets used whenever
   * compressing inputs whose size is <= 64kB.
   */
  private static class Table16 extends Table {

    private final short[] table;

    Table16(int size) {
      this.table = new short[size];
    }

    @Override
    void set(int index, int value) {
      assert value >= 0 && value < 1 << 16;
      table[index] = (short) value;
    }

    @Override
    int getAndSet(int index, int value) {
      int prev = Short.toUnsignedInt(table[index]);
      set(index, value);
      return prev;// 之前没有存储
    }

    @Override
    int getBitsPerValue() {
      return Short.SIZE;
    }

    @Override
    int size() {
      return table.length;
    }
  }

  /** 32 bits per value, only used when inputs exceed 64kB, e.g. very large stored fields. */
  private static class Table32 extends Table {

    private final int[] table;

    Table32(int size) {
      this.table = new int[size];
    }

    @Override
    void set(int index, int value) {
      table[index] = value;
    }

    @Override
    int getAndSet(int index, int value) {
      int prev = table[index];
      set(index, value);
      return prev;
    }

    @Override
    int getBitsPerValue() {
      return Integer.SIZE;
    }

    @Override
    int size() {
      return table.length;
    }
  }

  /**
   * Simple lossy {@link HashTable} that only stores the last ocurrence for each hash on {@code
   * 2^14} bytes of memory.
   */
  public static final class FastCompressionHashTable extends HashTable { // 作用：

    private byte[] bytes;// 数据存放进去了
    private int base; // 字典的起始位置
    private int lastOff;
    private int end;
    private int hashLog;
    private Table hashTable;

    /** Sole constructor */
    public FastCompressionHashTable() {}

    @Override
    void reset(byte[] bytes, int off, int len) {
      Objects.checkFromIndexSize(off, len, bytes.length);
      this.bytes = bytes;// 数据存放进去了
      this.base = off;// 字典的起始位置
      this.end = off + len;
      final int bitsPerOffset;
      if (len - LAST_LITERALS < 1 << Short.SIZE) {// 若长度小于16位
        bitsPerOffset = Short.SIZE;
      } else {
        bitsPerOffset = Integer.SIZE;
      }
      final int bitsPerOffsetLog = 32 - Integer.numberOfLeadingZeros(bitsPerOffset - 1);// bitsPerOffset的长度
      hashLog = MEMORY_USAGE + 3 - bitsPerOffsetLog;
      if (hashTable == null
          || hashTable.size() < 1 << hashLog// 词典规格太小
          || hashTable.getBitsPerValue() < bitsPerOffset) {
        if (bitsPerOffset > Short.SIZE) {// 若太长了，则直接使用int表示
          assert bitsPerOffset == Integer.SIZE;
          hashTable = new Table32(1 << hashLog);
        } else {
          assert bitsPerOffset == Short.SIZE;
          hashTable = new Table16(1 << hashLog);//否则继续bits
        }
      } else {
        // Avoid calling hashTable.clear(), this makes it costly to compress many short sequences
        // otherwise.
        // Instead, get() checks that references are less than the current offset.
      }
      this.lastOff = off - 1;
    }

    @Override
    void initDictionary(int dictLen) {
      for (int i = 0; i < dictLen; ++i) {
        final int v = readInt(bytes, base + i);
        final int h = hash(v, hashLog);
        hashTable.set(h, i);
      }
      lastOff += dictLen;
    }

    @Override
    int get(int off) {
      assert off > lastOff;
      assert off < end;

      final int v = readInt(bytes, off);//从当前位置读取4个byte
      final int h = hash(v, hashLog);// 计算4个byte的hash值

      final int ref = base + hashTable.getAndSet(h, off - base);// 都是这趟的绝对位置。仅仅是记录重复数据起始位置，辅助使用的
      lastOff = off;

      if (ref < off && off - ref < MAX_DISTANCE && readInt(bytes, ref) == v) {// 若读取到重复值了
        return ref;
      } else {
        return -1;
      }
    }

    @Override
    public int previous(int off) {
      return -1;
    }

    @Override
    boolean assertReset() {
      return true;
    }
  }

  /**
   * A higher-precision {@link HashTable}. It stores up to 256 occurrences of 4-bytes sequences in
   * the last {@code 2^16} bytes, which makes it much more likely to find matches than {@link
   * FastCompressionHashTable}.
   */
  public static final class HighCompressionHashTable extends HashTable {
    private static final int MAX_ATTEMPTS = 256;
    private static final int MASK = MAX_DISTANCE - 1;

    private byte[] bytes;
    private int base;
    private int next;
    private int end;
    private final int[] hashTable;
    private final short[] chainTable;
    private int attempts = 0;

    /** Sole constructor */
    public HighCompressionHashTable() {
      hashTable = new int[HASH_TABLE_SIZE_HC];
      Arrays.fill(hashTable, -1);
      chainTable = new short[MAX_DISTANCE];
      Arrays.fill(chainTable, (short) 0xFFFF);
    }

    @Override
    void reset(byte[] bytes, int off, int len) {
      Objects.checkFromIndexSize(off, len, bytes.length);
      if (end - base < chainTable.length) {
        // The last call to compress was done on less than 64kB, let's not reset
        // the hashTable and only reset the relevant parts of the chainTable.
        // This helps avoid slowing down calling compress() many times on short
        // inputs.
        int startOffset = base & MASK;
        int endOffset = end == 0 ? 0 : ((end - 1) & MASK) + 1;
        if (startOffset < endOffset) {
          Arrays.fill(chainTable, startOffset, endOffset, (short) 0xFFFF);
        } else {
          Arrays.fill(chainTable, 0, endOffset, (short) 0xFFFF);
          Arrays.fill(chainTable, startOffset, chainTable.length, (short) 0xFFFF);
        }
      } else {
        // The last call to compress was done on a large enough amount of data
        // that it's fine to reset both tables
        Arrays.fill(hashTable, -1);
        Arrays.fill(chainTable, (short) 0xFFFF);
      }
      this.bytes = bytes;
      this.base = off;
      this.next = off;
      this.end = off + len;
    }

    @Override
    void initDictionary(int dictLen) {
      assert next == base;
      for (int i = 0; i < dictLen; ++i) {
        addHash(base + i);
      }
      next += dictLen;
    }

    @Override
    int get(int off) {
      assert off >= next;
      assert off < end;

      for (; next < off; next++) {
        addHash(next);
      }

      final int v = readInt(bytes, off);
      final int h = hashHC(v);

      attempts = 0;
      int ref = hashTable[h];
      if (ref >= off) {
        // remainder from a previous call to compress()
        return -1;
      }
      for (int min = Math.max(base, off - MAX_DISTANCE + 1);
          ref >= min && attempts < MAX_ATTEMPTS;
          ref -= chainTable[ref & MASK] & 0xFFFF, attempts++) {
        if (readInt(bytes, ref) == v) {
          return ref;
        }
      }
      return -1;
    }

    private void addHash(int off) {
      final int v = readInt(bytes, off);
      final int h = hashHC(v);
      int delta = off - hashTable[h];
      if (delta <= 0 || delta >= MAX_DISTANCE) {
        delta = MAX_DISTANCE - 1;
      }
      chainTable[off & MASK] = (short) delta;
      hashTable[h] = off;
    }

    @Override
    int previous(int off) {
      final int v = readInt(bytes, off);
      for (int ref = off - (chainTable[off & MASK] & 0xFFFF);
          ref >= base && attempts < MAX_ATTEMPTS;
          ref -= chainTable[ref & MASK] & 0xFFFF, attempts++) {
        if (readInt(bytes, ref) == v) {
          return ref;
        }
      }
      return -1;
    }

    @Override
    boolean assertReset() {
      for (int i = 0; i < chainTable.length; ++i) {
        assert chainTable[i] == (short) 0xFFFF : i;
      }
      return true;
    }
  }
 // HashTable ht确定的最快压缩还是最高效压缩，默认是FastCompressionHashTable
  /**
   * Compress {@code bytes[off:off+len]} into {@code out} using at most 16kB of memory. {@code ht}
   * shouldn't be shared across threads but can safely be reused.
   */
  public static void compress(byte[] bytes, int off, int len, DataOutput out, HashTable ht)
      throws IOException {
    compressWithDictionary(bytes, off, 0, len, out, ht);
  }

  /**
   * Compress {@code bytes[dictOff+dictLen:dictOff+dictLen+len]} into {@code out} using at most 16kB
   * of memory. {@code bytes[dictOff:dictOff+dictLen]} will be used as a dictionary. {@code dictLen}
   * must not be greater than {@link LZ4#MAX_DISTANCE 64kB}, the maximum window size.
   *
   * <p>{@code ht} shouldn't be shared across threads but can safely be reused.
   */
  public static void compressWithDictionary(//dictOff：字典起始位置，dictLen：存在的字典长度。（写入字典时，dictLen=0），len：需要压缩的长度（除去字典）
      byte[] bytes, int dictOff, int dictLen, int len, DataOutput out, HashTable ht)
      throws IOException {
    Objects.checkFromIndexSize(dictOff, dictLen, bytes.length);
    Objects.checkFromIndexSize(dictOff + dictLen, len, bytes.length);
    if (dictLen > MAX_DISTANCE) {
      throw new IllegalArgumentException(
          "dictLen must not be greater than 64kB, but got " + dictLen);
    }

    final int end = dictOff + dictLen + len;// 此次读取数据的最终未知

    int off = dictOff + dictLen;// 词典的结束位置（词典+字符的绝对位置）
    int anchor = off;//还未压缩字符的起始未知

    if (len > LAST_LITERALS + MIN_MATCH) {// 一定进来

      final int limit = end - LAST_LITERALS;
      final int matchLimit = limit - MIN_MATCH;
      ht.reset(bytes, dictOff, dictLen + len);// 词典开始的后面len位置
      ht.initDictionary(dictLen);

      main:
      while (off <= limit) {
        // find a match
        int ref;
        while (true) {
          if (off >= matchLimit) {// 已经编码完了
            break main;
          }
          ref = ht.get(off);// 会有替换ht里面hash存储的行为，返回值是引用的起始位置
          if (ref != -1) {// 读取到重复值了
            assert ref >= dictOff && ref < off;
            assert readInt(bytes, ref) == readInt(bytes, off);// 说明每次前进的窗口是int
            break;// 读取到了一个重复值
          }
          ++off;
        }
        // 匹配到的相同的长度
        // compute match length
        int matchLen = MIN_MATCH + commonBytes(bytes, ref + MIN_MATCH, off + MIN_MATCH, limit);
        // 来发现更好相同的前缀
        // try to find a better match
        for (int r = ht.previous(ref), min = Math.max(off - MAX_DISTANCE + 1, dictOff);
            r >= min;
            r = ht.previous(r)) {
          assert readInt(bytes, r) == readInt(bytes, off);
          int rMatchLen = MIN_MATCH + commonBytes(bytes, r + MIN_MATCH, off + MIN_MATCH, limit);
          if (rMatchLen > matchLen) {
            ref = r;
            matchLen = rMatchLen;
          }
        }

        encodeSequence(bytes, anchor, ref, off, matchLen, out);//开始压缩未重复的+重复的长度
        off += matchLen;// 前进一个相同长度的量
        anchor = off;
      }
    }

    // last literals
    final int literalLen = end - anchor;
    assert literalLen >= LAST_LITERALS || literalLen == len;
    encodeLastLiterals(bytes, anchor, end - anchor, out);
  }
}
