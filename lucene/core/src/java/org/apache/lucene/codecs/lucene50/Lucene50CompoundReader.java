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
package org.apache.lucene.codecs.lucene50;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.CompoundDirectory;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.IOUtils;

/**
 * Class for accessing a compound stream.
 * This class implements a directory, but is limited to only read operations.
 * Directory methods that would normally modify data throw an exception.
 * @lucene.experimental
 */// 单个segment真正的读取者
final class Lucene50CompoundReader extends CompoundDirectory {
  
  /** Offset/Length for a slice inside of a compound file */
  public static final class FileEntry {
    long offset;
    long length;
  }
  // 所有复合文件作用详见：http://lucene.apache.org/core/8_2_0/core/org/apache/lucene/codecs/lucene80/package-summary.html
  private final Directory directory;
  private final String segmentName;
  private final Map<String,FileEntry> entries; // 读取了复合文件里面每个单独映射文件的文件名->映射的位置
  private final IndexInput handle;  // ByteBufferIndexInput.$SingleBufferImpl，底层还是靠的map来映射。仍然保留着映射能力，cfe文件内容后面再读
  private int version;
  
  /**
   * Create a new CompoundFileDirectory.
   */
  // TODO: we should just pre-strip "entries" and append segment name up-front like simpletext?
  // this need not be a "general purpose" directory anymore (it only writes index files)
  public Lucene50CompoundReader(Directory directory, SegmentInfo si, IOContext context) throws IOException { // 分别将cfe和cfs header给检查了
    this.directory = directory;
    this.segmentName = si.name; //_v
    String dataFileName = IndexFileNames.segmentFileName(segmentName, "", Lucene50CompoundFormat.DATA_EXTENSION); // 复合文件内容汇总的文件，cfs文件
    String entriesFileName = IndexFileNames.segmentFileName(segmentName, "", Lucene50CompoundFormat.ENTRIES_EXTENSION); // 复合文件名汇总（及每个复合文件在汇总文件中的偏移量） cfe
    this.entries = readEntries(si.getId(), directory, entriesFileName);  // 获取所有的复合文件名称cfe(NIO读取)，及每个文件的长度都给读取出来
    boolean success = false;

    long expectedLength = CodecUtil.indexHeaderLength(Lucene50CompoundFormat.DATA_CODEC, "");
    for(Map.Entry<String,FileEntry> ent : entries.entrySet()) {
      expectedLength += ent.getValue().length;
    }
    expectedLength += CodecUtil.footerLength(); 

    handle = directory.openInput(dataFileName, context); // 打开具体数据文件(cfs),检查文件名是否符合规范，读取shard里面的索引结构使用的MMAP方式打开的，见FsDirectoryFactory.Line152
    try {  // magic+segmentId+fileName 三部分进行头部验证
      CodecUtil.checkIndexHeader(handle, Lucene50CompoundFormat.DATA_CODEC, version, version, si.getId(), "");
      
      // NOTE: data file is too costly to verify checksum against all the bytes on open,
      // but for now we at least verify proper structure of the checksum footer: which looks
      // for FOOTER_MAGIC + algorithmID. This is cheap and can detect some forms of corruption
      // such as file truncation.
      CodecUtil.retrieveChecksum(handle);  // 校验整个文件成本太大，所以只校验文件尾部，可以检验出一些文件截取而损坏的

      // We also validate length, because e.g. if you strip 16 bytes off the .cfs we otherwise
      // would not detect it:
      if (handle.length() != expectedLength) {
        throw new CorruptIndexException("length should be " + expectedLength + " bytes, but is " + handle.length() + " instead", handle);
      }

      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(handle);
      }
    }
  }
  // 仅仅是从cfe中获取当前segment中每个文件在cfs中的位置信息。
  /** Helper method that reads CFS entries from an input stream */
  private Map<String, FileEntry> readEntries(byte[] segmentID, Directory dir, String entriesFileName) throws IOException {
    Map<String,FileEntry> mapping = null;// 在读集群元数据时候，dir=SimpleFSDirecotry,不会使用mmap映射
    try (ChecksumIndexInput entriesStream = dir.openChecksumInput(entriesFileName, IOContext.READONCE)) { // 首先读取cfe文件，通过NIO方式映射
      Throwable priorE = null;
      try {
        version = CodecUtil.checkIndexHeader(entriesStream, Lucene50CompoundFormat.ENTRY_CODEC,
                                                              Lucene50CompoundFormat.VERSION_START, 
                                                              Lucene50CompoundFormat.VERSION_CURRENT, segmentID, "");
        final int numEntries = entriesStream.readVInt(); // 复合文件个数
        mapping = new HashMap<>(numEntries);
        for (int i = 0; i < numEntries; i++) { // 遍历cfe，获取每个文件在cfs中的位置信息
          final FileEntry fileEntry = new FileEntry();
          final String id = entriesStream.readString();  // _Lucene50_0.tip  .nvm  .fnm   .fdt   _Lucene50_0.pos  .nvd   _Lucene50_0.tim   .fdx   _Lucene50_0.doc
          FileEntry previous = mapping.put(id, fileEntry);
          if (previous != null) {
            throw new CorruptIndexException("Duplicate cfs entry id=" + id + " in CFS ", entriesStream);
          }
          fileEntry.offset = entriesStream.readLong(); // 获取该文件在复合文件内容中的偏移量及长度，以便从复合文件内容中获取文件
          fileEntry.length = entriesStream.readLong();
        }
      } catch (Throwable exception) {
        priorE = exception;
      } finally {
        CodecUtil.checkFooter(entriesStream, priorE);
      }
    }
    return Collections.unmodifiableMap(mapping);
  }
  
  @Override
  public void close() throws IOException {
    IOUtils.close(handle);
  }
  
  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    ensureOpen();
    final String id = IndexFileNames.stripSegmentName(name); // 获得文件后缀  _5i.fnm
    final FileEntry entry = entries.get(id);
    if (entry == null) {
      String datFileName = IndexFileNames.segmentFileName(segmentName, "", Lucene50CompoundFormat.DATA_EXTENSION);
      throw new FileNotFoundException("No sub-file with id " + id + " found in compound file \"" + datFileName + "\" (fileName=" + name + " files: " + entries.keySet() + ")");
    }
    return handle.slice(name, entry.offset, entry.length); // 获取那段文件内容
  }
  
  /** Returns an array of strings, one for each file in the directory. */
  @Override
  public String[] listAll() {
    ensureOpen();
    String[] res = entries.keySet().toArray(new String[entries.size()]);
    
    // Add the segment name
    for (int i = 0; i < res.length; i++) {
      res[i] = segmentName + res[i];
    }
    return res;
  }
  
  /** Returns the length of a file in the directory.
   * @throws IOException if the file does not exist */
  @Override
  public long fileLength(String name) throws IOException {
    ensureOpen();
    FileEntry e = entries.get(IndexFileNames.stripSegmentName(name));
    if (e == null)
      throw new FileNotFoundException(name);
    return e.length;
  }

  @Override
  public String toString() {
    return "CompoundFileDirectory(segment=\"" + segmentName + "\" in dir=" + directory + ")";
  }

  @Override
  public Set<String> getPendingDeletions() {
    return Collections.emptySet();
  }

  @Override
  public void checkIntegrity() throws IOException {
    CodecUtil.checksumEntireFile(handle);
  }
}
