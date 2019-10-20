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
package org.apache.lucene.util.fst;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.RamUsageEstimator;

import java.io.IOException;

/** Provides off heap storage of finite state machine (FST),
 *  using underlying index input instead of byte store on heap
 *
 * @lucene.experimental
 */  // 在启动的时候，作为segment基本内容给初始化了
public final class OffHeapFSTStore implements FSTStore {

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(OffHeapFSTStore.class); // 32

    private IndexInput in; // MMAPIndexInput
    private long offset;
    private long numBytes;

    @Override
    public void init(DataInput in, long numBytes) throws IOException {
        if (in instanceof IndexInput) { // 默认跑这里
            this.in = (IndexInput) in; // ByteBufferIndexInput$SingleBufferImpl,里面buffer使用的DirestByteBufferR
            this.numBytes = numBytes;
            this.offset = this.in.getFilePointer();
        } else {
            throw new IllegalArgumentException("parameter:in should be an instance of IndexInput for using OffHeapFSTStore, not a "
                                               + in.getClass().getName());
        }
    }
    //这里就是内存优势，
    @Override
    public long ramBytesUsed() {
        return BASE_RAM_BYTES_USED;
    }

    @Override
    public long size() {
        return numBytes;
    }

    @Override
    public FST.BytesReader getReverseBytesReader() { // 从这里读取倒叙的FST结构
        try {
            return new ReverseRandomAccessReader(in.randomAccessSlice(offset, numBytes));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
        throw new UnsupportedOperationException("writeToOutput operation is not supported for OffHeapFSTStore");
    }
}
