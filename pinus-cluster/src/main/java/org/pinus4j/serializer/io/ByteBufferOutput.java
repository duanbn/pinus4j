/**
 * Copyright 2014 Duan Bingnan
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 *   
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pinus4j.serializer.io;

/**
 * 简单的ByteBuffer, 每次使用之前需要清理一下buffer.
 *
 * @author duanbn
 */
public class ByteBufferOutput extends AbstractBufferOutput {

    public static final int CAPACITY = 1024 * 2;
    private int             capacity;
    private float           loadFactor;

    private byte[]          _buf;

    public ByteBufferOutput() {
        this(CAPACITY, 0.4f);
    }

    public ByteBufferOutput(int capacity, float loadFactor) {
        this.capacity = capacity;
        _buf = new byte[this.capacity];
        limit = this.capacity;
        this.loadFactor = loadFactor;
    }

    public byte[] byteArray() {
        byte[] rbuf = new byte[offset];
        System.arraycopy(_buf, 0, rbuf, 0, offset);
        return rbuf;
    }

    public void reset() {
        offset = 0;
        limit = this.capacity;
        _buf = new byte[this.capacity];
    }

    @Override
    protected void _write(byte b) {
        if (offset == _buf.length) {
            _allocMore();
        }
        _buf[offset++] = b;
    }

    @Override
    protected void _allocMore() {
        limit = (int) (offset * (1 + loadFactor));
        byte[] newBuf = new byte[limit];
        System.arraycopy(_buf, 0, newBuf, 0, _buf.length);
        _buf = newBuf;
    }
}
