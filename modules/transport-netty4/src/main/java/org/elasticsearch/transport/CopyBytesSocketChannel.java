/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.transport;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.RecvByteBufAllocator;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import static io.netty.channel.internal.ChannelUtils.MAX_BYTES_PER_GATHERING_WRITE_ATTEMPTED_LOW_THRESHOLD;


/**
 * This class is adapted from {@link NioSocketChannel} class in the Netty project. It overrides the channel
 * read/write behavior to ensure that the bytes are always copied to a thread-local direct bytes buffer. This
 * happens BEFORE the call to the Java {@link SocketChannel} is issued.
 *
 * The purpose of this class is to allow the disabling of netty direct buffer pooling while allowing us to
 * control how bytes end up being copied to direct memory. If we simply disabled netty pooling, we would rely
 * on the JDK's internal thread local buffer pooling. Instead, this class allows us to create a one thread
 * local buffer with a defined size.
 *
 * 1。 netty的直接内存池化在这里去控制（不通过原生netty去做池化）
 * 2。可控制线程自己的直接内存池大小,通过MAX_BYTES_PER_WRITE
 *
 * 其实就是可以控制一次写入大小（通过直接内存），因为netty就是固定1024字节，而这里扩展了下，也像netty实现了本地直接内存缓存，不过可以通过调整参数，
 * 使得buffer大小更大，从而1次写入的数据更大（buffer大小变大了）
 *
 * 主要作用：更大内存空间，且可配置
 */
@SuppressForbidden(reason = "Channel#write")
public class CopyBytesSocketChannel extends Netty4NioSocketChannel {

    private static final int MAX_BYTES_PER_WRITE = StrictMath.toIntExact(ByteSizeValue.parseBytesSizeValue(
        System.getProperty("es.transport.buffer.size", "1m"), "es.transport.buffer.size").getBytes());

    // 默认1M
    // 重写的内存池：1. 空间更大 2. 有配置可修改大小 3. 每个线程独有的（ThreadLocal）
    private static final ThreadLocal<ByteBuffer> ioBuffer = ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(MAX_BYTES_PER_WRITE));
    private final WriteConfig writeConfig = new WriteConfig();

    public CopyBytesSocketChannel() {
        super();
    }

    CopyBytesSocketChannel(Channel parent, SocketChannel socket) {
        super(parent, socket);
    }

    @Override
    protected void doWrite(ChannelOutboundBuffer in) throws Exception {
        int writeSpinCount = config().getWriteSpinCount();
        do {
            if (in.isEmpty()) {
                // All written so clear OP_WRITE
                clearOpWrite();
                // Directly return here so incompleteWrite(...) is not called.
                return;
            }

            // Ensure the pending writes are made of ByteBufs only.
            int maxBytesPerGatheringWrite = writeConfig.getMaxBytesPerGatheringWrite();
            ByteBuffer[] nioBuffers = in.nioBuffers(1024, maxBytesPerGatheringWrite);
            int nioBufferCnt = in.nioBufferCount();

            if (nioBufferCnt == 0) {// We have something else beside ByteBuffers to write so fallback to normal writes.
                writeSpinCount -= doWrite0(in);
            } else {
                // Zero length buffers are not added to nioBuffers by ChannelOutboundBuffer, so there is no need
                // to check if the total size of all the buffers is non-zero.

                // 获取当前线程的直接内存空间buffer（内存池），且是es重新定义的（非netty原生）
                ByteBuffer ioBuffer = getIoBuffer();
                // 拷贝数据到线程自己的直接内存buffer
                copyBytes(nioBuffers, nioBufferCnt, ioBuffer);
                ioBuffer.flip();

                int attemptedBytes = ioBuffer.remaining();
                // 写入channel使用线程自己的直接内存buffer
                final int localWrittenBytes = writeToSocketChannel(javaChannel(), ioBuffer);
                if (localWrittenBytes <= 0) {
                    incompleteWrite(true);
                    return;
                }
                adjustMaxBytesPerGatheringWrite(attemptedBytes, localWrittenBytes, maxBytesPerGatheringWrite);
                setWrittenBytes(nioBuffers, localWrittenBytes);
                in.removeBytes(localWrittenBytes);
                --writeSpinCount;
            }
        } while (writeSpinCount > 0);

        incompleteWrite(writeSpinCount < 0);
    }

    @Override
    protected int doReadBytes(ByteBuf byteBuf) throws Exception {
        final RecvByteBufAllocator.Handle allocHandle = unsafe().recvBufAllocHandle();
        int writeableBytes = Math.min(byteBuf.writableBytes(), MAX_BYTES_PER_WRITE);
        allocHandle.attemptedBytesRead(writeableBytes);
        ByteBuffer ioBuffer = getIoBuffer();
        ioBuffer.limit(writeableBytes);
        int bytesRead = readFromSocketChannel(javaChannel(), ioBuffer);
        ioBuffer.flip();
        if (bytesRead > 0) {
            byteBuf.writeBytes(ioBuffer);
        }
        return bytesRead;
    }

    // Protected so that tests can verify behavior and simulate partial writes
    protected int writeToSocketChannel(SocketChannel socketChannel, ByteBuffer ioBuffer) throws IOException {
        return socketChannel.write(ioBuffer);
    }

    // Protected so that tests can verify behavior
    protected int readFromSocketChannel(SocketChannel socketChannel, ByteBuffer ioBuffer) throws IOException {
        return socketChannel.read(ioBuffer);
    }

    private static ByteBuffer getIoBuffer() {
        // 创建or获取当前线程的直接内存buffer对象（线程复用这个直接内存空间）
        ByteBuffer ioBuffer = CopyBytesSocketChannel.ioBuffer.get();
        // 清空本地线程直接内存的数据
        ioBuffer.clear();
        return ioBuffer;
    }

    private void adjustMaxBytesPerGatheringWrite(int attempted, int written, int oldMaxBytesPerGatheringWrite) {
        // By default we track the SO_SNDBUF when ever it is explicitly set. However some OSes may dynamically change
        // SO_SNDBUF (and other characteristics that determine how much data can be written at once) so we should try
        // make a best effort to adjust as OS behavior changes.
        if (attempted == written) {
            if (attempted << 1 > oldMaxBytesPerGatheringWrite) {
                writeConfig.setMaxBytesPerGatheringWrite(attempted << 1);
            }
        } else if (attempted > MAX_BYTES_PER_GATHERING_WRITE_ATTEMPTED_LOW_THRESHOLD && written < attempted >>> 1) {
            writeConfig.setMaxBytesPerGatheringWrite(attempted >>> 1);
        }
    }

    private static void copyBytes(ByteBuffer[] source, int nioBufferCnt, ByteBuffer destination) {
        for (int i = 0; i < nioBufferCnt && destination.hasRemaining(); i++) {
            ByteBuffer buffer = source[i];
            int nBytesToCopy = Math.min(destination.remaining(), buffer.remaining());
            if (buffer.hasArray()) {
                destination.put(buffer.array(), buffer.arrayOffset() + buffer.position(), nBytesToCopy);
            } else {
                int initialLimit = buffer.limit();
                int initialPosition = buffer.position();
                buffer.limit(buffer.position() + nBytesToCopy);
                destination.put(buffer);
                buffer.position(initialPosition);
                buffer.limit(initialLimit);
            }
        }
    }

    private static void setWrittenBytes(ByteBuffer[] source, int bytesWritten) {
        for (int i = 0; bytesWritten > 0; i++) {
            ByteBuffer buffer = source[i];
            int nBytes = Math.min(buffer.remaining(), bytesWritten);
            buffer.position(buffer.position() + nBytes);
            bytesWritten = bytesWritten - nBytes;
        }
    }

    private final class WriteConfig {

        private volatile int maxBytesPerGatheringWrite = MAX_BYTES_PER_WRITE;

        private WriteConfig() {
            calculateMaxBytesPerGatheringWrite();
        }

        void setMaxBytesPerGatheringWrite(int maxBytesPerGatheringWrite) {
            this.maxBytesPerGatheringWrite = Math.min(maxBytesPerGatheringWrite, MAX_BYTES_PER_WRITE);
        }

        int getMaxBytesPerGatheringWrite() {
            return maxBytesPerGatheringWrite;
        }

        private void calculateMaxBytesPerGatheringWrite() {
            // Multiply by 2 to give some extra space in case the OS can process write data faster than we can provide.
            int newSendBufferSize = config().getSendBufferSize() << 1;
            if (newSendBufferSize > 0) {
                setMaxBytesPerGatheringWrite(config().getSendBufferSize() << 1);
            }
        }
    }
}
