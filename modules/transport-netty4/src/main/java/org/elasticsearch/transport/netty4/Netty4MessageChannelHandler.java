/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport.netty4;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.InboundPipeline;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.Transports;

import java.nio.channels.ClosedChannelException;
import java.util.ArrayDeque;
import java.util.Queue;

/**
 * A handler (must be the last one!) that does size based frame decoding and forwards the actual message
 * to the relevant action.
 */
final class Netty4MessageChannelHandler extends ChannelDuplexHandler {

    private final Netty4Transport transport;

    private final Queue<WriteOperation> queuedWrites = new ArrayDeque<>();

    private WriteOperation currentWrite;
    private final InboundPipeline pipeline;

    Netty4MessageChannelHandler(PageCacheRecycler recycler, Netty4Transport transport) {
        this.transport = transport;
        final ThreadPool threadPool = transport.getThreadPool();
        final Transport.RequestHandlers requestHandlers = transport.getRequestHandlers();

        // 重新定义个pipeline（es实现，类似netty的）
        // messageHandler -> transport::inboundMessage转发到handler
        this.pipeline = new InboundPipeline(transport.getVersion(), transport.getStatsTracker(), recycler, threadPool::relativeTimeInMillis,
            transport.getInflightBreaker(), requestHandlers::getHandler, transport::inboundMessage);
    }

    // 读取
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        assert Transports.assertDefaultThreadContext(transport.getThreadPool().getThreadContext());
        assert Transports.assertTransportThread();
        assert msg instanceof ByteBuf : "Expected message type ByteBuf, found: " + msg.getClass();

        final ByteBuf buffer = (ByteBuf) msg;
        // 拿回这个连接的es channel
        Netty4TcpChannel channel = ctx.channel().attr(Netty4Transport.CHANNEL_KEY).get();
        // 对msg报装
        final BytesReference wrapped = Netty4Utils.toBytesReference(buffer);

        // 提交给pipeline
        try (ReleasableBytesReference reference = new ReleasableBytesReference(wrapped, buffer::release)) {
            pipeline.handleBytes(channel, reference);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        assert Transports.assertDefaultThreadContext(transport.getThreadPool().getThreadContext());
        ExceptionsHelper.maybeDieOnAnotherThread(cause);
        final Throwable unwrapped = ExceptionsHelper.unwrap(cause, ElasticsearchException.class);
        final Throwable newCause = unwrapped != null ? unwrapped : cause;
        Netty4TcpChannel tcpChannel = ctx.channel().attr(Netty4Transport.CHANNEL_KEY).get();
        if (newCause instanceof Error) {
            transport.onException(tcpChannel, new Exception(newCause));
        } else {
            transport.onException(tcpChannel, (Exception) newCause);
        }
    }

    // 发送，写缓冲
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        assert msg instanceof ByteBuf;
        assert Transports.assertDefaultThreadContext(transport.getThreadPool().getThreadContext());
        // 新增WriteOperation（里面是字节ByteBuf），放入queuedWrites 队列
        final boolean queued = queuedWrites.offer(new WriteOperation((ByteBuf) msg, promise));
        assert queued;
        assert Transports.assertDefaultThreadContext(transport.getThreadPool().getThreadContext());
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) {
        assert Transports.assertDefaultThreadContext(transport.getThreadPool().getThreadContext());
        if (ctx.channel().isWritable()) {
            doFlush(ctx);
        }
        ctx.fireChannelWritabilityChanged();
    }

    // 实际强制发生
    @Override
    public void flush(ChannelHandlerContext ctx) {
        assert Transports.assertDefaultThreadContext(transport.getThreadPool().getThreadContext());
        Channel channel = ctx.channel();
        // 把在当前对象的queue已缓冲的操作都发送
        if (channel.isWritable() || channel.isActive() == false) {
            doFlush(ctx);
        }
    }

    // 连接关闭
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        assert Transports.assertDefaultThreadContext(transport.getThreadPool().getThreadContext());
        doFlush(ctx);// 把已缓冲的操作都发送
        Releasables.closeExpectNoException(pipeline);// 关闭流
        super.channelInactive(ctx);
    }

    private void doFlush(ChannelHandlerContext ctx) {
        assert ctx.executor().inEventLoop();
        final Channel channel = ctx.channel();
        if (channel.isActive() == false) {
            if (currentWrite != null) {
                currentWrite.promise.tryFailure(new ClosedChannelException());
            }
            failQueuedWrites();
            return;
        }
        while (channel.isWritable()) {
            // 上1个请求操作已完成，从队列取出
            if (currentWrite == null) {
                currentWrite = queuedWrites.poll();
            }
            // 已有则是继续这个操作 -》拆包处理

            if (currentWrite == null) {
                break;
            }
            final WriteOperation write = currentWrite;
            if (write.buf.readableBytes() == 0) {
                write.promise.trySuccess();
                currentWrite = null;
                continue;
            }
            final int readableBytes = write.buf.readableBytes();
            final int bufferSize = Math.min(readableBytes, 1 << 18);
            final int readerIndex = write.buf.readerIndex();
            final boolean sliced = readableBytes != bufferSize;

            // 生成writeBuffer
            final ByteBuf writeBuffer;
            if (sliced) {
                writeBuffer = write.buf.retainedSlice(readerIndex, bufferSize);
                write.buf.readerIndex(readerIndex + bufferSize);
            } else {
                writeBuffer = write.buf;
            }
            // 触发netty 的write -》发送本次writeBuffer
            final ChannelFuture writeFuture = ctx.write(writeBuffer);
            if (sliced == false || write.buf.readableBytes() == 0) {// 已经全部写完
                currentWrite = null;// 清空

                // 加1个完成回调：
                writeFuture.addListener(future -> {
                    assert ctx.executor().inEventLoop();
                    // 成功
                    if (future.isSuccess()) {
                        write.promise.trySuccess();
                    } else {
                        write.promise.tryFailure(future.cause());
                    }
                });
            } else {
                // 拆包时，需要判断部分是否成功
                // 加1个完成后，判断失败则是调用失败函数
                writeFuture.addListener(future -> {
                    assert ctx.executor().inEventLoop();
                    // 失败，调用失败函数
                    if (future.isSuccess() == false) {
                        write.promise.tryFailure(future.cause());
                    }
                });
            }

            // 触发写入：netty flush
            ctx.flush();
            // 连接关闭
            if (channel.isActive() == false) {
                // 直接调用失败
                failQueuedWrites();
                return;
            }
        }
    }

    private void failQueuedWrites() {
        WriteOperation queuedWrite;
        while ((queuedWrite = queuedWrites.poll()) != null) {
            queuedWrite.promise.tryFailure(new ClosedChannelException());
        }
    }

    private static final class WriteOperation {

        private final ByteBuf buf;

        private final ChannelPromise promise;

        WriteOperation(ByteBuf buf, ChannelPromise promise) {
            this.buf = buf;
            this.promise = promise;
        }
    }
}
