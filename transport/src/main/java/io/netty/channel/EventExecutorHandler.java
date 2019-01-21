/*
 * Copyright 2019 The Netty Project
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
package io.netty.channel;

import io.netty.buffer.ByteBufAllocator;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.internal.ObjectUtil;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.PromiseNotificationUtil;
import io.netty.util.internal.ThrowableUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.net.SocketAddress;
import java.util.concurrent.Callable;

public final class EventExecutorHandler implements ChannelInboundHandler, ChannelOutboundHandler {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(EventExecutor.class);

    private final EventExecutor executor;
    private final ChannelHandler handler;
    private final boolean inbound;
    private final boolean outbound;

    private MessageSizeEstimator.Handle handle;
    private ChannelHandlerContext context;

    public EventExecutorHandler(EventExecutor executor, ChannelHandler handler) {
        this.executor = ObjectUtil.checkNotNull(executor, "executor");
        this.handler = ObjectUtil.checkNotNull(handler, "handler");
        inbound = handler instanceof ChannelInboundHandler;
        outbound = handler instanceof ChannelOutboundHandler;
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) {
        if (inbound) {
            if (executor.inEventLoop()) {
                invokeChannelRegistered();
            } else {
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        invokeChannelRegistered();
                    }
                });
            }
        } else {
            ctx.fireChannelRegistered();
        }
    }

    private void invokeChannelRegistered() {
        try {
            ((ChannelInboundHandler) handler).channelRegistered(context);
        } catch (Throwable cause) {
            notifyHandlerException(context, cause);
        }
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) {
        if (inbound) {
            if (executor.inEventLoop()) {
                invokeChannelUnregistered();
            } else {
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        invokeChannelUnregistered();
                    }
                });
            }
        } else {
            ctx.fireChannelUnregistered();
        }
    }

    private void invokeChannelUnregistered() {
        try {
            ((ChannelInboundHandler) handler).channelUnregistered(context);
        } catch (Throwable cause) {
            notifyHandlerException(context, cause);
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        if (inbound) {
            if (executor.inEventLoop()) {
                invokeChannelActive();
            } else {
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        invokeChannelActive();
                    }
                });
            }
        } else {
            ctx.fireChannelActive();
        }
    }

    private void invokeChannelActive() {
        try {
            ((ChannelInboundHandler) handler).channelActive(context);
        } catch (Throwable cause) {
            notifyHandlerException(context, cause);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        if (inbound) {
            if (executor.inEventLoop()) {
                invokeChannelInactive();
            } else {
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        invokeChannelInactive();
                    }
                });
            }
        } else {
            ctx.fireChannelInactive();
        }
    }

    private void invokeChannelInactive() {
        try {
            ((ChannelInboundHandler) handler).channelInactive(context);
        } catch (Throwable cause) {
            notifyHandlerException(context, cause);
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, final Object msg) {
        if (inbound) {
            if (executor.inEventLoop()) {
                invokeChannelRead(msg);
            } else {
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        invokeChannelRead(msg);
                    }
                });
            }
        } else {
            ctx.fireChannelRead(msg);
        }
    }

    private void invokeChannelRead(Object msg) {
        try {
            ((ChannelInboundHandler) handler).channelRead(context, msg);
        } catch (Throwable cause) {
            notifyHandlerException(context, cause);
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        if (inbound) {
            if (executor.inEventLoop()) {
                invokeChannelReadComplete();
            } else {
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        invokeChannelReadComplete();
                    }
                });
            }
        } else {
            ctx.fireChannelReadComplete();
        }
    }

    private void invokeChannelReadComplete() {
        try {
            ((ChannelInboundHandler) handler).channelReadComplete(context);
        } catch (Throwable cause) {
            notifyHandlerException(context, cause);
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, final Object evt) {
        if (inbound) {
            if (executor.inEventLoop()) {
                invokeUserEventTriggered(evt);
            } else {
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        invokeUserEventTriggered(evt);
                    }
                });
            }
        } else {
            ctx.fireUserEventTriggered(evt);
        }
    }

    private void invokeUserEventTriggered(Object evt) {
        try {
            ((ChannelInboundHandler) handler).userEventTriggered(context, evt);
        } catch (Throwable cause) {
            notifyHandlerException(context, cause);
        }
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) {
        if (inbound) {
            if (executor.inEventLoop()) {
                invokeChannelWritabilityChanged();
            } else {
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        invokeChannelWritabilityChanged();
                    }
                });
            }
        } else {
            ctx.fireChannelWritabilityChanged();
        }
    }

    private void invokeChannelWritabilityChanged() {
        try {
            ((ChannelInboundHandler) handler).channelWritabilityChanged(context);
        } catch (Throwable cause) {
            notifyHandlerException(context, cause);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, final Throwable cause) {
        if (inbound) {
            if (executor.inEventLoop()) {
                invokeExceptionCaught(cause);
            } else {
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        invokeExceptionCaught(cause);
                    }
                });
            }
        } else {
            ctx.fireExceptionCaught(cause);
        }
    }

    private void invokeExceptionCaught(Throwable cause) {
        try {
            ((ChannelInboundHandler) handler).exceptionCaught(context, cause);
        } catch (Throwable error) {
            notifyHandlerException(context, error);
        }
    }

    @Override
    public void bind(ChannelHandlerContext ctx, final SocketAddress localAddress, final ChannelPromise promise) {
        if (outbound) {
            if (executor.inEventLoop()) {
                invokeBind(localAddress, promise);
            } else {
                AbstractChannelHandlerContext.safeExecute(executor, new Runnable() {
                    @Override
                    public void run() {
                        invokeBind(localAddress, promise);
                    }
                }, promise, null);
            }
        } else {
            ctx.bind(localAddress, promise);
        }
    }

    private void invokeBind(SocketAddress localAddress, ChannelPromise promise) {
        try {
            ((ChannelOutboundHandler) handler).bind(context, localAddress, promise);
        } catch (Throwable t) {
            notifyOutboundHandlerException(t, promise);
        }
    }

    @Override
    public void connect(ChannelHandlerContext ctx, final SocketAddress remoteAddress, final SocketAddress localAddress,
                        final ChannelPromise promise) {
        if (outbound) {
            if (executor.inEventLoop()) {
                invokeConnect(remoteAddress, localAddress, promise);
            } else {
                AbstractChannelHandlerContext.safeExecute(executor, new Runnable() {
                    @Override
                    public void run() {
                        invokeConnect(remoteAddress, localAddress, promise);
                    }
                }, promise, null);
            }
        } else {
            ctx.connect(remoteAddress, localAddress, promise);
        }
    }

    private void invokeConnect(SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) {
        try {
            ((ChannelOutboundHandler) handler).connect(context, remoteAddress, localAddress, promise);
        } catch (Throwable t) {
            notifyOutboundHandlerException(t, promise);
        }
    }

    @Override
    public void disconnect(ChannelHandlerContext ctx, final ChannelPromise promise) {
        if (outbound) {
            if (executor.inEventLoop()) {
                invokeDisconnect(promise);
            } else {
                AbstractChannelHandlerContext.safeExecute(executor, new Runnable() {
                    @Override
                    public void run() {
                        invokeDisconnect(promise);
                    }
                }, promise, null);
            }
        } else {
            ctx.disconnect(promise);
        }
    }

    private void invokeDisconnect(ChannelPromise promise) {
        try {
            ((ChannelOutboundHandler) handler).disconnect(context, promise);
        } catch (Throwable t) {
            notifyOutboundHandlerException(t, promise);
        }
    }

    @Override
    public void close(ChannelHandlerContext ctx, final ChannelPromise promise) {
        if (outbound) {
            if (executor.inEventLoop()) {
                invokeClose(promise);
            } else {
                AbstractChannelHandlerContext.safeExecute(executor, new Runnable() {
                    @Override
                    public void run() {
                        invokeClose(promise);
                    }
                }, promise, null);
            }
        } else {
            ctx.close(promise);
        }
    }

    private void invokeClose(ChannelPromise promise) {
        try {
            ((ChannelOutboundHandler) handler).close(context, promise);
        } catch (Throwable t) {
            notifyOutboundHandlerException(t, promise);
        }
    }

    @Override
    public void register(ChannelHandlerContext ctx, final ChannelPromise promise) {
        if (outbound) {
            if (executor.inEventLoop()) {
                invokeRegister(promise);
            } else {
                AbstractChannelHandlerContext.safeExecute(executor, new Runnable() {
                    @Override
                    public void run() {
                        invokeRegister(promise);
                    }
                }, promise, null);
            }
        } else {
            ctx.register(promise);
        }
    }

    private void invokeRegister(ChannelPromise promise) {
        try {
            ((ChannelOutboundHandler) handler).register(context, promise);
        } catch (Throwable t) {
            notifyOutboundHandlerException(t, promise);
        }
    }

    @Override
    public void deregister(ChannelHandlerContext ctx, final ChannelPromise promise) {
        if (outbound) {
            if (executor.inEventLoop()) {
                invokeDeregister(promise);
            } else {
                AbstractChannelHandlerContext.safeExecute(executor, new Runnable() {
                    @Override
                    public void run() {
                        invokeDeregister(promise);
                    }
                }, promise, null);
            }
        } else {
            ctx.deregister(promise);
        }
    }

    private void invokeDeregister(ChannelPromise promise) {
        try {
            ((ChannelOutboundHandler) handler).deregister(context, promise);
        } catch (Throwable t) {
            notifyOutboundHandlerException(t, promise);
        }
    }

    @Override
    public void read(ChannelHandlerContext ctx) {
        if (outbound) {
            if (executor.inEventLoop()) {
                invokeRead();
            } else {
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        invokeRead();
                    }
                });
            }
        } else {
            ctx.read();
        }
    }

    private void invokeRead() {
        try {
            ((ChannelOutboundHandler) handler).read(context);
        } catch (Throwable t) {
            notifyHandlerException(context, t);
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) {
        if (outbound) {
            if (executor.inEventLoop()) {
                invokeWrite(msg, promise);
            } else {
                final long size;

                if (AbstractChannelHandlerContext.ESTIMATE_TASK_SIZE_ON_SUBMIT) {
                    size = handle.size(msg) + AbstractChannelHandlerContext.WRITE_TASK_OVERHEAD;
                    ChannelOutboundBuffer buffer = ctx.channel().unsafe().outboundBuffer();
                    if (buffer != null) {
                        buffer.incrementPendingOutboundBytes(size);
                    }
                } else {
                    size = 0;
                }

                if (!AbstractChannelHandlerContext.safeExecute(executor, new Runnable() {
                    @Override
                    public void run() {
                        decrementPendingBytes(size);
                        invokeWrite(msg, promise);
                    }
                }, promise, msg)) {
                    decrementPendingBytes(size);
                }
            }
        } else {
            ctx.write(msg, promise);
        }
    }

    private void decrementPendingBytes(long size) {
        if (size != 0) {
            ChannelOutboundBuffer buffer = context.channel().unsafe().outboundBuffer();
            if (buffer != null) {
                buffer.decrementPendingOutboundBytes(size);
            }
        }
    }

    private void invokeWrite(Object msg, ChannelPromise promise) {
        try {
            ((ChannelOutboundHandler) handler).write(context, msg, promise);
        } catch (Throwable t) {
            notifyOutboundHandlerException(t, promise);
        }
    }

    @Override
    public void flush(ChannelHandlerContext ctx) {
        if (outbound) {
            if (executor.inEventLoop()) {
                invokeFlush();
            } else {
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        invokeFlush();
                    }
                });
            }
        } else {
            ctx.flush();
        }
    }

    private void invokeFlush() {
        try {
            ((ChannelOutboundHandler) handler).flush(context);
        } catch (Throwable t) {
            notifyHandlerException(context, t);
        }
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        context = new EventExecutorChannelHandlerContext(ctx, executor);
        handle = ctx.channel().config().getMessageSizeEstimator().newHandle();

        if (executor.inEventLoop()) {
            handler.handlerAdded(context);
        } else {
            Throwable error = executor.submit(new Callable<Throwable>() {
                @Override
                public Throwable call() {
                    try {
                        handler.handlerAdded(context);
                        return null;
                    } catch (Throwable cause) {
                        return cause;
                    }
                }
            }).sync().getNow();
            if (error != null) {
                PlatformDependent.throwException(error);
            }
        }
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        if (executor.inEventLoop()) {
            handler.handlerRemoved(context);
        } else {
            Throwable error = executor.submit(new Callable<Throwable>() {
                @Override
                public Throwable call() {
                    try {
                        handler.handlerRemoved(context);
                        return null;
                    } catch (Throwable cause) {
                        return cause;
                    }
                }
            }).sync().getNow();
            if (error != null) {
                PlatformDependent.throwException(error);
            }
        }
    }

    private void notifyHandlerException(ChannelHandlerContext ctx, Throwable cause) {
        if (AbstractChannelHandlerContext.inExceptionCaught(cause)) {
            if (logger.isWarnEnabled()) {
                logger.warn(
                        "An exception was thrown by a user handler " +
                                "while handling an exceptionCaught event", cause);
            }
            return;
        }

        invokeExceptionCaught(ctx, cause);
    }

    @SuppressWarnings("deprecation")
    private void invokeExceptionCaught(ChannelHandlerContext ctx, final Throwable cause) {
        try {
            handler.exceptionCaught(ctx, cause);
        } catch (Throwable error) {
            if (logger.isDebugEnabled()) {
                logger.debug(
                        "An exception {}" +
                                "was thrown by a user handler's exceptionCaught() " +
                                "method while handling the following exception:",
                        ThrowableUtil.stackTraceToString(error), cause);
            } else if (logger.isWarnEnabled()) {
                logger.warn(
                        "An exception '{}' [enable DEBUG level for full stacktrace] " +
                                "was thrown by a user handler's exceptionCaught() " +
                                "method while handling the following exception:", error, cause);
            }
        }
    }

    private static void notifyOutboundHandlerException(Throwable cause, ChannelPromise promise) {
        // Only log if the given promise is not of type VoidChannelPromise as tryFailure(...) is expected to return
        // false.
        PromiseNotificationUtil.tryFailure(promise, cause, promise instanceof VoidChannelPromise ? null : logger);
    }

    private static final class EventExecutorChannelHandlerContext implements ChannelHandlerContext {
        private final ChannelHandlerContext ctx;
        private final EventExecutor executor;

        EventExecutorChannelHandlerContext(ChannelHandlerContext ctx, EventExecutor executor) {
            this.ctx = ctx;
            this.executor = executor;
        }

        @Override
        public Channel channel() {
            return ctx.channel();
        }

        @Override
        public EventExecutor executor() {
            return executor;
        }

        @Override
        public String name() {
            return ctx.name();
        }

        @Override
        public ChannelHandler handler() {
            // TODO: Should this return the wrapped handler ?
            return ctx.handler();
        }

        @Override
        public boolean isRemoved() {
            return ctx.isRemoved();
        }

        @Override
        public ChannelHandlerContext fireChannelRegistered() {
            ctx.fireChannelRegistered();
            return this;
        }

        @Override
        public ChannelHandlerContext fireChannelUnregistered() {
            ctx.fireChannelUnregistered();
            return this;
        }

        @Override
        public ChannelHandlerContext fireChannelActive() {
            ctx.fireChannelActive();
            return this;
        }

        @Override
        public ChannelHandlerContext fireChannelInactive() {
            ctx.fireChannelInactive();
            return this;
        }

        @Override
        public ChannelHandlerContext fireExceptionCaught(Throwable cause) {
            ctx.fireExceptionCaught(cause);
            return this;
        }

        @Override
        public ChannelHandlerContext fireUserEventTriggered(Object evt) {
            ctx.fireUserEventTriggered(evt);
            return this;
        }

        @Override
        public ChannelHandlerContext fireChannelRead(Object msg) {
            ctx.fireChannelRead(msg);
            return this;
        }

        @Override
        public ChannelHandlerContext fireChannelReadComplete() {
            ctx.fireChannelReadComplete();
            return this;
        }

        @Override
        public ChannelHandlerContext fireChannelWritabilityChanged() {
            ctx.fireChannelWritabilityChanged();
            return this;
        }

        @Override
        public ChannelHandlerContext read() {
            ctx.read();
            return this;
        }

        @Override
        public ChannelHandlerContext flush() {
            ctx.flush();
            return this;
        }

        @Override
        public ChannelPipeline pipeline() {
            return ctx.pipeline();
        }

        @Override
        public ByteBufAllocator alloc() {
            return ctx.alloc();
        }

        @Override
        @Deprecated
        public <T> Attribute<T> attr(AttributeKey<T> key) {
            return ctx.attr(key);
        }

        @Override
        @Deprecated
        public <T> boolean hasAttr(AttributeKey<T> key) {
            return ctx.hasAttr(key);
        }

        @Override
        public ChannelFuture bind(SocketAddress localAddress) {
            return ctx.bind(localAddress, newPromise());
        }

        @Override
        public ChannelFuture connect(SocketAddress remoteAddress) {
            return ctx.connect(remoteAddress, newPromise());
        }

        @Override
        public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress) {
            return ctx.connect(remoteAddress, localAddress, newPromise());
        }

        @Override
        public ChannelFuture disconnect() {
            return ctx.disconnect(newPromise());
        }

        @Override
        public ChannelFuture close() {
            return ctx.close(newPromise());
        }

        @Override
        public ChannelFuture register() {
            return ctx.register(newPromise());
        }

        @Override
        public ChannelFuture deregister() {
            return ctx.deregister(newPromise());
        }

        @Override
        public ChannelFuture bind(SocketAddress localAddress, ChannelPromise promise) {
            return ctx.bind(localAddress, promise);
        }

        @Override
        public ChannelFuture connect(SocketAddress remoteAddress, ChannelPromise promise) {
            return ctx.connect(remoteAddress, promise);
        }

        @Override
        public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) {
            return ctx.connect(remoteAddress, localAddress, promise);
        }

        @Override
        public ChannelFuture disconnect(ChannelPromise promise) {
            return ctx.disconnect(promise);
        }

        @Override
        public ChannelFuture close(ChannelPromise promise) {
            return ctx.close(promise);
        }

        @Override
        public ChannelFuture register(ChannelPromise promise) {
            return ctx.register(promise);
        }

        @Override
        public ChannelFuture deregister(ChannelPromise promise) {
            return ctx.deregister(promise);
        }

        @Override
        public ChannelFuture write(Object msg) {
            return ctx.write(msg, newPromise());
        }

        @Override
        public ChannelFuture write(Object msg, ChannelPromise promise) {
            return ctx.write(msg, promise);
        }

        @Override
        public ChannelFuture writeAndFlush(Object msg, ChannelPromise promise) {
            return ctx.writeAndFlush(msg, promise);
        }

        @Override
        public ChannelFuture writeAndFlush(Object msg) {
            return ctx.writeAndFlush(msg, newPromise());
        }

        @Override
        public ChannelPromise newPromise() {
            return new DefaultChannelPromise(channel(), executor());
        }

        @Override
        public ChannelProgressivePromise newProgressivePromise() {
            return new DefaultChannelProgressivePromise(channel(), executor());
        }

        @Override
        public ChannelFuture newSucceededFuture() {
            return new SucceededChannelFuture(channel(), executor());
        }

        @Override
        public ChannelFuture newFailedFuture(Throwable cause) {
            return new FailedChannelFuture(channel(), executor(), cause);
        }

        @Override
        public ChannelPromise voidPromise() {
            return ctx.voidPromise();
        }
    }
}
