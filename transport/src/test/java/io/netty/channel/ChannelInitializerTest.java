/*
 * Copyright 2016 The Netty Project
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

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalHandler;
import io.netty.channel.local.LocalServerChannel;
import io.netty.util.concurrent.AbstractEventExecutor;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ScheduledFuture;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.channels.ClosedChannelException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

public class ChannelInitializerTest {
    private static final int TIMEOUT_MILLIS = 1000;
    private static final LocalAddress SERVER_ADDRESS = new LocalAddress("addr");
    private EventLoopGroup group;
    private ServerBootstrap server;
    private Bootstrap client;
    private InspectableHandler testHandler;

    @Before
    public void setUp() {
        group = new MultithreadEventLoopGroup(1, LocalHandler.newFactory());
        server = new ServerBootstrap()
                .group(group)
                .channel(LocalServerChannel.class)
                .localAddress(SERVER_ADDRESS);
        client = new Bootstrap()
                .group(group)
                .channel(LocalChannel.class)
                .handler(new ChannelInboundHandlerAdapter());
        testHandler = new InspectableHandler();
    }

    @After
    public void tearDown() {
        group.shutdownGracefully(0, TIMEOUT_MILLIS, TimeUnit.MILLISECONDS).syncUninterruptibly();
    }

    @Test
    public void testInitChannelThrowsRegisterFirst() {
        testInitChannelThrows(true);
    }

    @Test
    public void testInitChannelThrowsRegisterAfter() {
        testInitChannelThrows(false);
    }

    private void testInitChannelThrows(boolean registerFirst) {
        final Exception exception = new Exception();
        final AtomicReference<Throwable> causeRef = new AtomicReference<>();

        ChannelPipeline pipeline = new LocalChannel(group.next()).pipeline();

        if (registerFirst) {
           pipeline.channel().register().syncUninterruptibly();
        }
        pipeline.addFirst(new ChannelInitializer<Channel>() {
            @Override
            protected void initChannel(Channel ch) throws Exception {
                throw exception;
            }

            @Override
            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                causeRef.set(cause);
                super.exceptionCaught(ctx, cause);
            }
        });

        if (!registerFirst) {
            assertTrue(pipeline.channel().register().awaitUninterruptibly().cause() instanceof ClosedChannelException);
        }
        pipeline.channel().close().syncUninterruptibly();
        pipeline.channel().closeFuture().syncUninterruptibly();

        assertSame(exception, causeRef.get());
    }

    @Test
    public void testChannelInitializerInInitializerCorrectOrdering() throws InterruptedException {
        final BlockingQueue<ChannelHandler> handlers = new LinkedBlockingDeque<>();

        class Handler extends ChannelInboundHandlerAdapter {
            @Override
            public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
                handlers.add(this);
            }
        }
        final ChannelHandler handler1 = new Handler();
        final ChannelHandler handler2 = new Handler();
        final ChannelHandler handler3 = new Handler();
        final ChannelHandler handler4 = new Handler();

        client.handler(new ChannelInitializer<Channel>() {
            @Override
            protected void initChannel(Channel ch) throws Exception {
                ch.pipeline().addLast(handler1);
                ch.pipeline().addLast(new ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(Channel ch) throws Exception {
                        ch.pipeline().addLast(handler2);
                        ch.pipeline().addLast(handler3);
                    }
                });
                ch.pipeline().addLast(handler4);
            }
        }).localAddress(LocalAddress.ANY);

        Channel channel = client.bind().syncUninterruptibly().channel();
        try {
            // Execute some task on the EventLoop and wait until its done to be sure all handlers are added to the
            // pipeline.
            channel.eventLoop().submit(new Runnable() {
                @Override
                public void run() {
                    // NOOP
                }
            }).syncUninterruptibly();

            assertSame(handler1, handlers.take());
            assertSame(handler2, handlers.take());
            assertSame(handler3, handlers.take());
            assertSame(handler4, handlers.take());
            assertTrue(handlers.isEmpty());
        } finally {
            channel.close().syncUninterruptibly();
        }
    }

    @Test
    public void testChannelInitializerReentrance() {
        final AtomicInteger registeredCalled = new AtomicInteger(0);
        final ChannelInboundHandlerAdapter handler1 = new ChannelInboundHandlerAdapter() {
            @Override
            public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
                registeredCalled.incrementAndGet();
            }
        };
        final AtomicInteger initChannelCalled = new AtomicInteger(0);
        client.handler(new ChannelInitializer<Channel>() {
            @Override
            protected void initChannel(Channel ch) throws Exception {
                initChannelCalled.incrementAndGet();
                ch.pipeline().addLast(handler1);
                ch.pipeline().fireChannelRegistered();
            }
        }).localAddress(LocalAddress.ANY);

        Channel channel = client.bind().syncUninterruptibly().channel();
        try {
            // Execute some task on the EventLoop and wait until its done to be sure all handlers are added to the
            // pipeline.
            channel.eventLoop().submit(new Runnable() {
                @Override
                public void run() {
                    // NOOP
                }
            }).syncUninterruptibly();
            assertEquals(1, initChannelCalled.get());
            assertEquals(2, registeredCalled.get());
        } finally {
            channel.close().syncUninterruptibly();
        }
    }

    @Test(timeout = TIMEOUT_MILLIS)
    public void firstHandlerInPipelineShouldReceiveChannelRegisteredEvent() {
        testChannelRegisteredEventPropagation(new ChannelInitializer<LocalChannel>() {
            @Override
            public void initChannel(LocalChannel channel) {
                channel.pipeline().addFirst(testHandler);
            }
        });
    }

    @Test(timeout = TIMEOUT_MILLIS)
    public void lastHandlerInPipelineShouldReceiveChannelRegisteredEvent() {
        testChannelRegisteredEventPropagation(new ChannelInitializer<LocalChannel>() {
            @Override
            public void initChannel(LocalChannel channel) {
                channel.pipeline().addLast(testHandler);
            }
        });
    }

    @Test
    public void testAddFirstChannelInitializer() {
        testAddChannelInitializer(true);
    }

    @Test
    public void testAddLastChannelInitializer() {
        testAddChannelInitializer(false);
    }

    private static void testAddChannelInitializer(final boolean first) {
        final AtomicBoolean called = new AtomicBoolean();
        EmbeddedChannel channel = new EmbeddedChannel(new ChannelInitializer<Channel>() {
            @Override
            protected void initChannel(Channel ch) throws Exception {
                ChannelHandler handler = new ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(Channel ch) throws Exception {
                        called.set(true);
                    }
                };
                if (first) {
                    ch.pipeline().addFirst(handler);
                } else {
                    ch.pipeline().addLast(handler);
                }
            }
        });
        channel.finish();
        assertTrue(called.get());
    }

    private void testChannelRegisteredEventPropagation(ChannelInitializer<LocalChannel> init) {
        Channel clientChannel = null, serverChannel = null;
        try {
            server.childHandler(init);
            serverChannel = server.bind().syncUninterruptibly().channel();
            clientChannel = client.connect(SERVER_ADDRESS).syncUninterruptibly().channel();
            assertEquals(1, testHandler.channelRegisteredCount.get());
        } finally {
            closeChannel(clientChannel);
            closeChannel(serverChannel);
        }
    }

    @SuppressWarnings("deprecation")
    @Test(timeout = 10000)
    public void testChannelInitializerEventExecutor() throws Throwable {
        final AtomicInteger invokeCount = new AtomicInteger();
        final AtomicInteger completeCount = new AtomicInteger();
        final AtomicReference<Throwable> errorRef = new AtomicReference<>();
        LocalAddress addr = new LocalAddress("test");

        final EventExecutor executor = new AbstractEventExecutor() {
            private final ScheduledExecutorService execService = Executors.newSingleThreadScheduledExecutor();

            @Override
            public boolean inEventLoop(Thread thread) {
                return false;
            }

            @Override
            public boolean isShuttingDown() {
                return execService.isShutdown();
            }

            @Override
            public Future<?> shutdownGracefully(long quietPeriod, long timeout, TimeUnit unit) {
                shutdown();
                return newSucceededFuture(null);
            }

            @Override
            public Future<?> terminationFuture() {
                return newFailedFuture(new UnsupportedOperationException());
            }

            @Override
            public void shutdown() {
                execService.shutdown();
            }

            @Override
            public List<Runnable> shutdownNow() {
                return execService.shutdownNow();
            }

            @Override
            public boolean isShutdown() {
                return execService.isShutdown();
            }

            @Override
            public boolean isTerminated() {
                return execService.isTerminated();
            }

            @Override
            public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
                return execService.awaitTermination(timeout, unit);
            }

            @Override
            public <T> List<java.util.concurrent.Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
                    throws InterruptedException {
                return execService.invokeAll(tasks);
            }

            @Override
            public <T> List<java.util.concurrent.Future<T>> invokeAll(
                    Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
                return execService.invokeAll(tasks, timeout, unit);
            }

            @Override
            public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
                    throws InterruptedException, ExecutionException {
                return execService.invokeAny(tasks);
            }

            @Override
            public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
                    throws InterruptedException, ExecutionException, TimeoutException {
                return execService.invokeAny(tasks, timeout, unit);
            }

            @Override
            public void execute(Runnable command) {
                execService.execute(command);
            }

            @Override
            public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
                throw new UnsupportedOperationException();
            }

            @Override
            public ScheduledFuture<?> scheduleAtFixedRate(
                    Runnable command, long initialDelay, long period, TimeUnit unit) {
                throw new UnsupportedOperationException();
            }

            @Override
            public ScheduledFuture<?> scheduleWithFixedDelay(
                    Runnable command, long initialDelay, long delay, TimeUnit unit) {
                throw new UnsupportedOperationException();
            }
        };

        final CountDownLatch latch = new CountDownLatch(1);
        ServerBootstrap serverBootstrap = new ServerBootstrap()
                .channel(LocalServerChannel.class)
                .group(group)
                .localAddress(addr)
                .childHandler(new ChannelInitializer<LocalChannel>() {
                    @Override
                    protected void initChannel(LocalChannel ch) {
                        ch.pipeline().addLast("handler", new EventExecutorHandler(executor,
                                new ChannelInitializer<Channel>() {
                            @Override
                            protected void initChannel(Channel ch) {
                                invokeCount.incrementAndGet();
                                ch.pipeline().addAfter(
                                        "handler", null, new EventExecutorHandler(executor,
                                                new ChannelInboundHandlerAdapter() {
                                            @Override
                                            public void channelRead(ChannelHandlerContext ctx, Object msg)  {
                                                // just drop on the floor.
                                            }

                                            @Override
                                            public void handlerRemoved(ChannelHandlerContext ctx) {
                                                latch.countDown();
                                            }
                                        }));
                                completeCount.incrementAndGet();
                            }

                            @Override
                            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                if (cause instanceof AssertionError) {
                                    errorRef.set(cause);
                                }
                            }
                        }));
                    }
                });

        Channel server = serverBootstrap.bind().sync().channel();

        Bootstrap clientBootstrap = new Bootstrap()
                .channel(LocalChannel.class)
                .group(group)
                .remoteAddress(addr)
                .handler(new ChannelInboundHandlerAdapter());

        Channel client = clientBootstrap.connect().sync().channel();
        client.writeAndFlush("Hello World").sync();

        client.close().sync();
        server.close().sync();

        client.closeFuture().sync();
        server.closeFuture().sync();

        // Wait until the handler is removed from the pipeline and so no more events are handled by it.
        latch.await();

        assertEquals(1, invokeCount.get());
        assertEquals(invokeCount.get(), completeCount.get());

        Throwable cause = errorRef.get();
        if (cause != null) {
            throw cause;
        }

        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }

    private static void closeChannel(Channel c) {
        if (c != null) {
            c.close().syncUninterruptibly();
        }
    }

    private static final class InspectableHandler extends ChannelDuplexHandler {
        final AtomicInteger channelRegisteredCount = new AtomicInteger(0);

        @Override
        public void channelRegistered(ChannelHandlerContext ctx) {
            channelRegisteredCount.incrementAndGet();
            ctx.fireChannelRegistered();
        }
    }
}
