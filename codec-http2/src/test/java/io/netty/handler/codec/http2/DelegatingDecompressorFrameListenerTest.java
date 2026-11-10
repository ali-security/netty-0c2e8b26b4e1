/*
 * Copyright 2025 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License, version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at:
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.netty.handler.codec.http2;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.compression.SnappyFrameEncoder;
import io.netty.handler.codec.compression.ZlibCodecFactory;
import io.netty.handler.codec.compression.ZlibWrapper;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.util.CharsetUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Tests that {@link DelegatingDecompressorFrameListener} hands decompressed data to the delegate listener as it is
 * produced, instead of accumulating the whole decompressed content of a {@code DATA} frame in memory first.
 */
public class DelegatingDecompressorFrameListenerTest {
    private static final int STREAM_ID = 3;

    private EmbeddedChannel channel;
    private ChannelHandlerContext ctx;
    private Http2Connection connection;
    private Http2LocalFlowController flowController;
    private RecordingListener listener;
    // Number of decompressed buffers produced by the decoder of the stream.
    private int decoded;

    @BeforeEach
    public void setUp() throws Exception {
        channel = new EmbeddedChannel(new ChannelInboundHandlerAdapter());
        ctx = channel.pipeline().firstContext();
        connection = new DefaultHttp2Connection(true);
        flowController = mock(Http2LocalFlowController.class);
        connection.local().flowController(flowController);
        connection.remote().createStream(STREAM_ID, false);
        listener = new RecordingListener();
    }

    @AfterEach
    public void tearDown() {
        channel.finishAndReleaseAll();
    }

    @Test
    public void snappyDecompressedBuffersAreDeliveredAsTheyAreProduced() throws Exception {
        final int chunks = 16;
        // SnappyFrameEncoder splits its input into chunks of at most Short.MAX_VALUE bytes, each of them is
        // decompressed into its own buffer.
        final int length = chunks * Short.MAX_VALUE;
        ByteBuf compressed = compress(new EmbeddedChannel(new SnappyFrameEncoder()), new byte[length]);

        DelegatingDecompressorFrameListener decompressorListener = newDecompressorListener();
        decompressorListener.onHeadersRead(ctx, STREAM_ID, newHeaders(HttpHeaderValues.SNAPPY), 0, false);
        try {
            // A single DATA frame that decompresses into many buffers.
            decompressorListener.onDataRead(ctx, STREAM_ID, compressed, 0, true);
        } finally {
            compressed.release();
        }

        assertEquals(length, listener.totalBytes);
        assertEquals(chunks, listener.chunkSizes.size());
        // Every decompressed buffer must be handed to the listener before the next one is produced, so the
        // decompressed content of a frame is never accumulated in memory.
        assertEquals(0, listener.maxBacklog);
        assertTrue(listener.endOfStreamReceived);
    }

    @Test
    public void gzipBombIsDeliveredIncrementally() throws Exception {
        final int length = 1024 * 1024;
        ByteBuf compressed = compress(new EmbeddedChannel(ZlibCodecFactory.newZlibEncoder(ZlibWrapper.GZIP)),
                new byte[length]);
        // The DATA frame is orders of magnitude smaller than the content it decompresses into.
        assertTrue(compressed.readableBytes() < length / 100,
                "compressed size: " + compressed.readableBytes());

        DelegatingDecompressorFrameListener decompressorListener = newDecompressorListener();
        decompressorListener.onHeadersRead(ctx, STREAM_ID, newHeaders(HttpHeaderValues.GZIP), 0, false);
        try {
            decompressorListener.onDataRead(ctx, STREAM_ID, compressed, 0, true);
        } finally {
            compressed.release();
        }

        assertEquals(length, listener.totalBytes);
        // The decompressed content must be streamed to the listener in pieces and not as one huge buffer.
        assertTrue(listener.chunkSizes.size() > 1, "chunks: " + listener.chunkSizes.size());
        assertTrue(listener.maxChunkSize < length, "max chunk size: " + listener.maxChunkSize);
        assertEquals(0, listener.maxBacklog);
        assertTrue(listener.endOfStreamReceived);
    }

    @Test
    public void paddingIsReturnedToFlowControl() throws Exception {
        final int padding = 10;
        final byte[] content = "hello world, hello world, hello world".getBytes(CharsetUtil.US_ASCII);
        ByteBuf compressed = compress(new EmbeddedChannel(ZlibCodecFactory.newZlibEncoder(ZlibWrapper.GZIP)),
                content);
        final int compressedBytes = compressed.readableBytes();

        DelegatingDecompressorFrameListener decompressorListener = newDecompressorListener();
        decompressorListener.onHeadersRead(ctx, STREAM_ID, newHeaders(HttpHeaderValues.GZIP), 0, false);
        try {
            // The listener reports the padding as processed, which must not exceed the decompressed byte count.
            assertEquals(0, decompressorListener.onDataRead(ctx, STREAM_ID, compressed, padding, true));
        } finally {
            compressed.release();
        }

        assertEquals(content.length, listener.totalBytes);
        // All the bytes of the frame, padding included, are returned to the flow control window.
        verify(flowController).consumeBytes(connection.stream(STREAM_ID), compressedBytes + padding);
        assertTrue(listener.endOfStreamReceived);
    }

    private DelegatingDecompressorFrameListener newDecompressorListener() {
        return new DelegatingDecompressorFrameListener(connection, listener) {
            @Override
            protected EmbeddedChannel newContentDecompressor(ChannelHandlerContext ctx, CharSequence contentEncoding)
                    throws Http2Exception {
                EmbeddedChannel decompressor = super.newContentDecompressor(ctx, contentEncoding);
                if (decompressor != null) {
                    // Count every buffer produced by the decoder before it is handed over for delivery.
                    decompressor.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                        @Override
                        public void channelRead(ChannelHandlerContext decoderCtx, Object msg) {
                            decoded++;
                            decoderCtx.fireChannelRead(msg);
                        }
                    });
                }
                return decompressor;
            }
        };
    }

    private static Http2Headers newHeaders(CharSequence contentEncoding) {
        return new DefaultHttp2Headers().method("POST").path("/")
                .set(HttpHeaderNames.CONTENT_ENCODING, contentEncoding);
    }

    private static ByteBuf compress(EmbeddedChannel encoder, byte[] content) {
        encoder.writeOutbound(Unpooled.wrappedBuffer(content));
        encoder.finish();
        ByteBuf compressed = Unpooled.buffer();
        for (;;) {
            ByteBuf buf = encoder.readOutbound();
            if (buf == null) {
                break;
            }
            compressed.writeBytes(buf);
            buf.release();
        }
        return compressed;
    }

    private static void releaseIfNeeded(ByteBuf buf) {
        if (buf.refCnt() > 0) {
            buf.release(buf.refCnt());
        }
    }

    /**
     * Regression test for CVE-2026-56819. The per-stream decompressor {@link EmbeddedChannel} is closed as soon as
     * a {@code DATA} frame with {@code endOfStream} was processed, but it stays attached to the stream until the
     * stream is removed from the connection. Any {@code DATA} frame which is received in between therefore still
     * reaches the decompressor. Writing into a closed {@link EmbeddedChannel} only fails <em>after</em> the passed
     * in buffer was retained and the channel will not release it in that case, so a remote peer was able to leak
     * one reference of the received buffer per frame and so exhaust the memory of the JVM.
     */
    @Test
    public void dataFramesAfterDecompressorClosedDoNotLeak() throws Exception {
        final DelegatingDecompressorFrameListener decompressorListener = newDecompressorListener();
        decompressorListener.onHeadersRead(ctx, STREAM_ID, newHeaders(HttpHeaderValues.GZIP), 0, false);

        // Closes (finishes) the decompressor while it is still attached to the stream.
        ByteBuf endOfStreamData = Unpooled.buffer();
        try {
            decompressorListener.onDataRead(ctx, STREAM_ID, endOfStreamData, 0, true);
        } finally {
            releaseIfNeeded(endOfStreamData);
        }
        assertTrue(listener.endOfStreamReceived);

        // Replay the same buffer as if it were received via a flood of DATA frames for the already finished
        // stream. Each of them must fail the stream without taking ownership of the buffer, so its reference
        // count must not grow. Without the fix it climbs by one for every frame.
        final ByteBuf data = Unpooled.buffer().writeZero(16);
        try {
            for (int i = 0; i < 8; i++) {
                Http2Exception e = assertThrows(Http2Exception.class, new Executable() {
                    @Override
                    public void execute() throws Throwable {
                        decompressorListener.onDataRead(ctx, STREAM_ID, data, 0, false);
                    }
                });
                assertEquals(Http2Error.INTERNAL_ERROR, e.error());
                assertEquals(1, data.refCnt(), "buffer was retained by DATA frame " + i + " and never released");
            }
        } finally {
            releaseIfNeeded(data);
        }
    }

    private final class RecordingListener extends Http2FrameAdapter {
        final List<Integer> chunkSizes = new ArrayList<Integer>();
        int totalBytes;
        int maxChunkSize;
        int maxBacklog;
        boolean endOfStreamReceived;

        @Override
        public int onDataRead(ChannelHandlerContext ctx, int streamId, ByteBuf data, int padding,
                              boolean endOfStream) {
            int readable = data.readableBytes();
            if (readable > 0) {
                chunkSizes.add(readable);
                totalBytes += readable;
                maxChunkSize = Math.max(maxChunkSize, readable);
                // Buffers which were decompressed but not handed to the listener yet are held in memory.
                maxBacklog = Math.max(maxBacklog, decoded - chunkSizes.size());
            }
            if (endOfStream) {
                endOfStreamReceived = true;
            }
            return readable + padding;
        }
    }
}
