/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.app.gpfdist.sink;

import java.time.Duration;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.reactivestreams.Processor;

import reactor.core.publisher.Flux;
import reactor.core.publisher.WorkQueueProcessor;
import reactor.ipc.buffer.Buffer;
import reactor.ipc.netty.common.NettyCodec;
import reactor.ipc.netty.config.ServerOptions;
import reactor.ipc.netty.http.HttpServer;

/**
 * Server implementation around reactor and netty providing endpoint
 * where data can be sent using a gpfdist protocol.
 *
 * @author Janne Valkealahti
 */
public class GpfdistServer {

	private final static Log log = LogFactory.getLog(GpfdistServer.class);

	private final Processor<Buffer, Buffer> processor;
	private final int port;
	private final int flushCount;
	private final int flushTime;
	private final int batchTimeout;
	private final int batchCount;
	private HttpServer server;
	private int localPort = -1;

	/**
	 * Instantiates a new gpfdist server.
	 *
	 * @param processor the processor
	 * @param port the port
	 * @param flushCount the flush count
	 * @param flushTime the flush time
	 * @param batchTimeout the batch timeout
	 * @param batchCount the batch count
	 */
	public GpfdistServer(Processor<Buffer, Buffer> processor, int port, int flushCount, int flushTime,
			int batchTimeout, int batchCount) {
		this.processor = processor;
		this.port = port;
		this.flushCount = flushCount;
		this.flushTime = flushTime;
		this.batchTimeout = batchTimeout;
		this.batchCount = batchCount;
	}

	/**
	 * Start a server.
	 *
	 * @return the http server
	 * @throws Exception the exception
	 */
	public synchronized HttpServer start() throws Exception {
		if (server == null) {
			server = createProtocolListener();
		}
		return server;
	}

	/**
	 * Stop a server.
	 *
	 * @throws Exception the exception
	 */
	public synchronized void stop() throws Exception {
		if (server != null) {
			server.shutdown().block();
		}
		server = null;
	}

	/**
	 * Gets the local port.
	 *
	 * @return the local port
	 */
	public int getLocalPort() {
		return localPort;
	}

	private HttpServer createProtocolListener()
			throws Exception {
		final NettyCodec<Buffer, Buffer> codec = NettyCodec.from(new GpfdistCodec());
		WorkQueueProcessor<Buffer> workProcessor = WorkQueueProcessor.create("gpfdist-sink-worker", 8192, false);

		final Flux<Buffer> stream = Flux
				.from(processor)
				.window(flushCount, Duration.ofSeconds(flushTime))
				.flatMap(s -> s
					.take(Duration.ofSeconds(2))
					.reduceWith(Buffer::new, Buffer::append))
				.subscribeWith(workProcessor)
				.as(Flux::from);
		HttpServer httpServer = HttpServer.create(ServerOptions.on("0.0.0.0", port));
//		HttpServer httpServer = HttpServer.create(ServerOptions.on("0.0.0.0", port).eventLoopGroup(new NioEventLoopGroup(10)));

		httpServer.get("/data", request -> {
			request.removeTransferEncodingChunked();
			request.addResponseHeader("Content-type", "text/plain");
			request.addResponseHeader("Expires", "0");
			request.addResponseHeader("X-GPFDIST-VERSION", "Spring Dataflow");
			request.addResponseHeader("X-GP-PROTO", "1");
			request.addResponseHeader("Cache-Control", "no-cache");
			request.addResponseHeader("Connection", "close");
			return request
					.flushEach()
					.map(stream
						.take(batchCount)
						.timeout(Duration.ofSeconds(batchTimeout), Flux.<Buffer>empty())
						.concatWith(Flux.just(Buffer.wrap(new byte[0]))), codec);
		});

		httpServer.start().block();
		log.info("Server running using address=[" + httpServer.getListenAddress() + "]");
		localPort = httpServer.getListenAddress().getPort();
		return httpServer;
	}
}
