package com.twitter.finagle.http2

import com.twitter.finagle.Stack
import com.twitter.finagle.netty4.Netty4Listener
import com.twitter.finagle.server.Listener
import io.netty.channel._
import io.netty.handler.codec.http.HttpObject
import io.netty.handler.codec.http2.{Http2Frame, Http2FrameLogger, Http2ServerDowngrader, Http2StreamFrame}

/**
 * Please note that the listener cannot be used for TLS yet.
 */
private[http2] object Http2Listener {
  private[this] val log = com.twitter.logging.Logger.get(getClass.getName)

  private[this] def mkHttp2(params: Stack.Params): ChannelInitializer[Channel] => ChannelHandler =
    (init: ChannelInitializer[Channel]) => new Http2ServerInitializer(init, params)

  private[this] val pipelineInit: ChannelPipeline => Unit = { pipeline: ChannelPipeline =>
    pipeline.addLast("debug.h1", new DebugHandler("srv[h1]"))
    pipeline.addLast(new Http2ServerDowngrader(false /*validateHeaders*/))
    pipeline.addLast("debug.h2", new DebugHandler("srv[h2]"))
    log.fatal(s"Http2Listener.pipeline: $pipeline")
  }

  def apply[In, Out](params: Stack.Params): Listener[In, Out] = Netty4Listener(
    pipelineInit = pipelineInit,
    // we turn off backpressure because Http2 only works with autoread on for now
    params = params + Netty4Listener.BackPressure(false),
    handlerDecorator = mkHttp2(params)
  )
}
