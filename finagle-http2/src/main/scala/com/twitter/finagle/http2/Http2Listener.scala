package com.twitter.finagle.http2

import com.twitter.finagle.Stack
import com.twitter.finagle.netty4.Netty4Listener
import com.twitter.finagle.server.Listener
import io.netty.channel._
import io.netty.handler.codec.http.HttpObject
import io.netty.handler.codec.http2.{Http2FrameLogger, Http2ServerDowngrader, Http2StreamFrame}
import io.netty.handler.logging.LogLevel

/**
 * Please note that the listener cannot be used for TLS yet.
 */
private[http2] object Http2Listener {
  val log = com.twitter.logging.Logger.get()

  object Http1Debug extends SimpleChannelInboundHandler[HttpObject](false) {
    override def channelActive(ctx: ChannelHandlerContext): Unit =
      log.info(s"active $ctx")
    override def channelRead0(ctx: ChannelHandlerContext, obj: HttpObject): Unit =
      log.info(s"read0 $ctx $obj")
    override def channelReadComplete(ctx: ChannelHandlerContext): Unit =
      log.info(s"read complete $ctx")
  }

  private[this] val init: ChannelPipeline => Unit = { pipeline: ChannelPipeline =>
    pipeline.addLast(new Http2FrameLogger(LogLevel.INFO))
    pipeline.addLast(new Http2ServerDowngrader(false /*validateHeaders*/))
    pipeline.addLast(Http1Debug)
    log.fatal(s"pipeline: $pipeline")
  }

  private[this] def decorate(params: Stack.Params): ChannelInitializer[Channel] => ChannelHandler =
    (init: ChannelInitializer[Channel]) => new Http2ServerInitializer(init, params)

  def apply[In, Out](params: Stack.Params): Listener[In, Out] = Netty4Listener(
    pipelineInit = init,
    // we turn off backpressure because Http2 only works with autoread on for now
    params = params + Netty4Listener.BackPressure(false),
    handlerDecorator = decorate(params)
  )
}
