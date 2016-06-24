package com.twitter.finagle.http2

import com.twitter.finagle.Stack
import com.twitter.finagle.netty4.Netty4Listener
import com.twitter.finagle.server.Listener
import io.netty.channel._
import io.netty.handler.codec.http.HttpObject
import io.netty.handler.codec.http2.{Http2Frame, Http2FrameLogger, Http2ServerDowngrader, Http2StreamFrame}
import io.netty.handler.logging.LogLevel

/**
 * Please note that the listener cannot be used for TLS yet.
 */
private[http2] object Http2Listener {
  val log = com.twitter.logging.Logger.get()

  private[this] def mkHttp2(params: Stack.Params): ChannelInitializer[Channel] => ChannelHandler =
    (init: ChannelInitializer[Channel]) => {
      new Http2ServerInitializer(init, params)
    }

  private[this] val pipelineInit: ChannelPipeline => Unit = { pipeline: ChannelPipeline =>
    pipeline.addLast(new Http2FrameLogger(LogLevel.INFO))

    pipeline.addLast(new SimpleChannelInboundHandler[Http2Frame] {
      private val prefix = s"Http2Listener[http2]"
      override def toString() = prefix
      override def channelActive(ctx: ChannelHandlerContext): Unit =
        log.info(s"$prefix.channelActive $ctx")
      override def channelInactive(ctx: ChannelHandlerContext): Unit =
        log.info(s"$prefix.channelInactive $ctx")
      override def channelRead0(ctx: ChannelHandlerContext, obj: Http2Frame): Unit =
        log.info(s"$prefix.channelRead0 $ctx $obj")
      override def channelReadComplete(ctx: ChannelHandlerContext): Unit =
        log.info(s"$prefix.channelReadComplete $ctx")
    })

    pipeline.addLast(new Http2ServerDowngrader(false /*validateHeaders*/))

    pipeline.addLast(new SimpleChannelInboundHandler[HttpObject] {
      private val prefix = s"Http2Listener[http1]"
      override def toString() = prefix
      override def channelActive(ctx: ChannelHandlerContext): Unit =
        log.info(s"$prefix.channelActive $ctx")
      override def channelInactive(ctx: ChannelHandlerContext): Unit =
        log.info(s"$prefix.channelInactive $ctx")
      override def channelRead0(ctx: ChannelHandlerContext, obj: HttpObject): Unit =
        log.info(s"$prefix.channelRead0 $ctx $obj")
      override def channelReadComplete(ctx: ChannelHandlerContext): Unit =
        log.info(s"$prefix.channelReadComplete $ctx")
    })

    log.fatal(s"Http2Listener.pipeline: $pipeline")
  }

  def apply[In, Out](params: Stack.Params): Listener[In, Out] = Netty4Listener(
    pipelineInit = pipelineInit,
    // we turn off backpressure because Http2 only works with autoread on for now
    params = params + Netty4Listener.BackPressure(false),
    handlerDecorator = mkHttp2(params)
  )
}
