package com.twitter.finagle.http2

import com.twitter.finagle.Http.{param => httpparam}
import com.twitter.finagle.Stack
import com.twitter.finagle.netty4.http.exp.initServer
import com.twitter.logging.Logger
import io.netty.channel.socket.SocketChannel
import io.netty.channel.{Channel, ChannelInitializer}
import io.netty.handler.codec.http.HttpServerUpgradeHandler.{UpgradeCodec, UpgradeCodecFactory}
import io.netty.handler.codec.http.{HttpServerCodec, HttpServerUpgradeHandler}
import io.netty.handler.codec.http2.{
  Http2Codec,
  Http2CodecUtil,
  Http2FrameLogger,
  Http2ServerUpgradeCodec
}
import io.netty.handler.logging.LogLevel
import io.netty.util.AsciiString

/**
 * The handler will be added to all http2 child channels, and must be Sharable.
 */
private[http2] class Http2ServerInitializer(
    init: ChannelInitializer[Channel],
    params: Stack.Params)
  extends ChannelInitializer[SocketChannel] {

  private[this] val log = Logger.get()
  private[this] val prefix = "Http2ServerInitializer"

  def initChannel(ch: SocketChannel): Unit = {
    // ch.pipeline.addLast(new Http2FrameLogger(LogLevel.INFO))

    // val sourceCodec = new HttpServerCodec
    // ch.pipeline.addLast(sourceCodec)

    // val maxRequestSize = params[httpparam.MaxRequestSize].size.inBytes.toInt
    // val upgrader = new HttpServerUpgradeHandler(sourceCodec, CodecUpgrade, maxRequestSize)
    // ch.pipeline.addLast(upgrader)

    ch.pipeline.addLast(new Http2Codec(true, channelInitializer))
    log.info(s"$prefix.initChannel(${ch}): ${ch.pipeline}")
  }

  // private[this] object CodecUpgrade extends UpgradeCodecFactory {
  //   private[this] val prefix = s"${Http2ServerInitializer.this.prefix}.CodecUpgrade"

  //   override def newUpgradeCodec(protocol: CharSequence): UpgradeCodec = {
  //     log.info(s"$prefix.newUpgradeProtocol(${AsciiString.of(protocol)})")
  //     if (AsciiString.contentEquals(Http2CodecUtil.HTTP_UPGRADE_PROTOCOL_NAME, protocol)) {
  //       val rv = new Http2ServerUpgradeCodec(new Http2MultiplexCodec(true, initializer))
  //       log.info(s"$prefix: codec installed: $rv")
  //       rv
  //     } else {
  //       log.info("$prefix: codec not installed")
  //       null
  //     }
  //   }
  // }


  val channelInitializer = new ChannelInitializer[Channel] {
    def initChannel(ch: Channel): Unit = {
      ch.pipeline.addLast(init)
      ch.pipeline.addLast(streamInitalizer)
      log.info(s"$prefix.channelInitializer($ch)")
    }
  }

  val streamInitalizer = new ChannelInitializer[Channel] {
    def initChannel(ch: Channel): Unit = {
      initServer(params)(ch.pipeline)
      log.info(s"$prefix.streamInitializer($ch)")
    }
  }
}
