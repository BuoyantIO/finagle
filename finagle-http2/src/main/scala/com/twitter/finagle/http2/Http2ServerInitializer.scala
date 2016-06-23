package com.twitter.finagle.http2

import com.twitter.finagle.Http.{param => httpparam}
import com.twitter.finagle.Stack
import com.twitter.finagle.netty4.http.exp.initServer
import com.twitter.logging.Logger
import io.netty.channel.socket.SocketChannel
import io.netty.channel.{Channel, ChannelInitializer}
import io.netty.handler.codec.http.HttpServerUpgradeHandler.{UpgradeCodec, UpgradeCodecFactory}
import io.netty.handler.codec.http.{HttpServerCodec, HttpServerUpgradeHandler}
import io.netty.handler.codec.http2.{Http2CodecUtil, Http2MultiplexCodec, Http2ServerUpgradeCodec}
import io.netty.util.AsciiString

/**
 * The handler will be added to all http2 child channels, and must be Sharable.
 */
private[http2] class Http2ServerInitializer(
    init: ChannelInitializer[Channel],
    params: Stack.Params)
  extends ChannelInitializer[SocketChannel] {

  private[this] val log = Logger.get()

  private[this] object InnerInitializer extends ChannelInitializer[Channel] {
    def initChannel(ch: Channel): Unit = {
      log.info(s"Http2ServerInitializer/InnerInitializer.initChannel($ch)")
      initServer(params)(ch.pipeline)
    }
  }

  private[this] object Initializer extends ChannelInitializer[Channel] {
    def initChannel(ch: Channel): Unit = {
      log.info(s"Http2ServerInitializer/Initializer.initChannel($ch)")
      ch.pipeline.addLast(init)
      ch.pipeline.addLast(InnerInitializer)
    }
  }

  private[this] object CodecUpgrade extends UpgradeCodecFactory {
    override def newUpgradeCodec(protocol: CharSequence): UpgradeCodec = {
      log.info(s"Http2ServerInitializer/CodecUpgrade: protocol=${AsciiString.of(protocol)}")
      if (AsciiString.contentEquals(Http2CodecUtil.HTTP_UPGRADE_PROTOCOL_NAME, protocol)) {
        val rv = new Http2ServerUpgradeCodec(new Http2MultiplexCodec(true, Initializer))
        log.info(s"Http2ServerInitializer/CodecUpgrade: codec installed: $rv")
        rv
      } else {
        log.info("Http2ServerInitializer/CodecUpgrade: codec not installed")
        null
      }
    }
  }

  def initChannel(ch: SocketChannel): Unit = {
    val p = ch.pipeline()
    val sourceCodec = new HttpServerCodec()
    val maxRequestSize = params[httpparam.MaxRequestSize].size

    log.info(s"Http2ServerInitializer.initChannel(${ch})")
    p.addLast(sourceCodec)
    p.addLast(new HttpServerUpgradeHandler(sourceCodec, CodecUpgrade, maxRequestSize.inBytes.toInt))
  }
}
