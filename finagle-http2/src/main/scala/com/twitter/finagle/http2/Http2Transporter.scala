package com.twitter.finagle.http2

import com.twitter.finagle.Http.{param => httpparam}
import com.twitter.finagle.Stack
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.netty4.Netty4Transporter
import com.twitter.finagle.netty4.channel.BufferingChannelOutboundHandler
import com.twitter.finagle.netty4.http.exp.initClient
import com.twitter.finagle.transport.{TransportProxy, Transport}
import com.twitter.util.{Future, Promise, Closable, Time}
import io.netty.channel._
import io.netty.handler.codec.http.{HttpResponse => Netty4Response, _}
import io.netty.handler.codec.http2.HttpConversionUtil.ExtensionHeaderNames.STREAM_ID
import io.netty.handler.codec.http2._
import io.netty.handler.logging.LogLevel
import java.net.SocketAddress
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

private[http2] object Http2Transporter {

  def getStreamId(msg: HttpMessage): Option[Int] = {
    val num = msg.headers.getInt(STREAM_ID.text())

    // java / scala primitive compatibility stuff
    if (num == null) None
    else Some(num)
  }

  def setStreamId(msg: HttpMessage, id: Int): Unit =
    msg.headers.setInt(STREAM_ID.text(), id)

  // XXX This doesn't really work yet.
  private[http2] def init(params: Stack.Params): ChannelPipeline => Unit = {
    val maxResponseSize = params[httpparam.MaxResponseSize].size.inBytes.toInt
    val maxChunkSize = params[httpparam.MaxChunkSize].size.inBytes.toInt
    val maxHeaderSize = params[httpparam.MaxHeaderSize].size.inBytes.toInt
    val maxInitialLineSize = params[httpparam.MaxInitialLineSize].size.inBytes.toInt

    // At the head of this pipeline is an Http2 client to a remote
    // server. At the tail is an Http1 application. Reads go from
    // Http2 to Http1 and writes from Http1 to Http2.
    { pipeline: ChannelPipeline =>
      pipeline.addLast("h2.debug", new DebugHandler("client[h2]"))

      // Prevent writing http2 requests until a SETTINGS message is
      // received from the server.
      //pipeline.addLast("bufferH2UntilUpgraded", new UpgradeRequestHandler)

      // val h2mux = new Http2MultiplexCodec(false /*server*/, IgnoreServerStreams)
      // pipeline.addLast("h2.mux", h2mux)

      // Translates outbound messages (requests written to the client)
      // from Http1 to Http2 and inbound messages (responses read from
      // the client) from Http2 to Http1.
      val inboundH2OutboundH1 = {
        val connection = new DefaultHttp2Connection(false /*server*/)

        val inboundH2ToH1 = new InboundHttp2ToHttpAdapterBuilder(connection)
          .maxContentLength(maxResponseSize)
          .propagateSettings(true)
          .build()

        // XXX Generally, requests should be read in the appropriate
        // content-encoding.  However, in linkerd we shouldn't have to
        // touch bodies in any way.
        // val decompressInbound = new DelegatingDecompressorFrameListener(connection, inboundH2ToH1)

        new HttpToHttp2ConnectionHandlerBuilder()
          .connection(connection)
          .initialSettings(new Http2Settings().pushEnabled(false))
          .frameListener(inboundH2ToH1)
          .frameLogger(new Http2FrameLogger(LogLevel.INFO))
          .build()
      }

      // val h1 = new HttpClientCodec(maxInitialLineSize, maxHeaderSize, maxChunkSize)
      // val h2Upgrade = new HttpClientUpgradeHandler(h1, new Http2ClientUpgradeCodec(inboundH2OutboundH1), Int.MaxValue)

      pipeline.addLast("h2.debug.upgrade", new DebugHandler("client[h2.upgrade]"))
      pipeline.addLast("h2upgrade", inboundH2OutboundH1)

      // pipeline.addLast("h1.debug.codec", new DebugHandler("client[h1.codec]"))
      // pipeline.addLast("h1", h1)

      // Add Http1 client stuff to the pipeline
      initClient(params)(pipeline)
      pipeline.addLast("h1.debug", new DebugHandler("client[h1]"))
    }
  }

  def apply(params: Stack.Params): Transporter[Any, Any] = new Transporter[Any, Any] {
    // current http2 client implementation doesn't support
    // netty-style backpressure
    // https://github.com/netty/netty/issues/3667#issue-69640214
    private[this] val underlying = Netty4Transporter[Any, Any](init(params), params + Netty4Transporter.Backpressure(false))

    private[this] val map = new ConcurrentHashMap[SocketAddress, Future[MultiplexedTransporter]]()

    private[this] def newConnection(addr: SocketAddress): Future[Transport[Any, Any]] = underlying(addr).map { transport =>
      new TransportProxy[Any, Any](transport) {
        def write(msg: Any): Future[Unit] =
          transport.write(msg)

        def read(): Future[Any] = {
          transport.read().flatMap {
            case req: Netty4Response =>
              Future.value(req)
            case settings: Http2Settings =>
              // drop for now
              // TODO: we should handle settings properly
              read()
            case req => Future.exception(new IllegalArgumentException(
              s"expected a Netty4Response, got a ${req.getClass.getName}"))
          }
        }
      }
    }

    private[this] def unsafeCast(trans: Transport[HttpObject, HttpObject]): Transport[Any, Any] =
      trans.map(_.asInstanceOf[HttpObject], _.asInstanceOf[Any])

    def apply(addr: SocketAddress): Future[Transport[Any, Any]] = Option(map.get(addr)) match {
      case Some(f) =>
        f.flatMap { multiplexed =>
          multiplexed(addr).map(unsafeCast _)
        }
      case None =>
        val p = Promise[MultiplexedTransporter]()
        if (map.putIfAbsent(addr, p) == null) {
          val f = newConnection(addr).map { trans =>
            // this has to be lazy because it has a forward reference to itself,
            // and that's illegal with strict values.
            lazy val multiplexed: MultiplexedTransporter = new MultiplexedTransporter(
              Transport.cast[HttpObject, HttpObject](trans),
              Closable.make { time: Time =>
                map.remove(addr, multiplexed)
                Future.Done
              }
            )
            multiplexed
          }
          p.become(f)
          f.flatMap { multiplexed =>
            multiplexed(addr).map(unsafeCast _)
          }
        } else {
          apply(addr) // lost the race, try again
        }
    }
  }

  // borrows heavily from the netty http2 example
  class UpgradeRequestHandler extends ChannelDuplexHandler with BufferingChannelOutboundHandler {
    override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit = {
      // receives an upgrade response
      // removes self from pipeline
      // drops message
      // Done with this handler, remove it from the pipeline.
      msg match {
        case settings: Http2Settings => // drop!
          ctx.pipeline.remove(this)
        case _ =>
          ctx.fireChannelRead(msg)
      }
    }

    private[this] val first = new AtomicBoolean(true)

    /**
     * We write the first message directly and it serves as the upgrade request.
     * The rest of the handlers will mutate it to look like a good upgrade
     * request.  We buffer the rest of the writes until we upgrade successfully.
     */
    override def write(ctx: ChannelHandlerContext, msg: Any, promise: ChannelPromise): Unit =
      if (first.compareAndSet(true, false)) ctx.writeAndFlush(msg, promise)
      else super.write(ctx, msg, promise) // this buffers the write until the handler is removed
  }

  @ChannelHandler.Sharable
  object IgnoreServerStreams extends SimpleChannelInboundHandler[Any]() {
    def channelRead0(ctx: ChannelHandlerContext, obj: Any): Unit = {} // ignore
  }
}
