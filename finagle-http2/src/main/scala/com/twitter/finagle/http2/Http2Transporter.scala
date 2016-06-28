package com.twitter.finagle.http2

import com.twitter.finagle.Http.{param => httpparam}
import com.twitter.finagle.Stack
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.netty4.Netty4Transporter
import com.twitter.finagle.netty4.channel.BufferingChannelOutboundHandler
//import com.twitter.finagle.netty4.http.exp.initClient
import com.twitter.finagle.transport.{TransportProxy, Transport}
import com.twitter.logging.Logger
import com.twitter.util.{Future, Promise, Closable, Time}
import io.netty.channel.{ChannelHandler, ChannelPipeline, ChannelDuplexHandler,
  ChannelHandlerContext, ChannelPromise, SimpleChannelInboundHandler}
import io.netty.handler.codec.http.{HttpResponse => Netty4Response, _}
import io.netty.handler.codec.http2.HttpConversionUtil.ExtensionHeaderNames.STREAM_ID
import io.netty.handler.codec.http2._
import io.netty.handler.logging.LogLevel
import java.net.SocketAddress
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

private[http2] object Http2Transporter {

  private[this] val log = Logger.get()

  def getStreamId(msg: HttpMessage): Option[Int] = {
    val id = msg.headers.getInt(STREAM_ID.text)
    if (id == null) None else Some(id)
  }

  def setStreamId(msg: HttpMessage, id: Int): Unit =
    msg.headers.setInt(STREAM_ID.text, id)

  // constructing an http2 cleartext transport
  private[http2] def init(params: Stack.Params): ChannelPipeline => Unit =
    { pipeline: ChannelPipeline =>
      val maxResponseSize = params[httpparam.MaxResponseSize].size
      // val maxChunkSize = params[httpparam.MaxChunkSize].size
      // val maxHeaderSize = params[httpparam.MaxHeaderSize].size
      // val maxInitialLineSize = params[httpparam.MaxInitialLineSize].size
      // val http1 = new HttpClientCodec(
      //   maxInitialLineSize.inBytes.toInt,
      //   maxHeaderSize.inBytes.toInt,
      //   maxChunkSize.inBytes.toInt
      // )
      // pipeline.addLast("h1", http1)

      // val h2mux = new Http2MultiplexCodec(false /*server*/, new IgnoreInboundHandler)
      val h2conn = new DefaultHttp2Connection(false /*server*/)
      val http2 = new HttpToHttp2ConnectionHandlerBuilder()
        .connection(h2conn)
        .initialSettings(new Http2Settings()
          .pushEnabled(false))
        .frameListener(new InboundHttp2ToHttpAdapterBuilder(h2conn)
          .maxContentLength(maxResponseSize.inBytes.toInt)
          .propagateSettings(true)
          .build())
        .frameLogger(new Http2FrameLogger(LogLevel.ERROR, "tx.h2"))
        .build()

      pipeline.addLast("h1Debug", new DebugHandler("transporter[h1]"))
      pipeline.addLast("h1ToH2", http2)
      pipeline.addLast("h2Debug", new DebugHandler("transporter[h2]"))


      // pipeline.addLast("h1ObjectToH2Frame",
      //   new HttpClientUpgradeHandler(http1, new Http2ClientUpgradeCodec(http2), Int.MaxValue))
      // pipeline.addLast("h2Upgrade", new UpgradeRequestHandler)
      // initClient(params)(pipeline)

      log.info(s"Http2Transporter.initClient: pipeline=${pipeline}")
    }

  private[this] val newTransport: Transport[Any, Any] => Transport[Any, Any] =
    { transport: Transport[Any, Any] =>
      new TransportProxy[Any, Any](transport) {
        def write(msg: Any): Future[Unit] = {
          log.info(s"Http2Transporter.newTransport.writing $msg")
          transport.write(msg)
            .onSuccess(_ => log.info(s"Http2Transporter.newTransport.wrote $msg"))
        }

        def read(): Future[Any] = {
          log.info(s"Http2Transporter.newTransport.reading")
          transport.read().flatMap {
            case res: Netty4Response =>
              log.info(s"Http2Transporter.newTransport.read(): response=$res")
              Future.value(res)

            case settings: Http2Settings =>
              log.info(s"Http2Transporter.newTransport.read(): settings=$settings")
              // drop for now
              // TODO: we should handle settings properly
              read()

            case req => Future.exception(new IllegalArgumentException(
              s"expected a Netty4Response, got a ${req.getClass.getName}"))
          }
        }
      }
    }

  def apply(params: Stack.Params): Transporter[Any, Any] =
    new Transporter[Any, Any] {
      // current http2 client implementation doesn't support
      // netty-style backpressure
      // https://github.com/netty/netty/issues/3667#issue-69640214
      private[this] val underlying = Netty4Transporter[Any, Any](
        init(params),
        params + Netty4Transporter.Backpressure(false)
      )

      private[this] val promisedTransporters =
        new ConcurrentHashMap[SocketAddress, Future[MultiplexedTransporter]]

      private[this] def unsafeCast(trans: Transport[HttpObject, HttpObject]): Transport[Any, Any] =
        trans.map(_.asInstanceOf[HttpObject], _.asInstanceOf[Any])

      @scala.annotation.tailrec
      def apply(addr: SocketAddress): Future[Transport[Any, Any]] =
        Option(promisedTransporters.get(addr)) match {
          case Some(f) =>
            f.flatMap { multiplexed =>
              log.info(s"Http2Transporter.Transporter: found: addr=$addr")
              multiplexed(addr).map(unsafeCast _)
            }

          case None =>
            val p = Promise[MultiplexedTransporter]
            if (promisedTransporters.putIfAbsent(addr, p) == null) {
              log.info(s"Http2Transporter.Transporter: addr=$addr acquiring")
              val f = underlying(addr).map(newTransport).map { trans =>
                new MultiplexedTransporter(
                  Transport.cast[HttpObject, HttpObject](trans),
                  Closable.make { time: Time =>
                    log.info(s"Http2Transporter.Transporter: multiplex: remove($addr)")
                    promisedTransporters.remove(addr, p)
                    Future.Done
                  }
                )
              }
              p.become(f)
              f.flatMap { multiplexed =>
                log.info(s"Http2Transporter.Transporter: multiplex($addr)")
                multiplexed(addr).map(unsafeCast _)
              }
            } else apply(addr) // lost the race, try again
        }
    }

  @ChannelHandler.Sharable
  private[this] class IgnoreInboundHandler extends SimpleChannelInboundHandler[Http2Frame] {
    def channelRead0(ctx: ChannelHandlerContext, frame: Http2Frame): Unit = {
      log.fatal(s"Http2Transporter.IgnoreInbound.channelRead0 $ctx $frame")
    }
  }

  // /** borrows heavily from the netty http2 example */
  // private[this] class UpgradeRequestHandler
  //   extends ChannelDuplexHandler
  //   with BufferingChannelOutboundHandler {
  //   override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit = {
  //     // receives an upgrade response
  //     // removes self from pipeline
  //     // drops message
  //     // Done with this handler, remove it from the pipeline.
  //     log.info(s"Http2Transporter.Upgrader: read($ctx, $msg)")
  //     msg match {
  //       case settings: Http2Settings => // drop!
  //         log.info(s"Http2Transporter.Upgrader.channelRead($ctx, $settings) remove/ignore")
  //         ctx.pipeline.remove(this)
  //       case _ =>
  //         log.info(s"Http2Transporter.Upgrader.channelRead($ctx, $msg) fire read")
  //         ctx.fireChannelRead(msg)
  //     }
  //   }
  //
  //   private[this] val first = new AtomicBoolean(true)
  //
  //   /**
  //    * We write the first message directly and it serves as the upgrade request.
  //    * The rest of the handlers will mutate it to look like a good upgrade
  //    * request.  We buffer the rest of the writes until we upgrade successfully.
  //    */
  //   override def write(ctx: ChannelHandlerContext, msg: Any, promise: ChannelPromise): Unit = {
  //     val isFirst = first.compareAndSet(true, false)
  //     log.info(s"Http2Transporter.Upgrader.write($ctx, $msg) first=$isFirst")
  //     if (isFirst) ctx.writeAndFlush(msg, promise)
  //     else super.write(ctx, msg, promise) // this buffers the write until the handler is removed
  //   }
  // }

  private[this] class DebugHandler(prefix: String)
    extends ChannelDuplexHandler {

    override def channelActive(ctx: ChannelHandlerContext): Unit = {
      log.info(s"$prefix.channelActive $ctx")
      super.channelActive(ctx)
    }

    override def channelInactive(ctx: ChannelHandlerContext): Unit = {
      log.info(s"$prefix.channelInactive $ctx")
      super.channelInactive(ctx)
    }

    override def channelRead(ctx: ChannelHandlerContext, obj: Any): Unit = {
      log.info(s"$prefix.channelRead $ctx $obj")
      super.channelRead(ctx, obj)
    }

    override def channelReadComplete(ctx: ChannelHandlerContext): Unit = {
      log.info(s"$prefix.channelReadComplete $ctx")
      super.channelReadComplete(ctx)
    }

    override def write(ctx: ChannelHandlerContext, obj: Any, p: ChannelPromise): Unit = {
      log.info(new Exception, s"$prefix.write $ctx $obj")
      super.write(ctx, obj, p)
    }
  }
}
