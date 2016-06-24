package com.twitter.finagle.http2

import com.twitter.finagle.Http.{param => httpparam}
import com.twitter.finagle.Stack
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.netty4.Netty4Transporter
import com.twitter.finagle.netty4.channel.BufferingChannelOutboundHandler
import com.twitter.finagle.netty4.http.exp.initClient
import com.twitter.finagle.transport.{TransportProxy, Transport}
import com.twitter.logging.Logger
import com.twitter.util.{Future, Promise, Closable, Time}
import io.netty.channel.{ChannelPipeline, ChannelDuplexHandler, ChannelHandlerContext,
  ChannelPromise}
import io.netty.handler.codec.http.{HttpResponse => Netty4Response, _}
import io.netty.handler.codec.http2.HttpConversionUtil.ExtensionHeaderNames.STREAM_ID
import io.netty.handler.codec.http2._
import java.net.SocketAddress
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

private[http2] object Http2Transporter {

  private[this] val log = Logger.get()

  def getStreamId(msg: HttpMessage): Option[Int] =
    Option(msg.headers.getInt(STREAM_ID.text))

  def setStreamId(msg: HttpMessage, id: Int): Unit =
    msg.headers.setInt(STREAM_ID.text, id)

  // constructing an http2 cleartext transport
  private[http2] def init(params: Stack.Params): ChannelPipeline => Unit =
    { pipeline: ChannelPipeline =>
      val connection = new DefaultHttp2Connection(false /*server*/)

      val maxResponseSize = params[httpparam.MaxResponseSize].size
      val maxChunkSize = params[httpparam.MaxChunkSize].size
      val maxHeaderSize = params[httpparam.MaxHeaderSize].size
      val maxInitialLineSize = params[httpparam.MaxInitialLineSize].size

      // decompresses data frames according to the content-encoding header
      val adapter = new DelegatingDecompressorFrameListener(
        connection,
        // adapters http2 to http 1.1
        new InboundHttp2ToHttpAdapterBuilder(connection)
          .maxContentLength(maxResponseSize.inBytes.toInt)
          .propagateSettings(true)
          .build()
      )
      val connectionHandler = new HttpToHttp2ConnectionHandlerBuilder()
        .frameListener(adapter)
        .connection(connection)
        .build()

      val sourceCodec = new HttpClientCodec(
        maxInitialLineSize.inBytes.toInt,
        maxHeaderSize.inBytes.toInt,
        maxChunkSize.inBytes.toInt
      )
      val upgradeCodec = new Http2ClientUpgradeCodec(connectionHandler)
      val upgradeHandler = new HttpClientUpgradeHandler(sourceCodec, upgradeCodec, Int.MaxValue)
      pipeline.addLast(sourceCodec, upgradeHandler, new UpgradeRequestHandler)

      initClient(params)(pipeline)
      log.info(s"Http2Transporter.initClient: pipeline=${pipeline}")
    }

  private[this] val newTransport: Transport[Any, Any] => Transport[Any, Any] =
    { transport: Transport[Any, Any] =>
      new TransportProxy[Any, Any](transport) {
        def write(msg: Any): Future[Unit] = {
          log.info(s"Http2Transporter.newTransport.write($msg)")
          transport.write(msg)
            .ensure(log.info(s"Http2Transporter.newTransport.wrote"))
        }

        def read(): Future[Any] = {
          log.info(s"Http2Transporter.newTransport.read()")
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

  def apply(params: Stack.Params): Transporter[Any, Any] = new Transporter[Any, Any] {
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
          log.info(s"Http2Transporter.Transporter: addr=$addr found")
          f.flatMap { multiplexed =>
            log.info(s"Http2Transporter.Transporter: multiplex($addr)")
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
          } else {
            apply(addr) // lost the race, try again
          }
      }
  }

  // borrows heavily from the netty http2 example
  private[this] class UpgradeRequestHandler
    extends ChannelDuplexHandler
    with BufferingChannelOutboundHandler {

    override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit = {
      // receives an upgrade response
      // removes self from pipeline
      // drops message
      // Done with this handler, remove it from the pipeline.
      log.info(s"Http2Transporter.Upgrader: read($ctx, $msg)")
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
    override def write(ctx: ChannelHandlerContext, msg: Any, promise: ChannelPromise): Unit = {
      val isFirst = first.compareAndSet(true, false)
      log.info(s"Http2Transporter.Upgrader: write($ctx, $msg) first=$isFirst")
      if (isFirst) ctx.writeAndFlush(msg, promise)
      else super.write(ctx, msg, promise) // this buffers the write until the handler is removed
    }
  }
}
