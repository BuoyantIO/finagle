package com.twitter.finagle.http

import com.twitter.conversions.storage._
import com.twitter.conversions.time._
import com.twitter.finagle
import com.twitter.finagle._
import com.twitter.finagle.context.{Contexts, Deadline, Retries}
import com.twitter.finagle.http.service.HttpResponseClassifier
import com.twitter.finagle.param.Stats
import com.twitter.finagle.service.{ResponseClass, FailureAccrualFactory, ServiceFactoryRef}
import com.twitter.finagle.stats.{NullStatsReceiver, InMemoryStatsReceiver}
import com.twitter.finagle.tracing.Trace
import com.twitter.finagle.util.HashedWheelTimer
import com.twitter.io.{Buf, Reader, Writer}
import com.twitter.util._
import java.io.{PrintWriter, StringWriter}
import java.net.InetSocketAddress
import org.scalatest.{BeforeAndAfter, FunSuite}
import scala.language.reflectiveCalls

abstract class AbstractEndToEndTest extends FunSuite with BeforeAndAfter {
  sealed trait Feature
  object BasicFunctionality extends Feature
  object ClientAbort extends Feature
  object CompressedContent extends Feature
  object HandlesExpect extends Feature
  object InitialLineLength extends Feature
  object MaxHeaderSize extends Feature
  object ResponseClassifier extends Feature
  object CloseStream extends Feature
  object Streaming extends Feature
  object StreamFixed extends Feature
  object TooLongStream extends Feature

  var saveBase: Dtab = Dtab.empty
  private val statsRecv: InMemoryStatsReceiver = new InMemoryStatsReceiver()

  before {
    saveBase = Dtab.base
    Dtab.base = Dtab.read("/foo=>/bar; /baz=>/biz")
    statsRecv.clear()
  }

  after {
    Dtab.base = saveBase
    statsRecv.clear()
  }

  type HttpService = Service[Request, Response]
  type HttpTest = (HttpService => HttpService) => Unit

  def drip(w: Writer): Future[Unit] = w.write(buf("*")) before drip(w)
  def buf(msg: String): Buf = Buf.Utf8(msg)

  def implName: String
  def clientImpl(): finagle.Http.Client
  def serverImpl(): finagle.Http.Server
  def initClient(client: HttpService): Unit = {}
  def initService: HttpService = Service.mk { req: Request =>
    Future.exception(new Exception("boom!"))
  }
  def featureImplemented(feature: Feature): Boolean
  def testIfImplemented(feature: Feature)(name: String)(testFn: => Unit): Unit = {
    if (!featureImplemented(feature)) ignore(name)(testFn) else test(name)(testFn)
  }

  /**
   * Read `n` number of bytes from the bytestream represented by `r`.
   */
  def readNBytes(n: Int, r: Reader): Future[Buf] = {
    def loop(left: Buf): Future[Buf] = (n - left.length) match {
      case x if x > 0 =>
        r.read(x) flatMap {
          case Some(right) => loop(left concat right)
          case None => Future.value(left)
        }
      case _ => Future.value(left)
    }

    loop(Buf.Empty)
  }

  private def requestWith(status: Status): Request =
    Request("/", ("statusCode", status.code.toString))

  private val statusCodeSvc = new HttpService {
    def apply(request: Request) = {
      val statusCode = request.getIntParam("statusCode", Status.BadRequest.code)
      Future.value(Response(Status.fromCode(statusCode)))
    }
  }

  def run(tests: HttpTest*)(connect: HttpService => HttpService): Unit = {
    tests.foreach(t => t(connect))
  }

  def standardErrors(connect: HttpService => HttpService): Unit = {
    testIfImplemented(InitialLineLength)(implName + ": request uri too long") {
      val service = new HttpService {
        def apply(request: Request) = Future.value(Response())
      }
      val client = connect(service)
      val request = Request("/" + "a" * 4096)
      val response = Await.result(client(request), 5.seconds)
      assert(response.status == Status.RequestURITooLong)
      Await.ready(client.close(), 5.seconds)
    }

    test(implName + ": request header fields too large") {
      val service = new HttpService {
        def apply(request: Request) = Future.value(Response())
      }
      val client = connect(service)
      val request = Request("/")
      request.headers().add("header", "a" * 8192)
      val response = Await.result(client(request), 5.seconds)
      assert(response.status == Status.RequestHeaderFieldsTooLarge)
      Await.ready(client.close(), 5.seconds)
    }

    testIfImplemented(ResponseClassifier)(implName + ": with default client-side ResponseClassifier") {
      val service = new HttpService {
        def apply(request: Request) = Future.value(Response())
      }
      val client = connect(service)

      Await.ready(client(requestWith(Status.Ok)), 1.second)
      assert(statsRecv.counters(Seq("client", "requests")) == 1)
      assert(statsRecv.counters(Seq("client", "success")) == 1)

      Await.ready(client(requestWith(Status.ServiceUnavailable)), 1.second)
      assert(statsRecv.counters(Seq("client", "requests")) == 2)
      // by default any `Return` is a successful response.
      assert(statsRecv.counters(Seq("client", "success")) == 2)

      Await.ready(client.close(), 5.seconds)
    }

    testIfImplemented(ResponseClassifier)(implName + ": with default server-side ResponseClassifier") {
      val client = connect(statusCodeSvc)

      Await.ready(client(requestWith(Status.Ok)), 1.second)
      assert(statsRecv.counters(Seq("server", "requests")) == 1)
      assert(statsRecv.counters(Seq("server", "success")) == 1)

      Await.ready(client(requestWith(Status.ServiceUnavailable)), 1.second)
      assert(statsRecv.counters(Seq("server", "requests")) == 2)
      // by default any `Return` is a successful response.
      assert(statsRecv.counters(Seq("server", "success")) == 2)

      Await.ready(client.close(), 5.seconds)
    }

    test(implName + ": unhandled exceptions are converted into 500s") {
      val service = new HttpService {
        def apply(request: Request) = Future.exception(new IllegalArgumentException("bad news"))
      }

      val client = connect(service)
      val response = Await.result(client(Request("/")), 5.seconds)
      assert(response.status == Status.InternalServerError)
      Await.ready(client.close(), 5.seconds)
    }

    test(implName + ": HTTP/1.0") {
      val service = new HttpService {
        def apply(request: Request) = Future.value(Response(request.version, Status.Ok))
      }
      val client = connect(service)
      val request = Request(Method.Get, "/http/1.0")
      request.version = Version.Http10
      val response = Await.result(client(request), 5.seconds)
      assert(response.status == Status.Ok)
      Await.ready(client.close(), 5.seconds)
    }

    test(implName + ": with 'Connection: close'") {
      val service = new HttpService {
        def apply(request: Request) = Future.value(Response())
      }
      val client = connect(service)
      val request = Request(Method.Get, "/connection-close")
      request.headerMap("Connection") = "close"
      val response = Await.result(client(request), 5.seconds)
      assert(response.status == Status.Ok)
      Await.ready(client.close(), 5.seconds)
    }

    test(implName + ": return 413s for fixed-length requests with too large payloads") {
      val service = new HttpService {
        def apply(request: Request) = Future.value(Response())
      }
      val client = connect(service)

      val tooBig = Request("/")
      tooBig.content = Buf.ByteArray.Owned(new Array[Byte](200))

      val justRight = Request("/")
      justRight.content = Buf.ByteArray.Owned(Array[Byte](100))

      assert(Await.result(client(tooBig), 5.seconds).status == Status.RequestEntityTooLarge)
      assert(Await.result(client(justRight), 5.seconds).status == Status.Ok)
      Await.ready(client.close(), 5.seconds)
    }

    testIfImplemented(TooLongStream)(implName + ": return 413s for chunked requests which stream too much data") {
      val service = new HttpService {
        def apply(request: Request) = Future.value(Response())
      }
      val client = connect(service)

      val justRight = Request("/")
      assert(Await.result(client(justRight), 2.seconds).status == Status.Ok)

      val tooMuch = Request("/")
      tooMuch.setChunked(true)
      val w = tooMuch.writer
      w.write(buf("a"*1000)).before(w.close)
      val res = Await.result(client(tooMuch), 2.seconds)
      assert(res.status == Status.RequestEntityTooLarge)
      Await.ready(client.close(), 5.seconds)
    }
  }

  def standardBehaviour(connect: HttpService => HttpService) {

    testIfImplemented(MaxHeaderSize)(implName + ": client stack observes max header size") {
      val service = new HttpService {
        def apply(req: Request) = {
          val res = Response()
          res.headerMap.put("Foo", ("*" * 8192) + "Bar: a")
          Future.value(res)
        }
      }
      val client = connect(service)
      // Whether this fails or not, which determined by configuration of max
      // header size in client configuration, there should definitely be no
      // "Bar" header.
      val hasBar = client(Request()).transform {
        case Throw(_) => Future.False
        case Return(res) =>
          val names = res.headerMap.keys
          Future.value(names.exists(_.contains("Bar")))
      }
      assert(!Await.result(hasBar, 5.seconds))
      Await.ready(client.close(), 5.seconds)
    }

    test(implName + ": client sets content length") {
      val service = new HttpService {
        def apply(request: Request) = {
          val response = Response()
          val len = request.headerMap.get(Fields.ContentLength)
          response.contentString = len.getOrElse("")
          Future.value(response)
        }
      }
      val body = "hello"
      val client = connect(service)
      val req = Request()
      req.contentString = body
      assert(Await.result(client(req), 5.seconds).contentString == body.length.toString)
      Await.ready(client.close(), 5.seconds)
    }

    test(implName + ": echo") {
      val service = new HttpService {
        def apply(request: Request) = {
          val response = Response()
          response.contentString = request.uri
          Future.value(response)
        }
      }

      val client = connect(service)
      val response = client(Request("123"))
      assert(Await.result(response, 5.seconds).contentString == "123")
      Await.ready(client.close(), 5.seconds)
    }

    test(implName + ": dtab") {
      val service = new HttpService {
        def apply(request: Request) = {
          val stringer = new StringWriter
          val printer = new PrintWriter(stringer)
          Dtab.local.print(printer)
          val response = Response(request)
          response.contentString = stringer.toString
          Future.value(response)
        }
      }

      val client = connect(service)

      Dtab.unwind {
        Dtab.local ++= Dtab.read("/a=>/b; /c=>/d")

        val res = Await.result(client(Request("/")), 5.seconds)
        assert(res.contentString == "Dtab(2)\n\t/a => /b\n\t/c => /d\n")
      }

      
      Await.ready(client.close(), 5.seconds)
    }

    test(implName + ": (no) dtab") {
      val service = new HttpService {
        def apply(request: Request) = {
          val stringer = new StringWriter

          val response = Response(request)
          response.contentString = "%d".format(Dtab.local.length)
          Future.value(response)
        }
      }

      val client = connect(service)

      val res = Await.result(client(Request("/")), 5.seconds)
      assert(res.contentString == "0")

      Await.ready(client.close(), 5.seconds)
    }

    test(implName + ": context") {
      val writtenDeadline = Deadline.ofTimeout(5.seconds)
      val service = new HttpService {
        def apply(request: Request) = {
          val deadline = Deadline.current.get
          assert(deadline.deadline == writtenDeadline.deadline)

          val retries = Retries.current.get
          assert(retries == Retries(0))

          val response = Response(request)
          Future.value(response)
        }
      }

      Contexts.broadcast.let(Deadline, writtenDeadline) {
        val req = Request()
        val client = connect(service)
        val res = Await.result(client(Request("/")), 5.seconds)
        assert(res.status == Status.Ok)
        Await.ready(client.close(), 5.seconds)
      }
    }

    testIfImplemented(ClientAbort)(implName + ": client abort") {
      import com.twitter.conversions.time._
      val timer = new JavaTimer
      val promise = new Promise[Response]
      val service = new HttpService {
        def apply(request: Request) = promise
      }
      val client = connect(service)
      client(Request())
      Await.result(timer.doLater(20.milliseconds) {
        Await.result(client.close(), 5.seconds)
        intercept[CancelledRequestException] {
          promise.isInterrupted match {
            case Some(intr) => throw intr
            case _ =>
          }
        }
      }, 5.seconds)
    }

    test(implName + ": measure payload size") {
      val service = new HttpService {
        def apply(request: Request) = {
          val rep = Response()
          rep.content = request.content.concat(request.content)

          Future.value(rep)
        }
      }

      val client = connect(service)
      val req = Request()
      req.content = Buf.Utf8("." * 10)
      Await.ready(client(req), 5.seconds)

      assert(statsRecv.stat("client", "request_payload_bytes")() == Seq(10.0f))
      assert(statsRecv.stat("client", "response_payload_bytes")() == Seq(20.0f))
      assert(statsRecv.stat("server", "request_payload_bytes")() == Seq(10.0f))
      assert(statsRecv.stat("server", "response_payload_bytes")() == Seq(20.0f))
      Await.ready(client.close(), 5.seconds)
    }
  }

  def streaming(connect: HttpService => HttpService) {
    def service(r: Reader) = new HttpService {
      def apply(request: Request) = {
        val response = new Response {
          final val httpResponse = request.response.httpResponse
          override def reader = r
        }
        response.setChunked(true)
        Future.value(response)
      }
    }

    testIfImplemented(CloseStream)(s"$implName (streaming)" + ": stream") {
      val writer = Reader.writable()
      val client = connect(service(writer))
      val reader = Await.result(client(Request()), 5.seconds).reader
      Await.result(writer.write(buf("hello")), 5.seconds)
      assert(Await.result(readNBytes(5, reader), 5.seconds) == Buf.Utf8("hello"))
      Await.result(writer.write(buf("world")), 5.seconds)
      assert(Await.result(readNBytes(5, reader), 5.seconds) == Buf.Utf8("world"))
      Await.ready(client.close(), 5.seconds)
    }

    test(s"$implName (streaming)" + ": stream via ResponseProxy filter") {
      class ResponseProxyFilter extends SimpleFilter[Request, Response] {
        override def apply(request: Request, service: Service[Request, Response]): Future[Response] = {
          service(request).map { responseOriginal =>
            new ResponseProxy {
              override val response = responseOriginal
              override def reader = responseOriginal.reader
            }
          }
        }
      }

      def service = new HttpService {
        def apply(request: Request) = {
          val response = Response()
          response.setChunked(true)
          response.writer.write(buf("goodbye")).before {
            response.writer.write(buf("world")).before {
              response.close()
            }
          }
          Future.value(response)
        }
      }

      val serviceWithResponseProxy = (new ResponseProxyFilter).andThen(service)

      val client = connect(serviceWithResponseProxy)
      val response = Await.result(client(Request()), 5.seconds)
      val Buf.Utf8(actual) = Await.result(Reader.readAll(response.reader), 5.seconds)
      assert(actual == "goodbyeworld")
      Await.ready(client.close(), 5.seconds)
    }

    test(s"$implName (streaming)" + ": stream via ResponseProxy class") {
      case class EnrichedResponse(resp: Response) extends ResponseProxy {
        override val response = resp
      }

      // Test streaming partial data separated in time
      def service = new HttpService {
        def apply(request: Request) = {
          val response = EnrichedResponse(Response(Version.Http11, Status.Ok))
          response.setChunked(true)

          response.writer.write(Buf.Utf8("hello")) before {
            Future.sleep(Duration.fromSeconds(3))(HashedWheelTimer.Default) before {
              response.writer.write(Buf.Utf8("world")) ensure {
                response.close()
              }
            }
          }

          Future.value(response)
        }
      }

      val client = connect(service)
      val response = Await.result(client(Request()), 5.seconds)
      val Buf.Utf8(actual) = Await.result(Reader.readAll(response.reader), 5.seconds)
      assert(actual == "helloworld")
      Await.ready(client.close(), 5.seconds)
    }

    testIfImplemented(CompressedContent)(s"$implName (streaming)" + ": streaming clients can decompress content") {
      val svc = new Service[Request, Response] {
        def apply(request: Request) = {
          val response = Response()
          response.contentString = "raw content"
          Future.value(response)
        }
      }
      val client = connect(svc)
      val req = Request("/")
      req.headerMap.set("accept-encoding", "gzip")

      val content = Await.result(client(req).flatMap { rep => Reader.readAll(rep.reader) }, 5.seconds)
      assert(Buf.Utf8.unapply(content).get == "raw content")
      Await.ready(client.close(), 5.seconds)
    }

    test(s"$implName (streaming)" + ": symmetric reader and getContent") {
      val s = Service.mk[Request, Response] { req =>
        val buf = Await.result(Reader.readAll(req.reader), 5.seconds)
        assert(buf == Buf.Utf8("hello"))
        assert(req.contentString == "hello")

        req.response.content = req.content
        Future.value(req.response)
      }
      val req = Request()
      req.contentString = "hello"
      req.headerMap.put("Content-Length", "5")
      val client = connect(s)
      val res = Await.result(client(req), 5.seconds)

      val buf = Await.result(Reader.readAll(res.reader), 5.seconds)
      assert(buf == Buf.Utf8("hello"))
      assert(res.contentString == "hello")
    }

    testIfImplemented(Streaming)(s"$implName (streaming)" +
      ": transport closure propagates to request stream reader") {

      val p = new Promise[Buf]
      val s = Service.mk[Request, Response] { req =>
        p.become(Reader.readAll(req.reader))
        Future.value(Response())
      }
      val client = connect(s)
      val req = Request()
      req.setChunked(true)
      Await.result(client(req), 5.seconds)
      Await.ready(client.close(), 5.seconds)
      intercept[ChannelClosedException] { Await.result(p, 5.seconds) }
    }

    testIfImplemented(Streaming)(s"$implName (streaming)" + ": transport closure propagates to request stream producer") {
      val s = Service.mk[Request, Response] { _ => Future.value(Response()) }
      val client = connect(s)
      val req = Request()
      req.setChunked(true)
      client(req)
      Await.ready(client.close(), 5.seconds)
      intercept[Reader.ReaderDiscarded] { Await.result(drip(req.writer), 5.seconds) }
    }

    testIfImplemented(Streaming)(s"$implName (streaming)" +
      ": request discard terminates remote stream producer") {

      val s = Service.mk[Request, Response] { req =>
        val res = Response()
        res.setChunked(true)
        def go = for {
          Some(c) <- req.reader.read(Int.MaxValue)
          _  <- res.writer.write(c)
          _  <- res.close()
        } yield ()
        // discard the reader, which should terminate the drip.
        go ensure req.reader.discard()

        Future.value(res)
      }

      val client = connect(s)
      val req = Request()
      req.setChunked(true)
      val resf = client(req)

      Await.result(req.writer.write(buf("hello")), 5.seconds)

      val contentf = resf flatMap { res => Reader.readAll(res.reader) }
      assert(Await.result(contentf, 5.seconds) == Buf.Utf8("hello"))

      // drip should terminate because the request is discarded.
      intercept[Reader.ReaderDiscarded] { Await.result(drip(req.writer), 5.seconds) }
    }

    testIfImplemented(Streaming)(s"$implName (streaming)" +
      ": client discard terminates stream and frees up the connection") {

      val s = new Service[Request, Response] {
        var rep: Response = null

        def apply(req: Request) = {
          rep = Response()
          rep.setChunked(true)

          // Make sure the body is fully read.
          // Then we hang forever.
          val body = Reader.readAll(req.reader)

          Future.value(rep)
        }
      }

      val client = connect(s)
      val rep = Await.result(client(Request()), 10.seconds)
      assert(s.rep != null)
      rep.reader.discard()

      s.rep = null

      // Now, make sure the connection doesn't clog up.
      Await.result(client(Request()), 10.seconds)
      assert(s.rep != null)
    }

    testIfImplemented(StreamFixed)(s"$implName (streaming)" + ": two fixed-length requests") {
      val svc = Service.mk[Request, Response] { _ => Future.value(Response()) }
      val client = connect(svc)
      Await.result(client(Request()), 5.seconds)
      Await.result(client(Request()), 5.seconds)
      Await.ready(client.close(), 5.seconds)
    }

    test(s"$implName (streaming)" +": does not measure payload size") {
      val svc = Service.mk[Request, Response] { _ => Future.value(Response()) }
      val client = connect(svc)
      Await.result(client(Request()), 5.seconds)

      assert(statsRecv.stat("client", "request_payload_bytes")() == Nil)
      assert(statsRecv.stat("client", "response_payload_bytes")() == Nil)
      assert(statsRecv.stat("server", "request_payload_bytes")() == Nil)
      assert(statsRecv.stat("server", "response_payload_bytes")() == Nil)
      Await.ready(client.close(), 5.seconds)
    }

    test(s"$implName (streaming)" + ": with 'Connection: close'") {
      val service = new HttpService {
        def apply(request: Request) = Future.value(Response())
      }
      val client = connect(service)
      val request = Request()
      request.headerMap("Connection") = "close"
      val response = Await.result(client(request), 5.seconds)
      assert(response.status == Status.Ok)
      Await.ready(client.close(), 5.seconds)
    }
  }

  def tracing(connect: HttpService => HttpService) {
    test(implName + ": trace") {
      var (outerTrace, outerSpan) = ("", "")

      val inner = connect(new HttpService {
        def apply(request: Request) = {
          val response = Response(request)
          response.contentString = Seq(
            Trace.id.traceId.toString,
            Trace.id.spanId.toString,
            Trace.id.parentId.toString
          ).mkString(".")
          Future.value(response)
        }
      })

      val outer = connect(new HttpService {
        def apply(request: Request) = {
          outerTrace = Trace.id.traceId.toString
          outerSpan = Trace.id.spanId.toString
          inner(request)
        }
      })

      val response = Await.result(outer(Request()), 5.seconds)
      val Seq(innerTrace, innerSpan, innerParent) =
        response.contentString.split('.').toSeq
      assert(innerTrace == outerTrace, "traceId")
      assert(outerSpan == innerParent, "outer span vs inner parent")
      assert(innerSpan != outerSpan, "inner (%s) vs outer (%s) spanId".format(innerSpan, outerSpan))

      Await.ready(outer.close(), 5.seconds)
      Await.ready(inner.close(), 5.seconds)
    }
  }

  run(standardErrors, standardBehaviour, tracing) { service =>
    val ref = new ServiceFactoryRef(ServiceFactory.const(initService))
    val server = serverImpl()
      .withLabel("server")
      .configured(Stats(statsRecv))
      .withMaxRequestSize(100.bytes)
      .serve("localhost:*", ref)
    val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
    val client = clientImpl()
      .configured(Stats(statsRecv))
      .newService("%s:%d".format(addr.getHostName, addr.getPort), "client")

    val ret = new ServiceProxy(client) {
      override def close(deadline: Time) =
        Closable.all(client, server).close(deadline)
    }
    initClient(client)
    ref() = ServiceFactory.const(service)
    statsRecv.clear()
    ret
  }

  run(streaming) { service =>
    val ref = new ServiceFactoryRef(ServiceFactory.const(initService))
    val server = serverImpl()
      .withStreaming(true)
      .withLabel("server")
      .serve("localhost:*", ref)

    val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
    val client = clientImpl()
      .withStreaming(true)
      .configured(Stats(statsRecv))
      .newService("%s:%d".format(addr.getHostName, addr.getPort), "client")

    initClient(client)
    ref() = ServiceFactory.const(service)
    new ServiceProxy(client) {
      override def close(deadline: Time) =
        Closable.all(client, server).close(deadline)
    }
  }

  // use 1 less than the requeue limit so that we trigger failure accrual
  // before we run into the requeue limit.
  private val failureAccrualFailures = 19

  testIfImplemented(BasicFunctionality)(implName + ": Status.busy propagates along the Stack") {
    val st = new InMemoryStatsReceiver
    val failService = new HttpService {
      def apply(req: Request): Future[Response] =
        Future.exception(Failure.rejected("unhappy"))
    }

    val clientName = "http"
    val server = serverImpl().serve(new InetSocketAddress(0), failService)
    val client = clientImpl()
      .withStatsReceiver(st)
      .configured(FailureAccrualFactory.Param(failureAccrualFailures, () => 1.minute))
      .newService(Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])), clientName)

    val e = intercept[Exception](Await.result(client(Request("/")), 5.seconds))

    assert(st.counters(Seq(clientName, "failure_accrual", "removals")) == 1)
    assert(st.counters(Seq(clientName, "retries", "requeues")) == failureAccrualFailures - 1)
    assert(st.counters(Seq(clientName, "failures", "restartable")) == failureAccrualFailures)
    Await.ready(Closable.all(client, server).close(), 5.seconds)
  }

  testIfImplemented(ResponseClassifier)("Client-side ResponseClassifier based on status code") {
    val classifier = HttpResponseClassifier {
      case (_, r: Response) if r.status == Status.ServiceUnavailable =>
        ResponseClass.NonRetryableFailure
    }

    val server = serverImpl()
      .configured(param.Stats(NullStatsReceiver))
      .serve("localhost:*", statusCodeSvc)
    val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
    val client = clientImpl()
      .configured(param.Stats(statsRecv))
      .withResponseClassifier(classifier)
      .newService("%s:%d".format(addr.getHostName, addr.getPort), "client")

    val rep1 = Await.result(client(requestWith(Status.Ok)), 1.second)
    assert(statsRecv.counters(Seq("client", "requests")) == 1)
    assert(statsRecv.counters(Seq("client", "success")) == 1)

    val rep2 = Await.result(client(requestWith(Status.ServiceUnavailable)), 1.second)

    assert(statsRecv.counters(Seq("client", "requests")) == 2)
    assert(statsRecv.counters(Seq("client", "success")) == 1)

    Await.ready(client.close(), 5.seconds)
    Await.ready(server.close(), 5.seconds)
  }

  testIfImplemented(ResponseClassifier)("server-side ResponseClassifier based on status code") {
    val classifier = HttpResponseClassifier {
      case (_, r: Response) if r.status == Status.ServiceUnavailable =>
        ResponseClass.NonRetryableFailure
    }

    val server = serverImpl()
      .withResponseClassifier(classifier)
      .withStatsReceiver(statsRecv)
      .withLabel("server")
      .serve("localhost:*", statusCodeSvc)
    val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
    val client = clientImpl()
      .newService("%s:%d".format(addr.getHostName, addr.getPort), "client")

    Await.ready(client(requestWith(Status.Ok)), 1.second)
    assert(statsRecv.counters(Seq("server", "requests")) == 1)
    assert(statsRecv.counters(Seq("server", "success")) == 1)

    Await.ready(client(requestWith(Status.ServiceUnavailable)), 1.second)
    assert(statsRecv.counters(Seq("server", "requests")) == 2)
    assert(statsRecv.counters(Seq("server", "success")) == 1)
    assert(statsRecv.counters(Seq("server", "failures")) == 1)

    Await.ready(client.close(), 5.seconds)
    Await.ready(server.close(), 5.seconds)
  }

  test("codec should require a message size be less than 2Gb") {
    intercept[IllegalArgumentException](Http().maxRequestSize(2.gigabytes))
    intercept[IllegalArgumentException](Http(_maxResponseSize = 100.gigabytes))
    intercept[IllegalArgumentException] {
      serverImpl().withMaxRequestSize(2049.megabytes)
    }
    intercept[IllegalArgumentException] {
      clientImpl().withMaxResponseSize(3000.megabytes)
    }
  }

  testIfImplemented(HandlesExpect)("server handles expect continue header") {
    val expectP = new Promise[Boolean]

    val svc = new HttpService {
      def apply(request: Request) = {
        expectP.setValue(request.headerMap.contains("expect"))
        val response = Response()
        Future.value(response)
      }
    }
    val server = serverImpl()
      .withStatsReceiver(NullStatsReceiver)
      .withStreaming(true)
      .serve("localhost:*", svc)

    val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
    val client = clientImpl()
      .withStatsReceiver(statsRecv)
      .newService(s"${addr.getHostName}:${addr.getPort}", "client")

    val req = Request("/streaming")
    req.setChunked(false)
    req.headerMap.set("expect", "100-continue")

    val res = client(req)
    assert(Await.result(res, 2.seconds).status == Status.Continue)
    assert(Await.result(expectP, 2.seconds) == false)
    Await.ready(client.close(), 5.seconds)
    Await.ready(server.close(), 5.seconds)
  }

  testIfImplemented(CompressedContent)("non-streaming clients can decompress content") {
    val svc = new Service[Request, Response] {
      def apply(request: Request) = {
        val response = Response()
        response.contentString = "raw content"
        Future.value(response)
      }
    }
    val server = serverImpl()
      .withStatsReceiver(NullStatsReceiver)
      .withCompressionLevel(5)
      .serve("localhost:*", svc)

    val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
    val client = clientImpl()
      .withStatsReceiver(NullStatsReceiver)
      .newService(s"${addr.getHostName}:${addr.getPort}", "client")

    val req = Request("/")
    req.headerMap.set("accept-encoding", "gzip")
    assert(Await.result(client(req), 5.seconds).contentString == "raw content")
    Await.ready(client.close(), 5.seconds)
    Await.ready(server.close(), 5.seconds)
  }

  test("request remote address") {
    val svc = new Service[Request, Response] {
      def apply(request: Request) = {
        val response = Response()
        response.contentString = request.remoteAddress.toString
        Future.value(response)
      }
    }
    val server = serverImpl()
      .serve("localhost:*", svc)

    val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
    val client = clientImpl()
      .newService(s"${addr.getHostName}:${addr.getPort}", "client")

    assert(Await.result(client(Request("/")), 5.seconds).contentString.startsWith("/127.0.0.1"))
    Await.ready(client.close(), 5.seconds)
    Await.ready(server.close(), 5.seconds)
  }
}
