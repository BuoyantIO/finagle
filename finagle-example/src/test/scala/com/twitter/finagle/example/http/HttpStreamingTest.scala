package com.twitter.finagle.example.http

import com.twitter.concurrent.AsyncStream
import com.twitter.conversions.time._
import com.twitter.finagle.http.{Method, Request, Response, Status}
import com.twitter.finagle.{Http, Service}
import com.twitter.io.{Buf, Reader}
import com.twitter.util.{Await, Future, JavaTimer}
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar
import scala.util.Random

class HttpStreamingTest extends FunSuite with MockitoSugar {

  test("does not hang when a client makes two streaming requests") {
    Http.server
      .withStreaming(enabled = true)
      .serve("0.0.0.0:8080", Service.mk[Request, Response] { req =>
        val writable = Reader.writable() // never gets closed
        Future.value(Response(req.version, Status.Ok, writable))
    })

    val client = Http.client.withStreaming(enabled = true).newService(s"/$$/inet/localhost/8080")

    val rsp0 = Await.result(client(Request()), 2.seconds)
    // rsp0 stream is never closed
    println("first request succeeded")

    val rsp1 = Await.result(client(Request()), 2.seconds)
    // hangs
    println("second request succeeded")
  }
}
