package com.twitter.finagle.http.codec

import com.twitter.finagle.Status
import com.twitter.finagle.http.{Message, Request, Response}
import com.twitter.util.Future
/**
 * The HTTP connection manager implements connection management in
 * accordance with RFC 2616 § 8. This is just the state machine; the
 * codec implementations are in {Server,Client}ConnectionManager.
 */
private[finagle] class ConnectionManager {

  private[this] var isKeepAlive = true
  private[this] var activeRequests = 0
  private[this] var activeStreams = 0

  def status: Status =
    synchronized((isKeepAlive, activeRequests, activeStreams)) match {
      case (true, 0, 0) => Status.Open
      case (false, 0, 0) => Status.Closed
      case _ => Status.Busy
    }

  def observeMessage(message: Message, onFinish: Future[Unit]): Unit = synchronized {
    message match {
      case req: Request => observeRequest(req, onFinish)
      case rep: Response => observeResponse(rep, onFinish)
      case _ => isKeepAlive = false  // conservative
    }
  }

  def observeRequest(request: Request, onFinish: Future[Unit]): Unit = synchronized {
    activeRequests += 1
    isKeepAlive = request.isKeepAlive
    handleIfStream(onFinish)
  }

  def observeResponse(response: Response, onFinish: Future[Unit]): Unit = synchronized {
    activeRequests -= 1
    if (noLength(response) || !response.isKeepAlive) isKeepAlive = false
    handleIfStream(onFinish)
  }

  private def noLength(msg: Message) =
    !msg.isChunked && msg.contentLength.isEmpty


  // this can be unsynchronized because all callers are synchronized.
  private[this] def handleIfStream(onFinish: Future[Unit]): Unit =
    if (!onFinish.isDefined) {
      activeStreams += 1
      onFinish.ensure {
        endStream()
      }
    }

  private[this] def endStream(): Unit = synchronized {
    activeStreams -= 1
  }
}
