package com.twitter.finagle.http

import com.twitter.finagle.{Status => CoreStatus}
import com.twitter.finagle.http.codec.ConnectionManager
import com.twitter.finagle.http.exp.{Multi, StreamTransportProxy, StreamTransport}
import com.twitter.util.{Future, Promise}
import scala.util.control.NonFatal

/**
 * A Transport with close behavior managed by ConnectionManager.
 *
 * @note the connection manager will close connections as required by RFC 2616 § 8
 *       irrespective of any pending requests in the dispatcher.
 */
private[finagle] class HttpTransport[A <: Message, B <: Message](
    self: StreamTransport[A, B],
    manager: ConnectionManager)
  extends StreamTransportProxy[A, B](self) {

  def this(self: StreamTransport[A, B]) = this(self, new ConnectionManager)

  def read(): Future[Multi[B]] =
    self.read().onSuccess(readFn)

  private[this] val readFn: Multi[B] => Unit = { case Multi(m, onFinish) =>
    manager.observeMessage(m, onFinish)
    if (manager.status == CoreStatus.Closed)
      self.close()
  }

  def write(m: A): Future[Unit] =
    try {
      val p = Promise[Unit]
      manager.observeMessage(m, p)
      val f = self.write(m)
      p.become(f)
      if (manager.status == CoreStatus.Closed)
        f.before(self.close())
      else f
    } catch {
      case NonFatal(e) => Future.exception(e)
    }

  override def status: CoreStatus =
    CoreStatus.worst(manager.status, self.status)
}
