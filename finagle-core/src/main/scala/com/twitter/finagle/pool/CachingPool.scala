package com.twitter.finagle.pool

import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.util.Cache
import com.twitter.finagle.{
  ClientConnection, Service, ServiceClosedException, ServiceFactory, ServiceProxy,
  Status}
import com.twitter.util.{Future, Time, Duration, Timer}
import scala.annotation.tailrec

/**
 * A pool that temporarily caches items from the underlying one, up to
 * the given timeout amount of time.
 *
 * @see The [[https://twitter.github.io/finagle/guide/Clients.html#caching-pool user guide]]
 *      for more details.
 */
private[finagle] class CachingPool[Req, Rep](
  factory: ServiceFactory[Req, Rep],
  cacheSize: Int,
  ttl: Duration,
  timer: Timer,
  statsReceiver: StatsReceiver = NullStatsReceiver)
  extends ServiceFactory[Req, Rep]
{ pool =>
  private[this] val cache =
    new Cache[Service[Req, Rep]](cacheSize, ttl, timer, Some(_.close()))
  @volatile private[this] var isOpen = true
  private[this] val sizeGauge =
    statsReceiver.addGauge("pool_cached") { cache.size }

  private[this] class WrappedService(underlying: Service[Req, Rep])
    extends ServiceProxy[Req, Rep](underlying)
  {
    override def close(deadline: Time) =
      (pool.isOpen, status) match {
        case (false, _) | (_, Status.Closed) =>
          underlying.close(deadline)
        case _ =>
          cache.put(underlying)
          Future.Done
      }
  }

  /**
   * Attempt to select a cached service.
   *
   * As services are retrieved, if a service is marked in the
   * [[Status.Closed]] state, it is closed and removed from the
   * cache. [[Status.Busy]] services are accumulated and returned to
   * the cache once an [[Status.Open]] service is obtained or  no
   */
  @tailrec
  private[this] def get(busy: Seq[Service[Req, Rep]] = Nil): Option[Service[Req, Rep]] =
    cache.get() match {
      case s@Some(service) =>
        service.status match {
          case Status.Open =>
            busy.foreach(cache.put(_))
            s

          case Status.Busy =>
            get(service +: busy)

          case Status.Closed =>
            service.close()
            get(busy)
        }

      case None =>
        busy.foreach(cache.put(_))
        None
    }

  def apply(conn: ClientConnection): Future[Service[Req, Rep]] = synchronized {
    if (!isOpen) Future.exception(new ServiceClosedException) else {
      get() match {
        case Some(service) =>
          Future.value(new WrappedService(service))
        case None =>
          factory(conn) map { new WrappedService(_) }
      }
    }
  }

  def close(deadline: Time) = synchronized {
    isOpen = false

    cache.evictAll()
    factory.close(deadline)
  }

  override def status =
    if (isOpen) factory.status
    else Status.Closed

  override val toString = "caching_pool_%s".format(factory.toString)
}
