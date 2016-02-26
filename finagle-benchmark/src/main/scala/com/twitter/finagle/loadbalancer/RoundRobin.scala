package com.twitter.finagle.loadbalancer

import com.twitter.finagle.{ClientConnection, NoBrokersAvailableException, Service, ServiceFactory}
import com.twitter.finagle.service.FailingFactory
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.util.OnReady
import com.twitter.util.{Activity, Future, Promise, Time}

object RoundRobinBalancer {
  def rr(): LoadBalancerFactory =
    new LoadBalancerFactory {
      def newBalancer[Req, Rep](
        endpoints: Activity[Set[ServiceFactory[Req, Rep]]],
        sr: StatsReceiver,
        exc: NoBrokersAvailableException
      ): ServiceFactory[Req, Rep] = {
        new RoundRobinBalancer(endpoints, sr, exc) {
          private[this] val gauge = sr.addGauge("rr")(1)
        }
      }
    }
}

/**
  * A  basic round robin load balancer.
  */
class RoundRobinBalancer[Req, Rep](
  protected val activity: Activity[Traversable[ServiceFactory[Req, Rep]]],
  protected val statsReceiver: StatsReceiver,
  protected val emptyException: NoBrokersAvailableException
) extends ServiceFactory[Req, Rep]
    with Balancer[Req, Rep]
    with Updating[Req, Rep] {

  // For the OnReady mixin
  private[this] val ready = new Promise[Unit]
  override def onReady: Future[Unit] = ready

  override def apply(conn: ClientConnection): Future[Service[Req,Rep]] = {
    dist.pick()(conn)
  }

  protected class Node(val factory: ServiceFactory[Req, Rep]) extends NodeT { self =>
    type This = Node
    // We don't care about these metrics for round robin
    override def load = 0.0
    override def pending = 0
    override def token = 0

    def close(deadline: Time): Future[Unit] = factory.close(deadline)
    override def apply(conn: ClientConnection): Future[Service[Req,Rep]] = factory(conn)
  }

  /**
    * A simple round robin distributor.
    */
  protected class Distributor(val vector: Vector[Node])
      extends DistributorT {
    type This = Distributor

    var currentNode = 0

    // For each node that's requested, we move the currentNode index
    // around the wheel using mod arithmetic. This is the round robin
    // of our balancer.
    override def pick(): Node = {
      val nodeToPick = synchronized {
        currentNode = (currentNode + 1) % vector.size
        currentNode
      }

      vector(nodeToPick)
    }

    override def needsRebuild: Boolean = false
    override def rebuild(): This = new Distributor(vector)
    override def rebuild(vector: Vector[Node]): This = new Distributor(vector)
  }

  protected def maxEffort: Int = Int.MaxValue
  protected def initDistributor(): Distributor = new Distributor(Vector.empty)
  protected def newNode(factory: ServiceFactory[Req,Rep], statsReceiver: StatsReceiver): Node = new Node(factory)
  protected def failingNode(cause: Throwable): Node = new Node(new FailingFactory(cause))
}
