package twoPhazeCommit

import org.apache.zookeeper.Watcher.Event.EventType
import org.apache.zookeeper.data.Stat
import org.apache.zookeeper.{CreateMode, WatchedEvent, Watcher, ZooDefs, ZooKeeper}

import scala.util.Random

case class Client(hostPort: String, root: String) extends Watcher {

  val zk = new ZooKeeper(hostPort, 3000, this)
  val mutex = new Object()
  val nodePath: String = root + "/" + Random.nextInt(20).toString
  var decision: String = null

  if (zk == null) throw new Exception("ZK is NULL.")

  override def process(event: WatchedEvent): Unit = {
    mutex.synchronized {
      println(s"Event from keeper: ${event.getType}")
      if (event.getType != EventType.None) {
        val data = zk.getData(nodePath, this, new Stat())
        decision = new String(data)
        mutex.notify()
      }
    }
  }

  def execute(): Unit = {
    println("My node's path is " + nodePath)
    decision = if (Random.nextInt(20) > 9) "COMMIT" else "ROLLBACK"
    println("my decision is " + decision)
    zk.create(nodePath, decision.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
    zk.getData(nodePath, this, new Stat())

    mutex.synchronized {
      println("waiting for coordinator")

      mutex.wait()
      println("perform " + decision)
      zk.delete(nodePath, -1)
    }
  }

}
