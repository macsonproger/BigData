package twoPhazeCommit


import org.apache.zookeeper.Watcher.Event.EventType
import org.apache.zookeeper._
import org.apache.zookeeper.data.Stat


case class Coordinator(hostPort: String, root: String, clientCount: Int) extends Watcher {

  val zk = new ZooKeeper(hostPort, 3000, this)
  val mutex = new Object()
  var decision: String = "COMMIT"

  if (zk == null) throw new Exception("ZK is NULL.")

  override def process(event: WatchedEvent): Unit = {
    mutex.synchronized {
      println(s"Event from keeper: ${event.getType}")
      if (event.getType == EventType.NodeChildrenChanged) {
        mutex.notify()
      }
    }

  }

  def execute(): Unit = {

    println("Waiting for clients")
    mutex.synchronized {
      var clients = zk.getChildren(root, this)
      while (clients.size() < clientCount) {
        mutex.wait()
        println("Client connected")
        clients = zk.getChildren(root, this)
      }
    }

    println("Reading clients")
    val clients = zk.getChildren(root, true)

    clients.forEach(client => {
      val clientDecision = new String(zk.getData(root + "/" + client, false, new Stat()))
      println("Client " + client + " decided to " + clientDecision)
      if (clientDecision.equals("ROLLBACK") || decision.equals("ROLLBACK")) {
        decision = "ROLLBACK"
      }
    })

    println("Asking clients to " + decision)

    clients.forEach(client => {
      zk.setData(root +"/"+ client, decision.getBytes(), 0)
    })
    println("Finished coordination")

  }

}