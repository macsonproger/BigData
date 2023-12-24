package twoPhazeCommit


object MainCoordinator {

  val sleepTime = 3000

  def main(args: Array[String]): Unit = {

    val Seq(hostPort, root, clientCount) = args.toSeq
    val coordinator = new Coordinator(hostPort, root, clientCount.toInt)
    coordinator.execute()
  }
}
