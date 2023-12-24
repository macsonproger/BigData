package twoPhazeCommit

import scala.util.Random

object MainClient {

  val sleepTime = 3000

  def main(args: Array[String]): Unit = {

    val Seq(hostPort, root) = args.toSeq
    val client = new Client(hostPort, root)
    client.execute()
  }
}
