/**
 * This is a corrected and working version of the Producer/Consumer
 * in http://twitter.github.io/scala_school/concurrency.html.
 *
 * Each line in the input file has two comma separated fields.
 * First field has two single-space separated sub-fields.
 *
 * If the file does not conform (e.g. has no commas on a line),
 * then the process hangs.  When run with a single consumer thread,
 * and a file with 16 lines, the output is:
    Done: Producer
    t12 1 0       empty queue
    t12 2 15      queue.get returns first item
                  process hangs

 * Created by jraman
 */

import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}
import scala.io.Source
import scala.collection.mutable
import java.util.concurrent.Executors


case class User(name: String, id: Int)

object Main extends App {
  val filename = args(0)
  // Let's pretend we have 8 cores on this machine.
  val cores: Int = if (args.length > 1) args(1).toInt else 1

  val queue = new LinkedBlockingQueue[String]()

  // One thread for the producer
  val producer = new Producer(filename, queue)
  val tp = new Thread(producer)
  tp.start()

  val pool = Executors.newFixedThreadPool(cores)

  // Submit one consumer per core.
  val index = new InvertedIndex()
  for (i <- 0 until cores) {
    pool.submit(new IndexerConsumer(index, queue))
  }

  tp.join()
  while (!queue.isEmpty) {
    // print(queue.size + " ")
    Thread.sleep(500L)
  }

  println(s"Inverted index size: ${index.userMap.size}")
  index.userMap.foreach(println)

}


class InvertedIndex(val userMap: mutable.Map[String, User]) {

  def this() = this(new mutable.HashMap[String, User])

  def tokenizeName(name: String): Seq[String] = {
    name.split(" ").map(_.toLowerCase)
  }

  def add(term: String, user: User) {
    userMap += term -> user
  }

  def add(user: User) {
    tokenizeName(user.name).foreach { term =>
      add(term, user)
    }
  }
}


// Concrete producer
// The original example has this parameterized, but queue.put expects String
class Producer(path: String, queue: BlockingQueue[String]) extends Runnable {
  def run() {
    Source.fromFile(path, "utf-8").getLines.foreach { line =>
      queue.put(line)
    }
    println("Done: Producer")
  }
}


// Abstract consumer
abstract class Consumer[T](queue: BlockingQueue[T]) extends Runnable {
  def run() {
    val id = "t" + Thread.currentThread.getId
    while (true) {
      println(s"${id} 1 ${queue.size}")
      val item = queue.take()
      println(s"${id} 2 ${queue.size}")
      consume(item)
      println(s"${id} 3 ${queue.size}")
      Thread.sleep(20L)
    }
    println(s"${id}: Done: Consumer")
  }

  def consume(x: T)
}


trait UserMaker {
  def makeUser(line: String) = line.split(",") match {
    case Array(name, userid) => User(name, userid.trim().toInt)
    // what happens in case of a scala.MatchError (i.e. if the single case above is not satisfied)?
    // case _ => println(s"nomatch: ${line}"); User(line, 1)
  }
}


class IndexerConsumer(index: InvertedIndex, queue: BlockingQueue[String]) extends Consumer[String](queue) with UserMaker {
  def consume(t: String) = index.add(makeUser(t))
}
