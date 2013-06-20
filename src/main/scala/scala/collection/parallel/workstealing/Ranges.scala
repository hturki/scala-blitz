package scala.collection.parallel
package workstealing



import scala.language.experimental.macros
import scala.reflect.macros._
import scala.collection.parallel.generic._
import scala.reflect.ClassTag



object Ranges {

  trait Scope {
    implicit def rangeOps(r: Par[Range]) = new Ranges.Ops(r)
    implicit def canMergeRange(implicit ctx: WorkstealingTreeScheduler): CanMergeFrom[Range, Int, Par[Array[Int]]] = new CanMergeFrom[Range, Int, Par[Array[Int]]] {
      def apply(from: Range) = new Arrays.ArrayMerger[Int](ctx)
      def apply() = new Arrays.ArrayMerger[Int](ctx)
    }
    implicit def canMergeParRange(implicit ctx: WorkstealingTreeScheduler): CanMergeFrom[Par[Range], Int, Par[Array[Int]]] = new CanMergeFrom[Par[Range], Int, Par[Array[Int]]] {
      def apply(from: Par[Range]) = new Arrays.ArrayMerger[Int](ctx)
      def apply() = new Arrays.ArrayMerger[Int](ctx)
    }
    implicit def rangeIsZippable = new IsZippable[Range, Int] {
      def apply(pr: Par[Range]) = new Zippable[Int]{
      def iterator = ???
      def splitter =  ???
      def stealer = new RangeStealer(pr.seq, 0, pr.seq.length)
      }
    }
  }

  class Ops(val range: Par[collection.immutable.Range]) extends AnyVal with Zippables.OpsLike[Int, Par[collection.immutable.Range]] {
    def r = range.seq
    def stealer: PreciseStealer[Int] = new RangeStealer(r, 0, r.length)
    override def reduce[U >: Int](operator: (U, U) => U)(implicit ctx: WorkstealingTreeScheduler): U = macro methods.RangesMacros.reduce[U]
    override def mapReduce[R](mapper: Int => R)(reducer: (R, R) => R)(implicit ctx: WorkstealingTreeScheduler): R = macro methods.RangesMacros.mapReduce[Int,R]
    override def count[U >: Int](p: U => Boolean)(implicit ctx: WorkstealingTreeScheduler): Int = macro methods.RangesMacros.count[U]
    override def fold[U >: Int](z: => U)(op: (U, U) => U)(implicit ctx: WorkstealingTreeScheduler): U = macro methods.RangesMacros.fold[U]
    override def aggregate[S](z: S)(combop: (S, S) => S)(seqop: (S, Int) => S)(implicit ctx: WorkstealingTreeScheduler): S = macro methods.RangesMacros.aggregate[S]
    override def sum[U >: Int](implicit num: Numeric[U], ctx: WorkstealingTreeScheduler): U = macro methods.RangesMacros.sum[U]
    override def product[U >: Int](implicit num: Numeric[U], ctx: WorkstealingTreeScheduler): U = macro methods.RangesMacros.product[U]
    override def min[U >: Int](implicit ord: Ordering[U], ctx: WorkstealingTreeScheduler): Int = macro methods.RangesMacros.min[U]
    override def foreach[U >: Int](action: U => Unit)(implicit ctx: WorkstealingTreeScheduler): Unit = macro methods.RangesMacros.foreach[U]
    override def max[U >: Int](implicit ord: Ordering[U], ctx: WorkstealingTreeScheduler): Int = macro methods.RangesMacros.max[U]
    override def find[U >: Int](p: U=> Boolean)(implicit ctx: WorkstealingTreeScheduler): Option[Int] = macro methods.RangesMacros.find[U]
    override def exists[U >: Int](p: U=> Boolean)(implicit ctx: WorkstealingTreeScheduler): Boolean = macro methods.RangesMacros.exists[U]
    override def forall[U >: Int](p: U=> Boolean)(implicit ctx: WorkstealingTreeScheduler): Boolean = macro methods.RangesMacros.forall[U]
    override def map[S, That](func: Int => S)(implicit cmf: CanMergeFrom[Par[Range], S, That], ctx: WorkstealingTreeScheduler) = macro methods.RangesMacros.map[Int, S, That]
    override def copyToArray[U >: Int](arr: Array[U], start: Int, len: Int)(implicit ctx:WorkstealingTreeScheduler): Unit = macro methods.RangesMacros.copyToArray[U]
    def copyToArray[U >: Int](arr: Array[U], start: Int)(implicit ctx: WorkstealingTreeScheduler): Unit = macro methods.RangesMacros.copyToArray2[U]
    def copyToArray[U >: Int](arr: Array[U])(implicit ctx: WorkstealingTreeScheduler): Unit = macro methods.RangesMacros.copyToArray3[U]
    override def flatMap[S, That](func: Int => TraversableOnce[S])(implicit cmf: CanMergeFrom[Par[Range], S, That], ctx: WorkstealingTreeScheduler) = macro methods.RangesMacros.flatMap[Int, S, That]
    def filter(pred: Int => Boolean)(implicit ctx: WorkstealingTreeScheduler) = macro methods.RangesMacros.filter
    def seq = range
    def classTag = implicitly[ClassTag[Int]]
  }

  /* stealer implementation */

  import WorkstealingTreeScheduler.{ Kernel, Node }

  class RangeStealer(val range: collection.immutable.Range, start: Int, end: Int) extends IndexedStealer.Flat[Int](start, end) {
    type StealerType = RangeStealer

    var padding8: Int = _
    var padding9: Int = _
    var padding10: Int = _
    var padding11: Int = _
    var padding12: Int = _
    var padding13: Int = _
    var padding14: Int = _
    var padding15: Int = _

    def next(): Int = {
      val idx = nextProgress
      nextProgress += 1
      range.apply(idx)
    }

    def newStealer(s: Int, u: Int) = new RangeStealer(range, s, u)
    override def toString = {
    val p = READ_PROGRESS
    val dp = decode(p)
    "RangeStealer(id:%d, startIndex:%d, progress:%d, decodedProgress:%d, UntilIndex:%d, nextProgress:%d, nextUntil:%d)".format(java.lang.System.identityHashCode(this), startIndex, p, dp, untilIndex, nextProgress, nextUntil) 
  }
  }

  abstract class RangeKernel[@specialized R] extends IndexedStealer.IndexedKernel[Int, R] {
    def apply(node: Node[Int, R], chunkSize: Int): R = {
      val stealer = node.stealer.asInstanceOf[RangeStealer]
      val nextProgress = stealer.nextProgress
      val nextUntil = stealer.nextUntil
      val range = stealer.range
      val from = range.apply(nextProgress)

      if (nextProgress == nextUntil) apply0(node, from)
      else {
        val to = range.apply(nextUntil - 1)
        val step = range.step

        if (step == 1) apply1(node, from, to)
        else applyN(node, from, to, step)
      }
    }
    def apply0(node: Node[Int, R], at: Int): R
    def apply1(node: Node[Int, R], from: Int, to: Int): R
    def applyN(node: Node[Int, R], from: Int, to: Int, stride: Int): R
  }

  abstract class CopyMapRangeKernel[@specialized S] extends IndexedStealer.IndexedKernel[Int, Unit] {
    import scala.collection.parallel.workstealing.WorkstealingTreeScheduler.{ Ref, Node }
    def zero: Unit = ()
    def combine(a: Unit, b: Unit) = a
    def resultArray: Array[S]
    def apply(node: Node[Int, Unit], chunkSize: Int): Unit = {
      val stealer = node.stealer.asInstanceOf[RangeStealer]
      val nextProgress = stealer.nextProgress
      val nextUntil = stealer.nextUntil
      val range = stealer.range
      val from = range.apply(nextProgress)

      if (nextProgress == nextUntil) applyN(node, from, from, 1)
      else {
        val to = range.apply(nextUntil - 1)
        val step = range.step

        if (step == 1) applyN(node, from, to, 1)
        else if (step == -1) applyN(node, from, to, -1)
        else applyN(node, from, to, step)
      }
    }
    @inline def applyN(node: Node[Int, Unit], from: Int, to: Int, stride: Int): Unit
  }

  val EMPTY_RESULT = new AnyRef

  def newMerger(pa: Par[Range])(implicit ctx: WorkstealingTreeScheduler): Arrays.ArrayMerger[Int] = {
    val am = pa.seq match {
      case x: Range => new Arrays.ArrayMerger[Int](ctx)
      case null => throw new NullPointerException
    }
    am
  }

}

