package scala.collection.parallel



import sun.misc.Unsafe
import scala.reflect.macros._




package object workstealing {

  /* utilities */

  val unsafe = getUnsafe()

  def getUnsafe(): Unsafe = {
    if (this.getClass.getClassLoader == null) Unsafe.getUnsafe()
    try {
      val fld = classOf[Unsafe].getDeclaredField("theUnsafe")
      fld.setAccessible(true)
      return fld.get(this.getClass).asInstanceOf[Unsafe]
    } catch {
      case e: Throwable => throw new RuntimeException("Could not obtain access to sun.misc.Unsafe", e)
    }
  }

  class ResultCell[@specialized T] {
    private var r: T = _
    private var empty = true
    def result: T = r
    def result_=(v: T) = {
      r = v
      empty = false
    }
    def isEmpty = empty
  }

  final case class ProgressStatus(val start: Int, var progress: Int)

  object ResultFound extends WorkstealingTreeScheduler.TerminationCause {
    def validateResult[R](r: R) = if (r.isInstanceOf[Option[_]]) r else ???
  }

  /* macro utilities */

  implicit def Util(context: Context) = new Util[context.type](context)

  class Util[C <: Context](val c: C) {
  }

}








