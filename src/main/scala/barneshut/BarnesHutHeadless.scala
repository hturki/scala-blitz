package barneshut

import java.io.{File, PrintWriter}

import scala.collection.mutable.ArrayBuffer
import scala.collection.par._
import scala.collection.parallel._
import scala.io.Source
import scala.reflect.ClassTag

object BarnesHutHeadless {
  self =>

  var bodies: Array[Quad.Body] = _
  var scheduler: Scheduler = _
  var tasksupport: TaskSupport = _
  var initialBoundaries: Boundaries = _
  var boundaries: Boundaries = _
  var sectors: Sectors[Conc.Buffer[Quad.Body]] = _
  var buckets: Array[Conc[Quad.Body]] = _
  var quadtree: Quad = _

  sealed trait Quad {
    def massX: Float
    def massY: Float
    def mass: Float
    def total: Int

    def update(fromx: Float, fromy: Float, sz: Float, b: Quad.Body, depth: Int = 0): Quad

    def distance(fromx: Float, fromy: Float): Float = {
      math.sqrt((fromx - massX) * (fromx - massX) + (fromy - massY) * (fromy - massY)).toFloat
    }

    def force(m: Float, dist: Float) = gee * m * mass / (dist * dist)
  }

  object Quad {
    case class Body(val id: Int)(val x: Float, val y: Float, val xspeed: Float, val yspeed: Float, val mass: Float, var index: Int)
    extends Quad {

      def massX = x
      def massY = y
      def total = 1

      def update(fromx: Float, fromy: Float, sz: Float, b: Body, depth: Int) = {
        assert(depth < 100, s"$fromx, $fromy, $sz; this: ${this.x}, ${this.y}, that: ${b.x}, ${b.y}")
        val cx = fromx + sz / 2
        val cy = fromy + sz / 2
        if (sz > 0.00001f) {
          val fork = new Fork(cx, cy, sz)(Empty, Empty, Empty, Empty)
          fork.update(fromx, fromy, sz, this, depth).update(fromx, fromy, sz, b, depth)
        } else {
          val bunch = new Bunch(cx, cy, sz)
          bunch.update(fromx, fromy, sz, this, depth).update(fromx, fromy, sz, b, depth)
        }
      }

      def updatePosition(quad: Quad) {
        var netforcex = 0.0f
        var netforcey = 0.0f

        def traverse(quad: Quad): Unit = (quad: Quad) match {
          case Empty =>
            // no force
          case _ =>
            // see if node is far enough, or recursion is needed
            val dist = quad.distance(x, y)
            if (dist > 1.0f) quad match {
              case f @ Fork(cx, cy, sz) if f.size / dist >= theta =>
                traverse(f.nw)
                traverse(f.sw)
                traverse(f.ne)
                traverse(f.se)
              case Body(thatid) if id == thatid =>
                // skip self
              case _ =>
                val dforce = quad.force(mass, dist)
                val xn = (quad.massX - x) / dist
                val yn = (quad.massY - y) / dist
                val dforcex = dforce * xn
                val dforcey = dforce * yn
                netforcex += dforcex
                netforcey += dforcey
                assert(!netforcey.isNaN, (x, y, quad.massX, quad.massY, quad, dist, dforce, xn, yn))
            }
        }

        traverse(quad)

        val nx = x + xspeed * delta
        val ny = y + yspeed * delta
        val nxspeed = xspeed + netforcex / mass * delta
        val nyspeed = yspeed + netforcey / mass * delta

        bodies(index) = new Quad.Body(id)(nx, ny, nxspeed, nyspeed, mass, index)

        //assert(netforcex < 1000, (netforcex, netforcey, this))
        //assert(netforcey < 1000, (netforcex, netforcey, this))

        //if (id == 0) println(s"pos: $x, $y, force: $netforcex, $netforcey, speed: $xspeed, $yspeed")
      }

      override def toString = s"Body($id; pos: $x, $y; speed: $xspeed, $yspeed; mass: $mass)"
    }

    case object Empty extends Quad {
      def massX = 0.0f
      def massY = 0.0f
      def mass = 0.0f
      def total = 0

      def update(fromx: Float, fromy: Float, sz: Float, b: Body, depth: Int) = b
    }

    case class Fork(val centerX: Float, val centerY: Float, val size: Float)(var nw: Quad, var ne: Quad, var sw: Quad, var se: Quad)
    extends Quad {
      var massX: Float = _
      var massY: Float = _
      var mass: Float = _

      def total = nw.total + ne.total + sw.total + se.total

      def update(fromx: Float, fromy: Float, sz: Float, b: Body, depth: Int) = {
        if (depth > 95) println(depth, fromx, fromy, centerX, centerY, b.x, b.y)
        val hsz = sz / 2
        if (b.x <= centerX) {
          if (b.y <= centerY) nw = nw.update(fromx, fromy, hsz, b, depth + 1)
          else sw = sw.update(fromx, centerY, hsz, b, depth + 1)
        } else {
          if (b.y <= centerY) ne = ne.update(centerX, fromy, hsz, b, depth + 1)
          else se = se.update(centerX, centerY, hsz, b, depth + 1)
        }

        updateStats()

        this
      }

      def updateStats() {
        mass = nw.mass + sw.mass + ne.mass + se.mass
        if (mass > 0.0f) {
          massX = (nw.mass * nw.massX + sw.mass * sw.massX + ne.mass * ne.massX + se.mass * se.massX) / mass
          massY = (nw.mass * nw.massY + sw.mass * sw.massY + ne.mass * ne.massY + se.mass * se.massY) / mass
        } else {
          massX = 0.0f
          massY = 0.0f
        }
      }
    }

    case class Bunch(centerX: Float, centerY: Float, size: Float) extends Quad {
      var massX: Float = _
      var massY: Float = _
      var mass: Float = _
      val bodies = ArrayBuffer[Body]()

      def total = bodies.size

      def update(fromx: Float, fromy: Float, sz: Float, b: Body, depth: Int) = {
        bodies += b
        updateStats()
        this
      }

      def updateStats() {
        mass = 0
        massX = 0
        massY = 0
        for (b <- bodies) {
          mass += b.mass
          massX += b.mass * b.massX
          massY += b.mass * b.massY
        }
        massX /= mass
        massY /= mass
      }

      override def toString = s"Quad.Bunch($centerX, $centerY, $size, ${bodies.mkString(", ")})"
    }

  }

  class Boundaries extends Accumulator[Quad.Body, Boundaries] {
    var minX = Float.MaxValue
    var minY = Float.MaxValue
    var maxX = Float.MinValue
    var maxY = Float.MinValue

    def width = maxX - minX

    def height = maxY - minY

    def size = math.max(width, height)

    def centerX = minX + width / 2

    def centerY = minY + height / 2

    def merge(that: Boundaries) = if (this eq that) this else {
      val res = new Boundaries
      res.minX = math.min(this.minX, that.minX)
      res.minY = math.min(this.minY, that.minY)
      res.maxX = math.max(this.maxX, that.maxX)
      res.maxY = math.max(this.maxY, that.maxY)
      res
    }

    def +=(b: Quad.Body) = {
      minX = math.min(b.x, minX)
      minY = math.min(b.y, minY)
      maxX = math.max(b.x, maxX)
      maxY = math.max(b.y, maxY)
      this
    }

    def clear() {
      minX = Float.MaxValue
      minY = Float.MaxValue
      maxX = Float.MinValue
      maxY = Float.MinValue
    }

    override def toString = s"Boundaries($minX, $minY, $maxX, $maxY)"
  }

  object Sectors {
    def apply[T <: AnyRef: ClassTag](b: Boundaries)(init: () => T)(comb: (T, T) => T)(op: (T, Quad.Body) => Unit) = {
      val s = new Sectors(b)(comb)(op)
      for (x <- 0 until sectorPrecision; y <- 0 until sectorPrecision) s.matrix(y * sectorPrecision + x) = init()
      s
    }
  }

  class Sectors[T <: AnyRef: ClassTag](val boundaries: Boundaries)(val comb: (T, T) => T)(val op: (T, Quad.Body) => Unit)
  extends Accumulator[Quad.Body, Sectors[T]] {
    var matrix = new Array[T](sectorPrecision * sectorPrecision)
    val sectorSize = boundaries.size / sectorPrecision

    def merge(that: Sectors[T]) = {
      val res = new Sectors(boundaries)(comb)(op)
      for (x <- 0 until sectorPrecision; y <- 0 until sectorPrecision) {
        val sid = y * sectorPrecision + x
        res.matrix(sid) = comb(this.matrix(sid), that.matrix(sid))
      }
      res
    }

    def +=(b: Quad.Body) = {
      val sx = math.min(sectorPrecision - 1, ((b.x - boundaries.minX) / sectorSize).toInt)
      val sy = math.min(sectorPrecision - 1, ((b.y - boundaries.minY) / sectorSize).toInt)
      val accum = matrix(sy * sectorPrecision + sx)
      op(accum, b)
      this
    }

    def clear() {
      matrix = new Array(sectorPrecision * sectorPrecision)
    }

    def apply(x: Int, y: Int) = matrix(y * sectorPrecision + x)

    override def toString = s"Sectors(${matrix.mkString(", ")})"
  }

  def toQuad(sectors: Sectors[Conc.Buffer[Quad.Body]])(implicit ctx: Scheduler): Quad = {
    buckets = sectors.matrix.map(_.result)
    val indexedBuckets: Array[(Conc[Quad.Body], Int)] = buckets.zipWithIndex
    val quads: Array[Quad] = if (useWsTree) {
      indexedBuckets.toPar.map(bi => sectorToQuad(sectors.boundaries, bi._1, bi._2)).seq
    } else {
      val pibs = indexedBuckets.par
      pibs.tasksupport = tasksupport
      pibs.map(bi => sectorToQuad(sectors.boundaries, bi._1, bi._2)).seq.toArray
    }

    // bind into a quad tree
    var level = sectorPrecision
    while (level > 1) {
      val nextlevel = level / 2
      for (qy <- 0 until nextlevel; qx <- 0 until nextlevel) {
        val rx = qx * 2
        val ry = qy * 2
        val nw = quads((ry + 0) * level + (rx + 0))
        val ne = quads((ry + 0) * level + (rx + 1))
        val sw = quads((ry + 1) * level + (rx + 0))
        val se = quads((ry + 1) * level + (rx + 1))
        val size = boundaries.size / nextlevel
        val centerX = boundaries.minX + size * (qx + 0.5f)
        val centerY = boundaries.minY + size * (qy + 0.5f)
        val fork = new Quad.Fork(centerX, centerY, size)(nw, ne, sw, se)
        fork.updateStats()
        quads(qy * nextlevel + qx) = fork
      }
      level = nextlevel
    }

    quads(0)
  }

  def sectorToQuad(boundaries: Boundaries, bs: Conc[Quad.Body], sid: Int): Quad = {
    val sx = sid % sectorPrecision
    val sy = sid / sectorPrecision
    val fromX = boundaries.minX + sx * sectors.sectorSize
    val fromY = boundaries.minY + sy * sectors.sectorSize
    var quad: Quad = Quad.Empty

    for (b <- bs) {
      quad = quad.update(fromX, fromY, sectors.sectorSize, b)
    }

    quad
  }

  var useWsTree = true

  def sectorPrecision = 16

  def delta = 0.1f

  def theta = 0.5f

  def eliminationThreshold = 8.0f

  def eliminationQuantity = 4

  def gee = 100.0f

  def init(inputFile: String) {
    initScheduler()
    initBodies(inputFile)
  }

  def initBodies(inputFile: String) {
    val bufferedSource = Source.fromFile(inputFile)
    bodies = bufferedSource.getLines().map(line => {
      val split = line.split(",")
      val i = split(0).toInt
      val x = split(1).toFloat
      val y = split(2).toFloat
      val xspeed = split(3).toFloat
      val yspeed = split(4).toFloat
      val mass = split(5).toFloat
      new Quad.Body(i)(x, y, xspeed, yspeed, mass, i)
    }).toArray

    bufferedSource.close

    // compute center and boundaries
    initialBoundaries = bodies.toPar.accumulate(new Boundaries)(scheduler)
    boundaries = initialBoundaries
  }

  def initScheduler() {
    val p = Runtime.getRuntime.availableProcessors
    val conf = new Scheduler.Config.Default(p)
    scheduler = new Scheduler.ForkJoin(conf)
    tasksupport = new collection.parallel.ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(p))
    println(s"parallelism level: $p")
  }

  val timeMap = collection.mutable.Map[String, (Double, Int)]()

  def timed(title: String)(body: =>Any): Any = {
    val startTime = System.nanoTime
    val res = body
    val endTime = System.nanoTime
    val totalTime = (endTime - startTime) / 1000000.0

    timeMap.get(title) match {
      case Some((total, num)) => timeMap(title) = (total + totalTime, num + 1)
      case None => timeMap(title) = (0.0, 0)
    }

    println(s"$title: ${totalTime} ms; avg: ${timeMap(title)._1 / timeMap(title)._2}")
    res
  }

  def updateInfo() {
    val text = timeMap map {
      case (k, (total, num)) => k + ": " + (total / num * 100).toInt / 100.0 + " ms"
    } mkString("\n")
  }

  def step()(implicit s: Scheduler): Unit = self.synchronized {
    def constructTree() {
      // construct sectors
      if (useWsTree) {
        sectors = bodies.toPar.accumulate(Sectors(boundaries)(() => new Conc.Buffer[Quad.Body])(_ merge _)({
          _ += _
        }))
      } else {
        sectors = bodies.aggregate(Sectors(boundaries)(() => new Conc.Buffer[Quad.Body])(_ merge _)({
          _ += _
        }))(_ += _, _ merge _)
      }

      // construct a quad tree for each sector
      quadtree = toQuad(sectors)
    }

    def updatePositions() {
      if (useWsTree) {
        for (buck <- buckets.toPar; b <- buck) b.updatePosition(quadtree)
      } else {
        val pbs = buckets.par
        pbs.tasksupport = tasksupport
        for (buck <- pbs; b <- buck) b.updatePosition(quadtree)
      }

      // recompute center and boundaries
      if (useWsTree) {
        boundaries = bodies.toPar.accumulate(new Boundaries)
      } else {
        boundaries = bodies.aggregate(new Boundaries)(_ += _, _ merge _)
      }
    }

    def eliminateOutliers() {
      val outliers = collection.mutable.LinkedHashSet[Quad.Body]()

      def checkOutlier(x: Int, y: Int) {
        val sector = buckets(y * sectorPrecision + x)
        if (sector.size < eliminationQuantity) for (b <- sector) {
          val dx = quadtree.massX - b.x
          val dy = quadtree.massY - b.y
          val d = math.sqrt(dx * dx + dy * dy)
          if (d > eliminationThreshold * sectors.sectorSize) {
            val nx = dx / d
            val ny = dy / d
            val relativeSpeed = b.xspeed * nx + b.yspeed * ny
            if (relativeSpeed < 0) {
              val escapeSpeed = math.sqrt(2 * gee * quadtree.mass / d)
              if (-relativeSpeed > 2 * escapeSpeed) outliers += b
            }
          }
        }
      }

      for (x <- 0 until sectorPrecision) {
        checkOutlier(x, 0)
        checkOutlier(x, sectorPrecision - 1)
      }
      for (y <- 1 until sectorPrecision - 1) {
        checkOutlier(0, y)
        checkOutlier(sectorPrecision - 1, y)
      }

      if (outliers.nonEmpty) {
        bodies = bodies.filterNot(b => outliers contains b)
        for (i <- 0 until bodies.length) bodies(i).index = i
      }
    }

    timed(s"quadtree construction($useWsTree)") {
      constructTree()
    }
    timed(s"position update($useWsTree)") {
      updatePositions()
    }
    timed(s"elimination($useWsTree)") {
      eliminateOutliers()
    }
    println("bodies remaining: " + bodies.length)
    updateInfo()
  }

  def main(args: Array[String]) {
    init(args(0))

    if (args.length == 3) {
      val writer = new PrintWriter(new File(args(2)).toPath.resolve("0.csv").toFile)
      bodies.foreach(b => writer.write(s"${b.index},${b.x},${b.y},${b.xspeed},${b.yspeed},${b.mass}\n"))
      writer.close()
    }

    for (i <- 0 until args(1).toInt) {
      step()(scheduler)
      if (args.length == 3) {
        val writer = new PrintWriter(new File(args(2)).toPath.resolve(s"${i + 1}.csv").toFile)
        bodies.foreach(b => writer.write(s"${b.index},${b.x},${b.y},${b.xspeed},${b.yspeed}\n"))
        writer.close()
      }
    }
  }

}




