package barneshut

import java.io.File
import java.io.PrintWriter
import scala.collection.par._

object DataGenerator {
  self =>

  def gee = 100.0f

  def init2Galaxies(totalBodies: Int, outputFile: String) {
    val random = new scala.util.Random(213L)
    val writer = new PrintWriter(new File(outputFile))

    def galaxy(from: Int, num: Int, maxradius: Float, cx: Float, cy: Float, sx: Float, sy: Float) {
      val totalM = 1.5f * num
      val blackHoleM = 1.0f * num
      val cubmaxradius = maxradius * maxradius * maxradius
      for (i <- from until (from + num)) {
        val b = if (i == from) {
          writer.write(s"$i,$cx,$cy,$sx,$sy,$blackHoleM\n")
        } else {
          val angle = random.nextFloat * 2 * math.Pi
          val radius = 25 + maxradius * random.nextFloat
          val starx = cx + radius * math.sin(angle).toFloat
          val stary = cy + radius * math.cos(angle).toFloat
          val speed = math.sqrt(gee * blackHoleM / radius + gee * totalM * radius * radius / cubmaxradius)
          val starspeedx = sx + (speed * math.sin(angle + math.Pi / 2)).toFloat
          val starspeedy = sy + (speed * math.cos(angle + math.Pi / 2)).toFloat
          val starmass = 1.0f + 1.0f * random.nextFloat
          writer.write(s"$i,$starx,$stary,$starspeedx,$starspeedy,$starmass\n")
        }
      }
    }

    galaxy(0, totalBodies / 8, 300.0f, 0.0f, 0.0f, 0.0f, 0.0f)
    galaxy(totalBodies / 8, totalBodies / 8 * 7, 350.0f, -1800.0f, -1200.0f, 0.0f, 0.0f)
    writer.close()
  }

  def main(args: Array[String]) {
    init2Galaxies(args(0).toInt, args(1))
  }

}




