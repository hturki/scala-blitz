package barneshut

import java.io.{File, PrintWriter}

import scala.io.Source

object BlitzToManticoreConverter {

  def main(args: Array[String]) {
    var dir1 = new File(args(0)).listFiles.filter(x => !x.getName.contains("manticore")).toList

    dir1.foreach(file => {
      val source = Source.fromFile(file)
      val fileName = file.getName
      val converted = new PrintWriter(
        new File(args(0)).toPath.resolve(fileName.substring(0, fileName.length - ".csv".length) + "-manticore.csv")
          .toFile)

      val numBodies = fileName.split("-")(1).split("\\.")(0).toInt
      converted.write(s"$numBodies\n")

      source.getLines().zipWithIndex.foreach(line => {
        val split = line._1.split(",")
        val x = split(1).toDouble
        val y = split(2).toDouble
        val xspeed = split(3).toDouble
        val yspeed = split(4).toDouble
        val mass = split(5).toDouble
        converted.write(s"$x $y $mass $xspeed $yspeed\n")
      })

      converted.close()
    })
  }
}
