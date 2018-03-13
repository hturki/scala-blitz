package barneshut

import java.io.{File, PrintWriter}

import scala.io.Source

object ManticoreToBlitzConverter {

  def main(args: Array[String]) {
    var dir1 = new File(args(0)).listFiles.filter(_.getName.contains("manticore")).toList

    dir1.foreach(file => {
      val source = Source.fromFile(file)
      val fileName = file.getName
      val converted = new PrintWriter(
        new File(args(0)).toPath.resolve(fileName.substring(0, fileName.length - "-manticore.csv".length) + ".csv")
          .toFile)

      var first = true
      source.getLines().zipWithIndex.foreach(line => {
        if (first) {
          first = false
        } else {
          val split = line._1.split(" ")
          val i = line._2 - 1
          val x = split(0).toDouble
          val y = split(1).toDouble
          val mass = split(2).toDouble
          val xspeed = split(3).toDouble
          val yspeed = split(4).toDouble
          converted.write(s"$i,$x,$y,$xspeed,$yspeed,$mass\n")
        }
      })

      converted.close()
    })
  }
}
