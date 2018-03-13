package barneshut

import java.io.File
import java.nio.file.Paths

import scala.io.Source

object ResultDiff {

  def main(args: Array[String]) {
    var dir1 = new File(args(0)).listFiles.filter(x => x.isFile && !x.getName.contains("0.csv")).toList

    dir1.foreach(file => {
      val source = Source.fromFile(file)
      val source2 = Source.fromFile(Paths.get(args(1)).resolve(file.getName).toFile)

      source.getLines().zip(source2.getLines()).foreach(x => {
        val line1 = x._1
        val line2 = x._2

        val equal = line1.split(",").zip(line2.split(",")).forall(y => Math.abs(y._1.toDouble - y._2.toDouble) < 0.1)

        if (!equal) {
          System.out.println(s"Difference found in file ${file}\n${line1}\n${line2}\n\n")
        }
      })
    })
  }
}
