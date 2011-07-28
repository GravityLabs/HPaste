package com.gravity.hadoop

import scala.collection.JavaConversions._
import org.apache.hadoop.fs.{PathFilter, Path, FileSystem}
import java.io.SequenceInputStream

class RichFileSystem(fs: FileSystem) {
  /**
   * Opens and streams the Hadoop part files in the given directory, so you
   * can treat them as a single merged file.
   */
  def openParts(dir: Path): SequenceInputStream = {
    val parts = fs.listStatus(dir, new PathFilter {
      override def accept(path: Path) = path.getName.startsWith("part-")
    }).toIterator
    val inputStreams = parts map (fileStatus => fs.open(fileStatus.getPath))
    new SequenceInputStream(inputStreams)
  }
}