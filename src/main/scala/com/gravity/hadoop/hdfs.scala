package com.gravity.hadoop

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{PathFilter, Path, FileSystem}
import org.apache.hadoop.io.{SequenceFile, Writable}
import java.io._
import scala.collection.mutable.Buffer

/**
 * Convenience methods for reading and writing files to and from hdfs.
 */
package object hdfs {
  implicit def asRichFileSystem(fs: FileSystem) = new RichFileSystem(fs)


  /**
   * Gives you a file writer into the local cluster hdfs instance
   * @param relpath The relative path
   * @param recreateIfPresent If true, will delete the file if it already exists
   * @param work A function that works with the output.  The output will be closed when this function goes out of scope.
   * @return
   */
  def withHdfsWriter(fs: FileSystem, relpath: String, recreateIfPresent: Boolean = true)(work: (BufferedWriter) => Unit) {
    val path = new Path(relpath)
    val fileSystem = fs
    if (recreateIfPresent) {
      if (fileSystem.exists(path)) {
        fileSystem.delete(path)
      }
    }
    val output = new BufferedWriter(new OutputStreamWriter(fileSystem.create(path)))
    try {
      work(output)
    } finally {
      output.close()
    }
  }

  def perPartSequenceFileKV[K <: Writable, V <: Writable](fs: FileSystem, relpath: String,  conf: Configuration,fileBeginsWith:String="part-")(key: K, value: V)(line: (K, V) => Unit) {
    val glob = new Path(relpath)

    val files = fs.listStatus(glob, new PathFilter {
      override def accept(path: Path) = path.getName.startsWith(fileBeginsWith)
    })

    for (file <- files) {
      perSequenceFileKV(fs, file.getPath.toString, conf)(key, value)(line)
    }
  }

  def perSequenceFileKV[K <: Writable, V <: Writable](fs: FileSystem, relpath: String, conf: Configuration)(key: K, value: V)(line: (K, V) => Unit) {
      val reader = new SequenceFile.Reader(fs, new Path(relpath), conf)

      try {
        while (reader.next(key, value)) {
          line(key, value)
        }
      } finally {
        reader.close()
      }
  }


  /**
   * Allows you to work with a reader opened into an hdfs file on the test cluster.
   * @param relpath The path to the file
   * @param work The work you will do
   * @tparam A If you want to return a value after the work, here it is.
   * @return
   */
  def withHdfsReader[A](fs: FileSystem, relpath: String)(work: (BufferedReader) => A): A = {
    val path = new Path(relpath)
    val input = new BufferedReader(new InputStreamReader(fs.open(path)))

    try {
      work(input)
    } finally {
      input.close()
    }
  }

  def withHdfsDirectoryReader[A](fs: FileSystem, relpath: String)(work: (BufferedReader) => A): A = {
    val path = new Path(relpath)
    val input = new BufferedReader(new InputStreamReader(new RichFileSystem(fs).openParts(path)))
    try {
      work(input)
    } finally {
      input.close()
    }
  }


  /**
   * Reads a file into a buffer, allowing you to decide what's in the buffer depending on the output of the linereader function
   * @param relpath Path to local hdfs buffer
   * @param linereader Function to return an element in the buffer, given the line fo the file
   * @tparam A
   * @return
   */
  def perHdfsLineToSeq[A](fs: FileSystem, relpath: String)(linereader: (String) => A): Seq[A] = {
    val result = Buffer[A]()
    withHdfsReader(fs, relpath) {
      input =>
        var done = false
        while (!done) {
          val line = input.readLine()
          if (line == null) {
            done = true
          } else {
            result += linereader(line)
          }
        }
    }
    result.toSeq
  }


  /**
   * Reads a file line by line.  If you want to have the results in a buffer, use perHdfsLineToSeq
   * @param relpath
   * @param linereader
   * @tparam A
   * @return
   */
  def perHdfsLine[A](fs: FileSystem, relpath: String)(linereader: (String) => Unit) {
    withHdfsReader(fs, relpath) {
      input =>
        var done = false
        while (!done) {
          val line = input.readLine()
          if (line == null) {
            done = true
          } else {
            linereader(line)
          }
        }
    }
  }

  /**
   * For each line in a directory of files
   * @param relpath Path to files (or glob path)
   * @param linereader Will be invoked once per line with a string representation
   * @return Bupkiss
   */
  def perHdfsDirectoryLine(fs: FileSystem, relpath: String)(linereader: (String) => Unit) {
    withHdfsDirectoryReader(fs, relpath) {
      input =>
        var done = false
        while (!done) {
          val line = input.readLine()
          if (line == null) {
            done = true
          } else {
            linereader(line)
          }
        }
    }
  }
}