package com.gravity.hadoop

import org.apache.hadoop.fs.FileSystem

object Implicits {
  implicit def asRichFileSystem(fs: FileSystem) = new RichFileSystem(fs)
}