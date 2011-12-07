/** Licensed to Gravity.com under one
  * or more contributor license agreements. See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership. Gravity.com licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License. You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

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