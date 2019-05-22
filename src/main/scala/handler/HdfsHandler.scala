/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

package handler

import metric.analysis.conf.HdfsConf._
import org.apache.hadoop.fs.{FileSystem, Path}

object HdfsHandler {

  /**
    * Move a file from local to hdfs
    * @param source       : Location of the file on the local fs
    * @param dest         : Location to save the file in hdfs
    */
  def moveFromLocal(source: Path, dest: Path): Unit = {
    val fs = FileSystem.get(load())
    fs.moveFromLocalFile(source, dest)
  }

  /**
    * Recursively Delete a file/folder in hdfs
    * @param path     : path of the file/folder to delete
    * @return         : Boolean corresponding to whether or not operation was executed correctly
    */
  def delete(path: Path): Boolean = {
    val fs = FileSystem.get(load)
    fs.delete(path, true)
  }

  /**
    * Checks if a specified path exists
    * @param path     : Path to check
    * @return         : Boolean corresponding to whether or not the path exists
    */
  def exists(path: Path): Boolean = {
    val fs = FileSystem.get(load)
    fs.exists(path)
  }

  /**
    * Creates a directory
    * @param path   : Path to create the directory in
    * @return       : Boolean corresponding to whether or not not the operation was successful
    */
  def mkdirs(path: Path): Boolean = {
    val fs = FileSystem.get(load)
    fs.mkdirs(path)
  }

  /**
    * Lists all files in a directory
    * @param path         : Path to the directory
    * @param extension    : Extension to filter on. optional. Default behavior is to display everything.
    * @return List of Strings consisting of all the files found.
    */
  def listFiles(path: Path, extension: String = "") = {
    val fs = FileSystem.get(load)
    fs.listStatus(path)
      .map(x => x.getPath.getName)
      .filter(x => x.endsWith(extension))
      .toList
  }

}
