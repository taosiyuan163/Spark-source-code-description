/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.scheduler

/**
 * A location where a task should run. This can either be a host or a (host, executorID) pair.
 * In the latter case, we will prefer to launch the task on that executorID, but our next level
 * of preference will be executors on the same host if this is not possible.
 */
private[spark] sealed trait TaskLocation {
  def host: String
}

/**
 * A location that includes both a host and an executor id on that host.
 */
private [spark]
case class ExecutorCacheTaskLocation(override val host: String, executorId: String)
  extends TaskLocation {
  // executor_host_executorId
  override def toString: String = s"${TaskLocation.executorLocationTag}${host}_$executorId"
}

/**
 * A location on a host.
 */
private [spark] case class HostTaskLocation(override val host: String) extends TaskLocation {
  override def toString: String = host
}

/**
 * A location on a host that is cached by HDFS.
 */
private [spark] case class HDFSCacheTaskLocation(override val host: String) extends TaskLocation {
  override def toString: String = TaskLocation.inMemoryLocationTag + host
}

private[spark] object TaskLocation {
  // We identify hosts on which the block is cached with this prefix.  Because this prefix contains
  // underscores, which are not legal characters in hostnames, there should be no potential for
  // confusion.  See  RFC 952 and RFC 1123 for information about the format of hostnames.
  val inMemoryLocationTag = "hdfs_cache_"

  // Identify locations of executors with this prefix.
  val executorLocationTag = "executor_"

  def apply(host: String, executorId: String): TaskLocation = {
    new ExecutorCacheTaskLocation(host, executorId)
  }

  /**
   * Create a TaskLocation from a string returned by getPreferredLocations.
   * These strings have the form executor_[hostname]_[executorid], [hostname], or
   * hdfs_cache_[hostname], depending on whether the location is cached.
   */
  // 若之checkpoint过那传递过来的str可能是hdfs_cache_[hostname]或者executor_[hostname]_[executorid]
  // 若没有则是[hostname]
  def apply(str: String): TaskLocation = {
    // inMemoryLocationTag = "hdfs_cache_"
    // 截取掉前面是hdfs_cache_字符的str，若前缀没有包含就直接返回原来的str
    val hstr = str.stripPrefix(inMemoryLocationTag)
    // 判断是否是被持久化到过hdfs
    if (hstr.equals(str)) {
      // 如果不是则判断前缀是否是executor_
      if (str.startsWith(executorLocationTag)) {
        // 转换成[hostname]_[executorid]
        val hostAndExecutorId = str.stripPrefix(executorLocationTag)
        // 返回的是Array[String](hostname,executorid)
        val splits = hostAndExecutorId.split("_", 2)
        require(splits.length == 2, "Illegal executor location format: " + str)
        val Array(host, executorId) = splits
        // 生成的对象仅包含标识符：executor_host_executorId
        new ExecutorCacheTaskLocation(host, executorId)
      } else {
        // 走到这说明没有被checkpoint过
        // 生成的对象仅包含标识符：host
        new HostTaskLocation(str)
      }
    } else {
      // 走到这里说明之前有被checkpoint到hdfs
      // 生成的对象仅包含标识符：hdfs_cache_host
      new HDFSCacheTaskLocation(hstr)
    }
  }
}
