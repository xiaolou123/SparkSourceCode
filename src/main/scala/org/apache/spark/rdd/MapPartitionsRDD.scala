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

package org.apache.spark.rdd

import scala.reflect.ClassTag

import org.apache.spark.{Partition, TaskContext}

private[spark] class MapPartitionsRDD[U: ClassTag, T: ClassTag](
    prev: RDD[T],
    f: (TaskContext, Int, Iterator[T]) => Iterator[U],  // (TaskContext, partition index, iterator)
    preservesPartitioning: Boolean = false)
  extends RDD[U](prev) {

  override val partitioner = if (preservesPartitioning) firstParent[T].partitioner else None

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  /**
   * 这里就很有含义了
   * compute实际上，什么意思？
   * 就是，针对RDD中的某个partition执行我们给这个RDD定义的算子和函数
   * 我们定义的算子和函数，是什么东东？？我们是不是在这里没有看到啊！！
   * 这个f，你可以理解成我们自己定义的算子和函数，但是呢，Spark内部进行了封装的，还实现了一些其他的逻辑
   * 调用到这里为止，其实就是在针对rdd的partition，执行自定义的计算操作，并返回新的RDD的partition的数据
   */
  override def compute(split: Partition, context: TaskContext) =
    f(context, split.index, firstParent[T].iterator(split, context))
}
