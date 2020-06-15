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

// scalastyle:off println
package org.apache.spark.examples

import org.apache.spark.sql.SparkSession

object BranchTest {

  def main(args: Array[String]) {

    val iters = if (args.length > 1) args(1).toInt else 5

    val spark = SparkSession
      .builder
      .appName("SparkPageRank").config("spark.shuffle.sort.bypassMergeThreshold", 0)
      .getOrCreate()

    for (i <- 1 to iters) {
      val lines = spark.read.textFile(args(0)).rdd
      val ll1 = lines.map{ s =>
        val parts = s.split("\\s+")
        (parts(0), parts(1))
      }.mapValues(v => 1.0).reduceByKey(_ + _).groupByKey()

      // var ranks = ll1.reduceByKey(_ + _)

      val ranks1 = ll1.collect()
    }

    spark.stop()
  }
}
