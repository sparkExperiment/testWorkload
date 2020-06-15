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

object SparkPageRank11 {

  def showWarning() {
    System.err.println(
      """WARN: This is a naive implementation of PageRank and is given as an example!
        |Please use the PageRank implementation found in org.apache.spark.graphx.lib.PageRank
        |for more conventional use.
      """.stripMargin)
  }

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: SparkPageRank <file> <iter>")
      System.exit(1)
    }

    showWarning()

    val iters = if (args.length > 1) args(1).toInt else 5

    val spark = SparkSession
      .builder
      .appName("SparkPageRank").config("spark.shuffle.sort.bypassMergeThreshold", 0)
      .getOrCreate()

    for (i <- 1 to iters) {
      val lines = spark.read.textFile(args(0)).rdd
      var ll1 = lines.map{ s =>
        val parts = s.split("\\s+")
        (parts(0), parts(1))
      }.mapValues(v => 1.0).distinct()

      val lines2 = spark.read.textFile(args(2)).rdd
      var ll2 = lines2.map{ s =>
        val parts = s.split("\\s+")
        (parts(0), parts(1))
      }.groupByKey()

      val lines3 = spark.read.textFile(args(3)).rdd
      var ll3 = lines3.map{ s =>
        val parts = s.split("\\s+")
        (parts(0), parts(1))
      }.groupByKey()

      val lines4 = spark.read.textFile(args(4)).rdd
      var ll4 = lines4.map{ s =>
        val parts = s.split("\\s+")
        (parts(0), parts(1))
      }.groupByKey()

      val lines5 = spark.read.textFile(args(5)).rdd
      var ll5 = lines5.map{ s =>
        val parts = s.split("\\s+")
        (parts(0), parts(1))
      }.groupByKey()

      val lines6 = spark.read.textFile(args(6)).rdd
      var ll6 = lines6.map{ s =>
        val parts = s.split("\\s+")
        (parts(0), parts(1))
      }.groupByKey()

      val lines7 = spark.read.textFile(args(7)).rdd
      var ll7 = lines7.map{ s =>
        val parts = s.split("\\s+")
        (parts(0), parts(1))
      }.groupByKey()

      val lines8 = spark.read.textFile(args(8)).rdd
      var ll8 = lines8.map{ s =>
        val parts = s.split("\\s+")
        (parts(0), parts(1))
      }.groupByKey()

      val lines9 = spark.read.textFile(args(9)).rdd
      var ll9 = lines9.map{ s =>
        val parts = s.split("\\s+")
        (parts(0), parts(1))
      }.groupByKey()

      val lines10 = spark.read.textFile(args(10)).rdd
      var ll10 = lines10.map{ s =>
        val parts = s.split("\\s+")
        (parts(0), parts(1))
      }.groupByKey()

      val lines11 = spark.read.textFile(args(11)).rdd
      var ll11 = lines11.map{ s =>
        val parts = s.split("\\s+")
        (parts(0), parts(1))
      }.groupByKey()

      var ranks = ll1.reduceByKey(_ + _)

      val contribs = ll2.join(ranks).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }

      val ranks1 = contribs.reduceByKey(_ + _)

      val contribs2 = ll3.join(ranks1).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }

      val ranks2 = contribs2.reduceByKey(_ + _)

      val contribs3 = ll4.join(ranks2).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }

      val ranks3 = contribs3.reduceByKey(_ + _)

      val contribs4 = ll5.join(ranks3).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }

      val ranks4 = contribs4.reduceByKey(_ + _)

      val contribs5 = ll6.join(ranks4).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }

      val ranks5 = contribs5.reduceByKey(_ + _)

      val contribs6 = ll7.join(ranks5).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }

      val ranks6 = contribs6.reduceByKey(_ + _)

      val contribs7 = ll8.join(ranks6).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }

      val ranks7 = contribs7.reduceByKey(_ + _)

      val contribs8 = ll9.join(ranks7).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }

      val ranks8 = contribs8.reduceByKey(_ + _)

      val contribs9 = ll10.join(ranks8).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }

      val ranks9 = contribs9.reduceByKey(_ + _)

      val contribs10 = ll11.join(ranks9).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }

      val ranks10 = contribs10.reduceByKey(_ + _).collect()
    }


    spark.stop()
  }
}
