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

import java.util.Random

import scala.math.exp

import breeze.linalg.{DenseVector, Vector}

import org.apache.spark.sql.SparkSession

/**
 * Logistic regression based classification.
 * Usage: SparkLR [slices]
 *
 * This is an example implementation for learning how to use Spark. For more conventional use,
 * please refer to org.apache.spark.ml.classification.LogisticRegression.
 */
object LR {
  var N = 10000  // Number of data points
  var D = 10   // Number of dimensions
  val R = 0.7  // Scaling factor
  var ITERATIONS = 15
  val rand = new Random(42)

  case class DataPoint(x: Vector[Byte], y: Byte)

  def generateData: Array[DataPoint] = {
    def generatePoint(i: Int): DataPoint = {
      val y = if (i % 2 == 0) -1 else 1
      val x = DenseVector.fill(D) {(rand.nextGaussian + y * R).toByte}
      DataPoint(x, y.toByte)
    }
    Array.tabulate(N)(generatePoint)
  }

  def showWarning() {
    System.err.println(
      """WARN: This is a naive implementation of Logistic Regression and is given as an example!
        |Please use org.apache.spark.ml.classification.LogisticRegression
        |for more conventional use.
      """.stripMargin)
  }

  def main(args: Array[String]) {

    showWarning()

    val spark = SparkSession
      .builder
      .appName("LR")
      .getOrCreate()

    val dataset = if (args.length > 2) {
      if (args(2).toInt > 20) System.err.println(
        """Too much!!""".stripMargin)
      val dataPointNum = args(2).toLong * 1024 * 1024 * 1024 / 10

      if (dataPointNum / Int.MaxValue == 0) {
        N = dataPointNum.toInt
        generateData
      } else {
        val times = dataPointNum / Int.MaxValue
        var l = Array[DataPoint]()
        N = Int.MaxValue
        for (i <- 1.to(times.toInt)) {
          l = l ++ generateData
        }
        N = (dataPointNum - times * Int.MaxValue).toInt
        l = l ++ generateData
        l
      }
    } else {
      N = 10000
      generateData
    }
    val numSlices = if (args.length > 0) args(0).toInt else 2
    ITERATIONS = if (args.length > 1) args(1).toInt else 15
    val points = spark.sparkContext.parallelize(dataset, numSlices).cache()

    // Initialize w to a random value
    var w = DenseVector.fill(D) {2 * rand.nextDouble - 1}
    println("Initial w: " + w)

    for (i <- 1 to ITERATIONS) {
      println("On iteration " + i)
      val gradient = points.map { p =>
        val l = p.x.map(s => s.toDouble)
        val n = p.y.toDouble
        l * (1 / (1 + exp(-n * (w.dot(l)))) - 1) * n
      }.reduce(_ + _)
      w -= gradient
    }


    println("Final w: " + w)

    spark.stop()
  }
}
// scalastyle:on println
