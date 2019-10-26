package mx.cic

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.java.DataSet
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem

/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram
 * over some sample data
 *
 * This example shows how to:
 *
 *   - write a simple Flink program.
 *   - use Tuple data types.
 *   - write and use user-defined functions.
 */



object WordCount {
  def main(args: Array[String]) {

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    //val parameters = new Configuration
    // set the recursive enumeration parameter
    //parameters.setBoolean("recursive.file.enumeration", true)

    // get input data
    println("======================================")

    val text01 = env.readTextFile(args(0))
      //.withParameters(parameters)
    val text02 = env.readTextFile(args(1))
    val text03 = env.readTextFile(args(2))

    val counts01 = text01.flatMap { _.toLowerCase.split("\\W+") }
      .map { (_, 1) }
      .groupBy(0)
      .sum(1)

    val counts02 = text02.flatMap { _.toLowerCase.split("\\W+") }
      .map { (_, 1) }
      .groupBy(0)
      .sum(1)

    val counts03 = text03.flatMap { _.toLowerCase.split("\\W+") }
      .map { (_, 1) }
      .groupBy(0)
      .sum(1)

      //counts.writeAsText(args(2),FileSystem.WriteMode.OVERWRITE)

    // execute and print result
    println("======================================")
    System.out.println(args(0))
    System.out.println(args(1))
    System.out.println(args(2))
    println("Número de archivos 3")

    println("======================================")
    println("Palabras en archivo")
    System.out.println(args(0))
    counts01.sum(field = 1).print()

    println("Palabras en archivo")
    System.out.println(args(1))
    counts02.sum(field = 1).print()

    println("Palabras en archivo")
    System.out.println(args(2))
    counts03.sum(field = 1).print()



  }
}

