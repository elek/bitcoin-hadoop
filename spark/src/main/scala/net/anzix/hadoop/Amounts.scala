package net.anzix.hadoop

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.mapred.FileInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bitcoinj.script.Util
import org.zuinnote.hadoop.bitcoin.format._

import scala.collection.JavaConversions._
import scala.collection.mutable

object Amounts {
  def main(args: Array[String]) = {
    val sc = new SparkContext(new SparkConf().setAppName("btc").setMaster("local[1]"))
    val conf = new org.apache.hadoop.mapred.JobConf()
    val blockPath = new Path("/Users/melek/Library/Application Support/Bitcoin/blocks/blk00000.dat")
    FileInputFormat.addInputPath(conf, blockPath)
    val rdd = sc.hadoopRDD(conf, classOf[BitcoinTransactionFileInputFormat], classOf[BytesWritable], classOf[BitcoinTransaction], 2)
    val result = rdd
      .flatMap(t => {
        var key: String = t._1.getBytes.map(b => "%02x" format b).mkString
        println(key)
        val outs: mutable.Buffer[(String, Long)] = t._2.getListOfOutputs.zipWithIndex.map { case (out, ix) => (key + "_" + ix, out.getValue) }
        val ins: mutable.Buffer[(String, Long)] = t._2.getListOfInputs.zipWithIndex.map { case (out, ix) => (key + "_" + ix, -1 * out.) }
        outs
      })

    result.foreach(r => println(r._1, r._2))

  }
}
