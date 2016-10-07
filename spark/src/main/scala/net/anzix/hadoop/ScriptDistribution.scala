package net.anzix.hadoop

;

import org.zuinnote.hadoop.bitcoin.format._
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.mapred.FileInputFormat
import org.apache.hadoop.fs.Path
import org.bitcoinj.script.Util
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._

object ScriptDistribution {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("btc").setMaster("local[1]"))
    val conf = new org.apache.hadoop.mapred.JobConf()
    val blockPath = new Path("/Users/melek/Library/Application Support/Bitcoin/blocks/blk00001.dat")
    FileInputFormat.addInputPath(conf, blockPath)
    val rdd = sc.hadoopRDD(conf, classOf[BitcoinTransactionFileInputFormat], classOf[BytesWritable], classOf[BitcoinTransaction], 2)
    var result = rdd.flatMap(t => t._2.getListOfOutputs()).map(_.getTxOutScript()).map(Util.parseToString).map(v => (v, 1)).reduceByKey(_ + _).takeOrdered(100)(Ordering[Int].reverse.on(x => x._2))
    result.foreach(b => println(b._1, b._2))

  }
}
