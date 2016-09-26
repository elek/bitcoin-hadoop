import org.zuinnote.hadoop.bitcoin.format._;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.fs.Path;
val conf = new org.apache.hadoop.mapred.JobConf();
FileInputFormat.addInputPath(conf, new Path("hdfs://localhost:9000/bitcoin/blk00001.dat"));
val rdd = sc.hadoopRDD(conf, classOf[BitcoinBlockFileInputFormat], classOf[BytesWritable], classOf[BitcoinBlock], 2);