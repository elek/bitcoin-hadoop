package org.zuinnote.hadoop.bitcoin.example.tasks;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.zuinnote.hadoop.bitcoin.format.BitcoinTransaction;
import org.zuinnote.hadoop.bitcoin.format.BitcoinTransactionInput;

import java.io.IOException;

/**
 * Created by melek on 9/26/16.
 */
public class UnspentAmountMap  extends MapReduceBase implements Mapper<BytesWritable, BitcoinTransaction, Text, IntWritable> {

    @Override
    public void map(BytesWritable bytesWritable, BitcoinTransaction bitcoinTransaction, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
        for (BitcoinTransactionInput ti: bitcoinTransaction.getListOfInputs()){

        }
    }
}
