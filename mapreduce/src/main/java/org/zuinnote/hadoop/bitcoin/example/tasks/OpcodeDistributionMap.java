/**
 * Copyright 2016 ZuInnoTe (Jörn Franke) <zuinnote@gmail.com>
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * <p>
 * Simple Mapper for counting the total number of Bitcoin transaction inputs of all Bitcoin transactions
 * <p>
 * Simple Mapper for counting the total number of Bitcoin transaction inputs of all Bitcoin transactions
 * <p>
 * Simple Mapper for counting the total number of Bitcoin transaction inputs of all Bitcoin transactions
 * <p>
 * Simple Mapper for counting the total number of Bitcoin transaction inputs of all Bitcoin transactions
 * <p>
 * Simple Mapper for counting the total number of Bitcoin transaction inputs of all Bitcoin transactions
 * <p>
 * Simple Mapper for counting the total number of Bitcoin transaction inputs of all Bitcoin transactions
 * <p>
 * Simple Mapper for counting the total number of Bitcoin transaction inputs of all Bitcoin transactions
 * <p>
 * Simple Mapper for counting the total number of Bitcoin transaction inputs of all Bitcoin transactions
 * <p>
 * Simple Mapper for counting the total number of Bitcoin transaction inputs of all Bitcoin transactions
 * <p>
 * Simple Mapper for counting the total number of Bitcoin transaction inputs of all Bitcoin transactions
 */


/**
 * Simple Mapper for counting the total number of Bitcoin transaction inputs of all Bitcoin transactions
 */
package org.zuinnote.hadoop.bitcoin.example.tasks;

/**
 * Author: Jörn Franke <zuinnote@gmail.com>
 *
 */

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.bitcoinj.script.Script;
import org.bitcoinj.script.ScriptChunk;
import org.zuinnote.hadoop.bitcoin.format.BitcoinTransaction;
import org.zuinnote.hadoop.bitcoin.format.BitcoinTransactionOutput;

import java.io.IOException;

public class OpcodeDistributionMap extends MapReduceBase implements Mapper<BytesWritable, BitcoinTransaction, Text, IntWritable> {
    private final static Text defaultKey = new Text("Transaction Input Count:");

    public void map(BytesWritable key, BitcoinTransaction value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        // get the number of inputs to transaction
        for (BitcoinTransactionOutput outputTrans : value.getListOfOutputs()) {
            try {
                Script script = new Script(outputTrans.getTxOutScript());
                StringBuffer b = new StringBuffer();
                for (ScriptChunk chunk : script.getChunks()) {
                    if (chunk.isOpCode()) {
                        b.append(chunk + " ");
                    }
                }
                output.collect(new Text(b.toString()), new IntWritable(1));
            } catch (Exception ex) {
                System.out.println(value);
                for (byte b : outputTrans.getTxOutScript()) {
                    System.out.print(String.format("0x%01X ", b));

                }
                System.out.println();
                ex.printStackTrace();
            }

        }
    }

}

