package com.sohu.rdc.inf.cdn.offline.test;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;

import java.awt.image.ImagingOpException;
import java.io.IOException;

/**
 * Created by zengxiaosen on 2017/5/22.
 */
public class HBaseReadJob {
    public static class MyMapper extends TableMapper<ImmutableBytesWritable, Put>{
        public void map(ImmutableBytesWritable row, Result value, Context context){
            try {
                //将结果写入reduce
                context.write(row, resultToPut(row, value));
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        private static Put resultToPut(ImmutableBytesWritable key, Result value) throws IOException{
            Put put = new Put(key.get());
            // value是查询结果
            //将结果装载到Put
            for(KeyValue kv : value.raw()){
                put.add(kv);
            }
            return put;
        }


        public static void main(String[] args){

            // set other scan attrs
//        TableMapReduceUtil.initTableMapperJob(
//            "test",      // input table
//            scan,              // Scan instance to control CF and attribute selection
//            MyMapper.class,   // mapper class
//            ImmutableBytesWritable.class,              // mapper output key
//            Put.class,              // mapper output value
//            job);

        }
    }
}
