package com.mycompany.app;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author ajay
 * Modified by: Nathan Eloe
 * 
 */

public class MyMapper extends Mapper<LongWritable, Text, Text, Text>{
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String[] row = value.toString().split("\t");
        context.write(new Text(row[5]), new Text(row[2] + "\t" + Double.parseDouble(row[12])));
    }
    
}