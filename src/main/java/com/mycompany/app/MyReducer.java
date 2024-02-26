package com.mycompany.app;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author ajay
 */
public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
    
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
        int count = 0;
        int loan = 0;
        int min = 0;
        for(IntWritable value: values){
            loan = value.get();
            if(loan < min){
                min = loan;
            }
            count += 1;
        }
        context.write(key, new IntWritable(min));
               
    }
}