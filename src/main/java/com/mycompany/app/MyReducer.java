package com.mycompany.app;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import java.util.ArrayList;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author ajay
 */
public class MyReducer extends Reducer<Text, Text, Text, Text>{
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
        ArrayList<String> name = new ArrayList<>();
        ArrayList<Double> salary = new ArrayList<>();
        ArrayList<String> line = new ArrayList<>();
        Text output = null;
        for(Text value: values){
           String[] row = value.toString().split("\t");
           name.add(row[0]);
           salary.add(Double.parseDouble(row[1]));
           line.add(row[0] + "\t" + row[1]);
        }


        double max = 0;
        int count = 0;
        for(double num: salary){
            if(count == 0){
                max = num;
                count += 1;
            }
            else if(max < num){
                max = num;
            }
        }
        count = 0;
        for(String text: line){
            if(text.contains(Double.toString(max))){
                output = new Text(line.get(count));
                context.write(key, output);
            }
            count ++;
        }
               
    }
}