/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.anthonypoon.ngram.removespecial;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author ypoon
 */
public class ListReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        /**List<String> charArray = new ArrayList();
        String str = "Spec char =";
        for (Text val : values) {
            if (!charArray.contains(val.toString())) {
                charArray.add(val.toString());
                str = str + "\t" + val.toString();
            }
        }**/
        for (Text val : values) {
            context.write(key, val);
        }
        
    }
    
}
