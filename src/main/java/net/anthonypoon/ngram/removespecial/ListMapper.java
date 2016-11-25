/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.anthonypoon.ngram.removespecial;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author ypoon
 */
public class ListMapper extends Mapper<Text, Text, Text, Text> {

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        //System.out.println("Input line - > " + value.toString());
        String[] strArray = value.toString().split("\t");
        Pattern pattern = Pattern.compile("([^\\p{L}'\\-.])");
        Matcher matcher = pattern.matcher(strArray[0]);
        String str = "Special char =";
        boolean haveChar = false;
        while (matcher.find()) {
            haveChar = true;
            str = str + " " + matcher.group(1);
        }
        if (haveChar) {
            context.write(new Text(strArray[0]), new Text(strArray[1] + "\t" + strArray[2] + "\t" + strArray[3] + "\t" + strArray[4] + "\t" + str));
        }
    }
    
}
