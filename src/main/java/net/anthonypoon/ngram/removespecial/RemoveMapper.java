/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.anthonypoon.ngram.removespecial;

import java.io.IOException;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

/**
 *
 * @author ypoon
 */
public class RemoveMapper extends Mapper<Text, Text, Text, Text>{
    private static final Logger logger = Logger.getLogger(RemoveMapper.class);
    @Override
    protected void map(Text key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        //System.out.println("Input line - > " + value.toString());
        String[] strArray = value.toString().split("\t");
        Pattern whiteListPattern = Pattern.compile("^([\\p{L}'\\-. ]+)$");
        Matcher whiteListmatcher = whiteListPattern.matcher(strArray[0]);
        // Need to have at least 1 non-special character
        Pattern letterPattern = Pattern.compile("^((?!\\p{L}).)*$");
        boolean noWord = false;
        if (whiteListmatcher.matches()) {
            for (String str : strArray[0].split(" ")) {
                Matcher letterMatcher = letterPattern.matcher(str);
                // if the gram do not contain any letter
                if (letterMatcher.matches()) {
                    System.out.println(str);
                    noWord = true;
                }
            }
            if (!noWord) {
                context.write(new Text(strArray[0].toLowerCase(Locale.ENGLISH)), new Text(strArray[1]+"\t"+strArray[2]+"\t"+strArray[3]+"\t"+strArray[4]));
            }
        } 
    }
}
