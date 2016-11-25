/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.anthonypoon.ngram.removespecial;

import java.io.IOException;

import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

/**
 *
 * @author ypoon
 */
public class RemoveReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String, String[]> yearMap = new TreeMap();
        for (Text val : values) {
            String[] strArray = val.toString().split("\t");
            // strArray[0] is year
            if (!yearMap.containsKey(strArray[0])) {
                yearMap.put(strArray[0], strArray);
            } else {
                String[] tmpArray = yearMap.get(strArray[0]);
                for (int i = 1; i < 4; i ++) {
                    Integer tmpInt = Integer.valueOf(tmpArray[i]) + Integer.valueOf(strArray[i]);
                    tmpArray[i] = tmpInt.toString();
                }
                yearMap.put(strArray[0], tmpArray);
            }
        }
        for (Map.Entry<String, String[]> pair : yearMap.entrySet()) {
            context.write(key, new Text(pair.getValue()[0] + "\t" + pair.getValue()[1]+ "\t" + pair.getValue()[2]+ "\t" + pair.getValue()[3]));
        }
    }
    
    
}
