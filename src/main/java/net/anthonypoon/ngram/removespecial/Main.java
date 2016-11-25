/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.anthonypoon.ngram.removespecial;

import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author ypoon
 */
public class Main {
    public static void main(String args[]) throws Exception {
        Options options = new Options();
        options.addOption("a", "action", true, "Action");
        options.addOption("i", "input", true, "input");
        options.addOption("o", "output", true, "output");
        options.addOption("c", "compressed", false, "is lzo zipped");
        CommandLineParser parser = new GnuParser();
        CommandLine cmd = parser.parse(options, args);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        if (cmd.hasOption("compressed")) {
            job.setInputFormatClass(SequenceFileAsTextInputFormat.class);
        }
        job.setJarByClass(Main.class);
        //job.setInputFormatClass(SequenceFileAsTextInputFormat.class); 
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        switch (cmd.getOptionValue("action")) {
            case "list":
                job.setMapperClass(ListMapper.class);
                //job.setNumReduceTasks(0);
                job.setReducerClass(ListReducer.class);
                break;
            case "remove":
                job.setMapperClass(RemoveMapper.class);
                job.setReducerClass(RemoveReducer.class);
                break;
            default:
                throw new IllegalArgumentException("Missing action");
        }
        String timestamp = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss").format(new Date());
        FileInputFormat.setInputPaths(job, new Path(cmd.getOptionValue("input")));
        FileOutputFormat.setOutputPath(job, new Path(cmd.getOptionValue("output") + "/" + timestamp));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
