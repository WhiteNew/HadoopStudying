package org.hadooplab;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.conf.*;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Iterator;

public class WordCount {

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString().replaceAll("[^A-Za-z ]", "");
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				context.write(word, one);
			}
		}
	}
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
        	int sum = 0;
        	for (IntWritable val : values) {
        	sum += val.get();
        	}
        	context.write(key, new IntWritable(sum));
    }
	}
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
                System.err.println("Usage: WordCount needs two arguments <input> <output> files");
                System.exit(-1);
        }


        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(WordCount.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        int res = job.waitForCompletion(true) ? 0 : 1;
	if(job.isSuccessful()) {
        	System.out.println("Job was successful");
        } else if(!job.isSuccessful()) {
                System.out.println("Job was not successful");            
        }
        System.exit(res);
    }
	}
