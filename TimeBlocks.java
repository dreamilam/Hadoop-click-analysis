package timeblocks.timeblocks;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TimeBlocks {
	
	public static class TimeBlocksMapper extends Mapper<Object, Text, Text, IntWritable>{
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] tokens = value.toString().split(",");
			String[] timestamp = tokens[1].split("-");
			String hour = timestamp[2].substring(3,5) ;
			int price = Integer.parseInt(tokens[3]);
			int quantity = Integer.parseInt(tokens[4]);
			
			Text timeBlock = new Text(hour);
			IntWritable revenue = new IntWritable(price * quantity);
			context.write(timeBlock, revenue);
		}
		
	}
	
	public static class RevenueReducer extends Reducer<Text, IntWritable, Text, LongWritable>{
		
		public void reduce(Text timeBlock, Iterable<IntWritable> revenues, Context context) throws IOException, InterruptedException {
			long totalRevenue = 0;
			Iterator<IntWritable> iterator = revenues.iterator();
			while(iterator.hasNext()) {
				totalRevenue += iterator.next().get();
			}
			LongWritable netRevenue = new LongWritable(totalRevenue);
			context.write(timeBlock, netRevenue);
		}
	}
	
	public static class TimeBlocksMapper2 extends Mapper<Object, Text, Text, LongWritable>{
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\t");
			Text hour = new Text(tokens[0]);
			LongWritable revenue = new LongWritable(Long.parseLong(tokens[1]));
			context.write(hour, revenue);
		}
	}
	
	public static class RevenueReducer2 extends Reducer<Text, LongWritable, Text, LongWritable>{
		
		private Map<String, Long> revenueMap = new HashMap<String, Long>();
		public void reduce(Text timeBlock, Iterable<LongWritable> revenues, Context context) {
			for (LongWritable revenue: revenues) {
				revenueMap.put(timeBlock.toString(), revenue.get());
			}
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException {
			Map<String, Long> sortedMap = sort(revenueMap);
	  		  int counter = 0;
	  		  for (String key: sortedMap.keySet()) {
	  			  if (counter ++ == 24) {
	  				  break;
	  			  }
	  			  context.write(new Text(key),new LongWritable(sortedMap.get(key)));
	  		  }
		}
		
		public static Map<String, Long> sort(Map<String, Long> unsortMap) {

	        List<Map.Entry<String, Long>> list =
	                new LinkedList<Map.Entry<String, Long>>(unsortMap.entrySet());
//	        for (Map.Entry<String, Integer> entry : list) {
//	            System.out.println("-----IN COMPARATOR METHOD------"+entry.getKey()+"  "+entry.getValue());
//	        }
	        Collections.sort(list, new Comparator<Map.Entry<String, Long>>() {
	            public int compare(Map.Entry<String, Long> o1,
	                               Map.Entry<String, Long> o2) {
	            	if(!o2.getValue().equals(o1.getValue()))
	            		return (o2.getValue()).compareTo(o1.getValue());
	            	else
	            		return (o1.getKey().compareTo(o2.getKey()));
	            }
	        });

	        Map<String, Long> sortedMap = new LinkedHashMap<String, Long>();
	        for (Map.Entry<String, Long> entry : list) {
	            sortedMap.put(entry.getKey(), entry.getValue());
	        }

	        return sortedMap;
	    }
		
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if (otherArgs.length !=2) {
			System.err.println("Usage: input_file output_file");
			System.exit(2);
		}
		
		Job job1 = Job.getInstance(conf, "Time Blocks");
		job1.setJarByClass(TimeBlocks.class);
		job1.setMapperClass(TimeBlocksMapper.class);
		job1.setReducerClass(RevenueReducer.class);
		job1.setNumReduceTasks(10);
		job1.setOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]+"/temp"));
		
		if (!job1.waitForCompletion(true)) {
			  System.exit(1);
		}
		
		Job job2 = Job.getInstance(conf);
	    job2.setJarByClass(TimeBlocks.class);
	    job2.setJobName("sort");
	    job2.setNumReduceTasks(1);

	    FileInputFormat.setInputPaths(job2, new Path(otherArgs[1] + "/temp"));
	    FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1] + "/final"));

	    job2.setMapperClass(TimeBlocksMapper2.class);
	    job2.setReducerClass(RevenueReducer2.class);

	    job2.setMapOutputKeyClass(Text.class);
	    job2.setMapOutputValueClass(LongWritable.class);
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(LongWritable.class);
	    
	    if (!job2.waitForCompletion(true)) {
			  System.exit(1);
		}
	}
}

