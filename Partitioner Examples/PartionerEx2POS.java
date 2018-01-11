

	import java.io.*;

	import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;

	import org.apache.hadoop.fs.*;

	import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import org.apache.hadoop.util.*;

	public class PartionerEx2POS extends Configured implements Tool
	{
	   //Map class
		
	   public static class MapClass extends Mapper<LongWritable,Text,Text,Text>
	   {
	      public void map(LongWritable key, Text value, Context context)
	      {
	         try{
	            String[] str = value.toString().trim().split(",");
	            String ProdId =str[1].trim();
	        //    String StoreId = str[0].trim();
	  //          String Qty = str[2].trim();
	            //String salary = str[4];
	      //      String myValue = StoreId + ',' + Qty ;
	            context.write(new Text(ProdId), new Text(value));
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	   }
	   
	   //Reducer class
		
	   public static class ReduceClass extends Reducer<Text,Text,Text,IntWritable>
	   {
		   private Text outputKey = new Text();
	     
	      public void reduce(Text key, Iterable <Text> values, Context context) throws IOException, InterruptedException
	      {
	        int Quant = 0;
				
	         for (Text val : values)
	         {
	        	
      //  	outputKey.set(key);
	        	 String [] str = val.toString().trim().split(",");
	            Quant += Integer.parseInt(str[2].trim());
	            String mykey = key.toString();
            	outputKey.set(mykey);
	      //    String ProductId = str[0];
	          	
	         }
	            	
            	context.write(outputKey, new IntWritable(Quant));

	            }	
	         }
				
	        
	      
	   
	   
	   //Partitioner class
		
	   public static class CaderPartitioner extends Partitioner < Text, Text >
	   {
	      @Override
	      public int getPartition(Text key, Text value, int numReduceTasks)
	      {
	    	  String[] str = value.toString().trim().split(",");
	          int storeID = Integer.parseInt(str[0].trim());
	           
	          
	          
	          if(storeID == 21)
	          {
	             return 0;   //describes the order of the sort and shuffle to be accessed
	          }
	          else 
	          {
	             return 1 ;
	          }
	      }
	   }
	   

	   public int run(String[] arg) throws Exception
	   {
		
		   
		  Configuration conf = new Configuration();
		  Job job = Job.getInstance(conf);
		  job.setJarByClass(PartionerEx2POS.class);
		  job.setJobName("Total Qty sold for each product for each store");
	      FileInputFormat.setInputPaths(job, new Path(arg[0]));
	      FileOutputFormat.setOutputPath(job,new Path(arg[1]));
			
	      job.setMapperClass(MapClass.class);
		
	      job.setMapOutputKeyClass(Text.class);
	      job.setMapOutputValueClass(Text.class);
	      
	      //set partitioner statement
			
	      job.setPartitionerClass(CaderPartitioner.class);
	      job.setReducerClass(ReduceClass.class);
	      job.setNumReduceTasks(2);
	      job.setInputFormatClass(TextInputFormat.class);
			
	      job.setOutputFormatClass(TextOutputFormat.class);
	      job.setOutputKeyClass(Text.class);
	      job.setOutputValueClass(Text.class);
			
	      System.exit(job.waitForCompletion(true)? 0 : 1);
	      return 0;
	   }
	   
	   public static void main(String ar[]) throws Exception
	   {
	      ToolRunner.run(new Configuration(), new PartionerEx2POS(),ar);
	      System.exit(0);
	   }
	}


