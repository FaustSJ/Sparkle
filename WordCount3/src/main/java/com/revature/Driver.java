package com.revature;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.revature.spark.WordCount;

public class Driver {
	public static void main(String args[]) {
		if (args.length != 4) {
			System.out.println("WordCount usage <spark master> <input file> "
					+ "<output dir> <min word count>");
			System.exit(-1);
		}
		
		final String SPARK_MASTER = args[0];
		final String INPUT_PATH = args[1];
		final String OUTPUT_PATH = args[2];
		final Integer MIN_COUNT = Integer.parseInt(args[3]);
		
		/*
		 * Set Spark configuration for Context
		 */
		
		SparkConf conf = new SparkConf()
				.setAppName("Word Count").setMaster(SPARK_MASTER);
		JavaSparkContext context = new JavaSparkContext(conf);
		
		/*
		 * Run the WordCount Spark Logic
		 */
		new WordCount().execute(context, INPUT_PATH, OUTPUT_PATH,MIN_COUNT);
	}
}
