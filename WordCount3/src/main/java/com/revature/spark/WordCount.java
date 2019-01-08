package com.revature.spark;

import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class WordCount {
	
	/*
	 * Just like any Hadoop job, unless you change the output folder's name every time, you'll
	 * need to delete the previous run's output folder before every run.
	 * 
	 * You'll need http://public-repo-1.hortonworks.com/hdp-win-alpha/winutils.exe in order
	 * to run this in Windows.
	 */
	
	public void execute(JavaSparkContext context, String inputPath, String outputPath, Integer minCount) {
		/*
		 * Load file content in memory
		 */
		JavaRDD<String> lines = context.textFile(inputPath);
		
		JavaRDD<String> words = lines.flatMap((line) -> {
			return Arrays.asList(line.split("\\W+")).iterator();
		});
		/*
		 * Pair the words with a one to be reduced later
		 */
		JavaPairRDD<String, Integer> count = words.mapToPair((word) -> {
			return new Tuple2<String,Integer>(word,1);
		});

        JavaPairRDD<String, Integer> wordCount = count.reduceByKey(Integer::sum);

        /*
         * Remove words that have less than the min count.
         */
        JavaPairRDD<String, Integer> filtered = wordCount.filter((countTuple) -> {
            return countTuple._2 >= minCount;
        });
        
        /*
         * sortByKey(true)  ->  output is sorted in ascending order
         * sortByKey(false) ->  output is sorted in descending order
         */
        JavaPairRDD<String, Integer> sorted = filtered.sortByKey(false);
		
        /*
         * Store as a file
         * 
         * Use collect() to get a collection, or take(limit) to get partial rows
         */
		sorted.saveAsTextFile(outputPath);
		
		//sorted.collection
		//sorted.first
		//sorted.take
	}
}
