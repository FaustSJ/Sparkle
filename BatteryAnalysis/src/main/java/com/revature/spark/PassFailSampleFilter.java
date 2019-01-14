package com.revature.spark;

import java.util.List;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
/*
 * This class filters the given dataset to only include batteries with a definitive pass/fail status.
 * The output will have batteries that:
 * 	A) Have a battery_status of 2
 * 	B) Have a battery_status of 1 AND feature a test_period of 3 or higher
 * 	C) Have a battery_status of 3 AND feature a test_period of 9 or higher
 */

//for each row:
//	test type, raw score, score, test period, test catagory, builder id, group id, group type, battery id, battery status
//battery_status: dropped = 1, employed = 2, training = 3 (ignore others)

//"_c#" refer to the column names- the default names provided by spark
public class PassFailSampleFilter {
	
	//input:  table, 
	public FilterResult execute(Dataset<Row> csv, String outputPath) {
		
		
		//Get a list of battery_ids that match the criteria
		//String[] col = {"_c8"};
		Dataset<Row> filteredCSV = csv.filter("_c9 = 1 OR (_c9 = 2 AND _c3 = 3) OR (_c9 = 3 AND (_c3 = 9 OR _c3 = 10))")
				.drop("_c0", "_c1","_c2","_c3","_c4","_c5","_c6","_c7","_c9").dropDuplicates();
		//		.dropDuplicates(col);
		//filteredCSV.javaRDD();
		
		List<Row> battery_IDs = filteredCSV.collectAsList();
		StringBuilder listOfIDs = new StringBuilder("");
		for(Row r:battery_IDs) {
			listOfIDs.append(" "+r.mkString());
		}
		String batteryList = listOfIDs.toString();
		
		StringBuilder wtf = new StringBuilder("");
		csv = csv.filter((FilterFunction<Row>)row -> {
			return batteryList.contains(row.get(8).toString());
		});
		
		//Column batteries = filteredCSV.col("_c8");
		//batteries.toString();
		csv.javaRDD().saveAsTextFile(outputPath);
		
		return new FilterResult(listOfIDs.toString());
	}
}
