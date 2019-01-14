package com.revature.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

//for each row:
//	test type, raw score, score, test period, test catagory, builder id, group id, group type, battery id, battery status
//battery_status: dropped = 1, employed = 2, training = 3 (ignore others)
//Test type: verbal = 1, exam = 2, project = 3 (ignore others)

//"_c#" refer to the column names- the default names provided by spark
public class VerbalTestIndicator {
	
	//input:  table, 
	//public AnalyticResult execute(Dataset<Row> csv, Row inputRow, int period) {
	public AnalyticResult execute(Dataset<Row> csv, int batteryID, int period) {
		
		double score, scoreLowerBound, scoreUpperBound, outputPercentage = 0.0;
		long totalAmount, failedAmount;
		
		//filter by where testType = 1 for a specific period, and where the battery status is either 1 or 2
		csv = csv.filter("_c0 = 1 AND _c3 = "+period+" AND (_c9 = 1 OR _c9 = 2)");
		//score = Double.parseDouble(inputRow.get(2).toString());
		try {
			score = csv.filter("_c8 = "+batteryID).first().getDouble(2);
		}
		catch(Exception e) {
			return null;
		}
		scoreLowerBound = score-10;
		scoreUpperBound = score+10;
		
		//filter where score is within bounds
		Dataset<Row> csvTotal = csv.filter("_c2 >= " + scoreLowerBound + " AND _c2 <= " + scoreUpperBound);
		//group by battery id, count the distinct number of batteries
		totalAmount = csvTotal.groupBy("_c8").count().distinct().count();
		//filter where status is 1, group by battery id, count the number of distinct batteries with a status of 1
		failedAmount = csvTotal.filter("_c9 = 1").groupBy("_c8").count().distinct().count();
		if (totalAmount!=0)
			outputPercentage = (double)failedAmount/(double)totalAmount*100;
		
		return new AnalyticResult(outputPercentage, (int)totalAmount, "Result is based on those who scored "
				+ "similarly(+/- 10) on verbal tests taken in period " + period);
	}
}
