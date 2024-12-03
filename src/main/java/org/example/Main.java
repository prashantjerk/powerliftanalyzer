package org.example;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;

public class Main {
    public static void main(String[] args) throws FileNotFoundException {
        // THIS IS IN CASE YOU WANT TO WRITE THE LOGS TO THE LOCAL FILE

//        String localOutputFile = "src/main/resources/result.txt";
//        PrintStream out = new PrintStream(new File(localOutputFile));
//        System.setOut(out);

        SparkSession sparkSession = SparkSession.builder()
                .appName("Power Lift Analyzer")
                .master("yarn")
                .config("spark.submit.deployMode", "cluster")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
                .getOrCreate();

        // s3 input paths
        String meetPath = "s3a://powerlift/inputs/meets.csv";
        String openPowerLiftingPath = "s3a://powerlift/inputs/openpowerlifting.csv";

        // s3 output path
        String s3resultPath = "s3a://powerlift/output/result.txt";

        String result = "";
        try {
            JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());

            result += "ANALYSIS 1: Total Meets Per Federation";
            result += meetByFederation(sc, meetPath);
            result += "\n";

            result += "ANALYSIS 2: Average Performance by Equipment Type";
            result += averageByEquipment(sc, openPowerLiftingPath);
            result += "\n";

            result += "ANALYSIS 3: Performance Comparison/Average By States";
            result += performanceByState(sc, meetPath, openPowerLiftingPath);

            // upload the output file to s3
            writeToS3(s3resultPath, result);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private static String meetByFederation(JavaSparkContext sc, String meetPath) {
        JavaRDD<String> lines = sc.textFile(meetPath);

        JavaPairRDD<String, Integer> federationCountMap = lines.filter(line -> !line.startsWith("MeetID"))
                .map(line -> line.split(","))
                .filter(columns -> columns.length > 2 && !columns[2].isEmpty())
                .mapToPair(columns -> new Tuple2<>(columns[2], 1)) // Count each federation
                .reduceByKey(Integer::sum)
                .sortByKey();

        StringBuilder resultBuilder = new StringBuilder();
        federationCountMap.collect().forEach(entry ->
                resultBuilder.append(entry._1).append(": ").append(entry._2).append("\n")
        );

        return resultBuilder.toString();
    }


    private static String averageByEquipment(JavaSparkContext sc, String openPowerLiftingPath) {
        JavaRDD<String> lines = sc.textFile(openPowerLiftingPath);

        // equipment count
        JavaPairRDD<String, Integer> equipmentCountMap = lines.filter(line -> !line.startsWith("MeetID"))
                .map(line -> line.split(","))
                .filter(columns -> columns.length > 3 && !columns[3].isEmpty())
                .mapToPair(columns -> new Tuple2<>(columns[3], 1))
                .reduceByKey(Integer::sum)
                .sortByKey();

        // total number of records
        long totalRecords = equipmentCountMap.values().reduce(Integer::sum);

        // StringBuilder to construct the result string
        StringBuilder resultBuilder = new StringBuilder();
        equipmentCountMap.collect().forEach(entry -> {
            double average = (double) entry._2 / totalRecords;
            resultBuilder.append(entry._1).append(": ").append(String.format("%.2f", average)).append("\n");
        });

        return resultBuilder.toString();
    }

    private static String performanceByState(JavaSparkContext sc, String meetPath, String openPowerLiftingPath) {
        JavaRDD<String> meetRDD = sc.textFile(meetPath);
        JavaRDD<String> openPowerLiftingRDD = sc.textFile(openPowerLiftingPath);

        // Map MeetID to State
        JavaPairRDD<String, String> idStateMap = meetRDD.filter(line -> !line.startsWith("MeetID"))
                .map(line -> line.split(","))
                .filter(columns -> columns.length > 5 && !columns[0].isEmpty() && !columns[5].isEmpty())
                .mapToPair(columns -> new Tuple2<>(columns[0], columns[5]));

        // Map MeetID to TotalKg
        JavaPairRDD<String, Double> idTotalKgMap = openPowerLiftingRDD.filter(line -> !line.startsWith("MeetID"))
                .map(line -> line.split(","))
                .filter(columns -> columns.length > 14 && !columns[0].isEmpty() && !columns[14].isEmpty())
                .mapToPair(columns -> new Tuple2<>(columns[0], Double.parseDouble(columns[14])));

        // Join the two RDDs on MeetID
        JavaPairRDD<String, Tuple2<String, Double>> joinedRDD = idStateMap.join(idTotalKgMap);

        // Aggregate TotalKg and count per state
        JavaPairRDD<String, Tuple2<Double, Integer>> stateAggregates = joinedRDD
                .mapToPair(entry -> new Tuple2<>(entry._2._1, new Tuple2<>(entry._2._2, 1))) // (State, (TotalKg, Count))
                .reduceByKey((a, b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2)) // Sum TotalKg and counts per state
                .sortByKey();

        // Use StringBuilder to construct the result
        StringBuilder resultBuilder = new StringBuilder();
        stateAggregates.collect().forEach(entry -> {
            String state = entry._1;
            double totalKg = entry._2._1;
            int count = entry._2._2;
            double averageTotalKg = totalKg / count;
            resultBuilder.append(state).append(": ").append(String.format("%.2f", averageTotalKg)).append("\n");
        });

        return resultBuilder.toString(); // Return the constructed result as a string
    }

    private static void writeToS3(String s3resultPath, String result) throws IOException, URISyntaxException {
        FileSystem fs = FileSystem.get(new java.net.URI(s3resultPath), new org.apache.hadoop.conf.Configuration());

        // Create an output stream to the S3 path
        Path path = new Path(s3resultPath);
        if (fs.exists(path)) {
            fs.delete(path, true);  // Delete if it already exists
        }

        try (FSDataOutputStream outputStream = fs.create(path)) {
            outputStream.write(result.getBytes());
        } finally {
            fs.close();
        }
    }
}