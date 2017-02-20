package com.spark.experiments;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * @author JD
 *
 *         For each incoming csv data file, we are storing name of columns and
 *         their data-types in json file. Everytime we get csv file on s3
 *         landing layer,we validate its schema against predefined schema in
 *         json file. Compare no of columns,column names,column sequence and
 *         data-types of two data-sets
 */

public class SchemaValidator {

	private static final Logger LOGGER = LoggerFactory.getLogger(SchemaValidator.class);
	
	public static void main(String[] args) {
		// initializing spark context
		JavaSparkContext javaSparkContext = initializeSparkContext();

		// comparing schema of csv file with predefined json file
		validateSchema(javaSparkContext, args);
	}

	/**
	 * Creates spark context with s3 configuration
	 * 
	 * @return
	 */
	private static JavaSparkContext initializeSparkContext() {
		
		LOGGER.info("Initializing spark context");
		final SparkConf sparkConf = new SparkConf().setAppName("Spark")
				.setMaster("local[*]");
		final JavaSparkContext javaSparkContext = new JavaSparkContext(
				sparkConf);
		return javaSparkContext;
	}

	/**
	 * This method compares schema of csv data file against its predefined json
	 * file schema
	 * 
	 * @param javaSparkContext
	 * @param args
	 */
	private static void validateSchema(JavaSparkContext javaSparkContext,
			String[] args) {
		
		LOGGER.info("Example: json s3 Directory path =bucketName/src/folder1/ and csv s3 directory path=bucketName/src/folder2/");
		
		if (args.length != 2) {
			throw new IllegalArgumentException(
					"Validate schema method takes only two args : "
					+ "json s3 Directory path(bucketName/src/folder1/) and csv s3 directory path(bucketName/src/folder2/)");
		}
		
		final String jsonirectory = args[0];
		final String csvDirectory = args[1];

		LOGGER.info("Initializing spark-sql context");
		final SQLContext sqlContext = new SQLContext(javaSparkContext);

		// reading all input json files from s3
		final Map<String, StructType> jsonSchemaMap = new HashMap<String, StructType>();
		DataFrame jsonDf = null;
		
		List<String> jsonFiles = listS3Files(jsonirectory);
		for (int i = 0; i < jsonFiles.size(); i++) {
			final String file = jsonFiles.get(i);
			jsonDf = sqlContext.jsonFile(file);
			jsonDf.persist();
			jsonDf.show();
			final String fileName = file.substring(file.lastIndexOf("/")+1);
			final String name = fileName.substring(0, fileName.indexOf("."));

			jsonSchemaMap.put(name, jsonDf.schema());
		}

		
		/* We can persist jsonSchemaMap to hdfs/s3 for first time and then read
		 * same schema object(StructType) from next time onwards to compare with
		 * incoming csv files schema object. This way we will evaluate schema
		 * from json files only for first time and will use same hdfs schema
		 * object for future schema comparisions.
		 * 
		 * TO-DO
		 * */
		 

		// reading all input csv files from s3
		DataFrame csvDf = null;
		List<String> csvFiles = listS3Files(csvDirectory);
		for (int i = 0; i < csvFiles.size(); i++) {
			final String file = csvFiles.get(i);
			csvDf = sqlContext.read().format("com.databricks.spark.csv")
					.option("header", "true").option("inferSchema", "true")
					.load(file.toString());
			csvDf.persist();
			csvDf.show();

			final String fileName = file.substring(file.lastIndexOf("/")+1);
			final String csvName = fileName.substring(0, fileName.indexOf("."));

			final StructType jsonSchema = jsonSchemaMap.get(csvName);
			final StructType csvSchema = csvDf.schema();

			// no of columns check
			compareNoOfColumns(jsonSchema, csvSchema);

			// check both column names, column sequence and their data-types
			compareColumns(jsonSchema, csvSchema);

		}

	}

	/**
	 * This method retrives all files from all nested folders inside given
	 * directory path
	 * 
	 * @param directoryName
	 */
	private static List<String> listS3Files(String directoryName)
	{
		 LOGGER.info("Connecting to amazon s3");
	     AWSCredentials credentials = new BasicAWSCredentials("Value of awsAccessKeyId","Value of awsSecretAccessKey");
	     AmazonS3 s3Client = new AmazonS3Client(credentials);
	     
	     String bucketName = directoryName.substring(0, directoryName.indexOf("/")-1);
	     String prefix = directoryName.substring(directoryName.indexOf("/")+1);
		 LOGGER.info("Connecting to given amazon s3 directory path");
	     final ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(bucketName)
	                .withPrefix(prefix);
	     
	     LOGGER.info("Listing objects under given amazon s3 directory path");
	     ObjectListing objects = s3Client.listObjects(listObjectsRequest);
	     final List<S3ObjectSummary> keyList = objects.getObjectSummaries();
	     List<String> list = new ArrayList<String>();
	     for (S3ObjectSummary s3ObjectSummary : keyList) 
	     {
	     list.add("s3a://"+s3ObjectSummary.getKey());
	     }
	     return list;
	}

	/**
	 * This method compares no of columns in source and target datasets
	 * 
	 * @param jsonSchema
	 * @param csvSchema
	 */
	private static void compareNoOfColumns(StructType jsonSchema,
			StructType csvSchema) {
		String[] jsonFieldNames = jsonSchema.fieldNames();
		String[] csvFieldNames = csvSchema.fieldNames();
		LOGGER.info("Comparing no of columns in source and target");
		if (jsonFieldNames.length != csvFieldNames.length) {
			throw new IllegalArgumentException(
					String.format(
							"No. of columns does not match."
									+ "No. of columns in source are '%s' while no of columns in target are '%s'. ",
							new Object[] { csvFieldNames.length,
									csvFieldNames.length }));
		}

	}

	/**
	 * This method checks column names and column sequences in two data-sets are
	 * equal or not
	 * 
	 * @param jsonSchema
	 * @param csvSchema
	 */
	private static void compareColumns(StructType jsonSchema,
			StructType csvSchema) {
		String[] jsonFields = jsonSchema.fieldNames();
		List<String> jsonFieldNames = Arrays.asList(jsonFields);
		toLowerCase(jsonFieldNames);

		LOGGER.info("Comparing column names and column sequences in source and target");
		StructField[] csvFields = csvSchema.fields();
		for (int i = 0; i < csvFields.length; i++) {

			// column names check
			StructField sf = csvFields[i];
			String columnName = sf.name();
			if (!jsonFieldNames.contains(columnName.toLowerCase())) {
				throw new IllegalArgumentException(
						String.format(
								"the column '%s' in target was not found in source. The column names need to be the same. ",
								new Object[] { columnName }));

			}

			// column sequence check
			if (columnName.equalsIgnoreCase(jsonFields[jsonSchema
					.fieldIndex(columnName)])) {
				throw new IllegalArgumentException(
						String.format(
								"The sequence of column '%s' in target is '%s' and in source it's '%s'. "
										+ "Both should be same. ",
								new Object[] {
										columnName,
										csvSchema.getFieldIndex(columnName),
										jsonFields[jsonSchema
												.fieldIndex(columnName)] }));
			}

		}
	}

	/**
	 * This method converts all column names in list into lower-case
	 * 
	 * @param list
	 */
	private static void toLowerCase(List<String> list) {
		ListIterator<String> listIterator = list.listIterator();
		while (listIterator.hasNext())
			listIterator.set(((String) listIterator.next()).toLowerCase());
	}
}
