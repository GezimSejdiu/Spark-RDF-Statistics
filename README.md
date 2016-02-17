# Spark-RDF-Statistics
RDF-Statistics Spark Library.

## Description
RDF-Statistics Spark Library.

## Spark-RDF main application class
The main application class is `org.sansa.rdfstatistics.spark.App`.
The application requires as application arguments:

1. path to the input folder containing the RDF data as nt (e.g. `/data/input`)
2. path to the output folder to write the resulting to (e.g. `/data/output`)

All Spark workers should have access to the `/data/input` and `/data/output` directories.

## Running the application on a Spark standalone cluster

To run the application on a standalone Spark cluster

1. Setup a Spark cluster
2. Build the application with Maven

  ```
  cd /path/to/application
  mvn clean package
  ```

3. Submit the application to the Spark cluster

  ```
  spark-submit \
		--class org.sansa.rdfstatistics.spark.App \
		--master spark://spark-master:7077 \
 		C/app/application.jar \
		Cocktails /data/input /data/output  
  ```
