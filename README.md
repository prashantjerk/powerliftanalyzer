# PowerLift Analyzer Application

This project demonstrates the use of Apache Spark and Hadoop to process and analyze powerlifting datasets. The application performs various operations, such as federation-based statistics, performance analysis by equipment type, and state-wise performance comparison. The results are stored in an AWS S3 bucket.

## Features

1. **Total Meets Per Federation**: Calculates the total number of meets organized by each federation.
2. **Average Performance by Equipment Type**: Computes the average performance for each equipment type.
3. **Performance Comparison by State**: Analyzes and compares average performance across states.

## Prerequisites

To run this project, you need the following:

1. **Java Development Kit (JDK)**: Version 11 or later.
2. **Apache Maven**: To manage dependencies and build the project.
3. **Apache Spark**: Version 3.5.1.
4. **Hadoop (with Yarn)**: Version 3.3.6 or compatible.
5. **AWS S3 Access**: Proper AWS credentials configured for accessing S3 buckets.
6. **Cluster Environment**: Spark and Hadoop cluster setup with Yarn.

## Dataset

- **Meets**: `meets.csv` containing meet details like federation and state.
- **OpenPowerLifting**: `openpowerlifting.csv` containing lifter performance data.

These files should be stored in an AWS S3 bucket accessible to the application.

## Project Structure

- **Main.java**: Entry point of the application.
- **Pom.xml**: Maven configuration file for managing dependencies.

## Dependencies

The key dependencies used in this project are:

- **Apache Spark Core**: For distributed data processing.
- **Apache Spark SQL**: For querying structured data.
- **Apache Hadoop Common**: For integration with Hadoop.
- **Hadoop Yarn Client**: For deploying on Yarn clusters.

Refer to the `pom.xml` file for a complete list of dependencies.

## How to Build and Run

### 1. Clone the Repository
```bash
git clone https://github.com/prashantjerk/powerliftanalyzer.git
cd PowerLiftAnalyzer
```

### 2. Configure AWS Credentials
Ensure AWS credentials are properly configured on the system running this application.

### 3. Build the Project
```bash
mvn clean package
```

### 4. Run the Application
Submit the Spark job using the following command:
```bash
spark-submit \
  --class org.example.Main \
  --master yarn \
  --deploy-mode cluster \
  PowerLiftAnalyzer-1.0-SNAPSHOT.jar
```

### 5. Output

Results will be written to the specified S3 output path (e.g., `s3a://powerlift/output/result.txt`).

## Functions Overview

### `meetByFederation`
Counts the number of meets organized by each federation by filtering `meets.csv`.

### `averageByEquipment`
Calculates the average performance for each equipment type using `openpowerlifting.csv`.

### `performanceByState`
Analyzes performance by state by joining data from `meets.csv` and `openpowerlifting.csv`.

### `writeToS3`
Writes the analysis results to an S3 bucket.

## Error Handling
- Logs are printed to the console for debugging.
- Data validation ensures only valid records are processed.

## License
This project is licensed under the MIT License. See the LICENSE file for details.

## Acknowledgements
- **Apache Spark** for distributed data processing.
- **AWS S3** for cloud-based storage.
- **OpenPowerLifting** for the data used in this project.

## Author
[Prashant Karki](https://github.com/prashantjerk)

