# Europeana Recommendations Updater

Spring-Boot2 and Spring-Batch web application for generating updates to the Recommendation Engine

The software retrieves set names and uses this to loads records (of a particular set) from a Mongo databases.
Part of the record data is then send to the Embeddings API which returns vectors. The vectors are saved into a Milvus
database.

## Prerequisites
 * Java 11
 * Maven<sup>*</sup> 
 * [Europeana parent pom](https://github.com/europeana/europeana-parent-pom)
 * Record Solr search engine
 * Record Mongo database
 * Embeddings API
 * Milvus Recommendation Engine (v2.2.x)
 * At least 90 GB free HDD space (for about 60 million records)
 
 <sup>* A Maven installation is recommended, but you could use the accompanying `mvnw` (Linux, Mac OS) or `mvnw.cmd` (Windows) 
 files instead.
 
## Run

The application is a command-line application. Either select the `RecommendUpdaterApplication` class in your IDE and 'run' it

or 

go to the application root where the pom.xml is located and excute  
`./mvnw spring-boot:run` (Linux, Mac OS) or `mvnw.cmd spring-boot:run` (Windows)

### Command-line parameters

One of the following command-line options is required:

  1. `--FULL` to start a full update covering all records
  2. `--from=<date>` to start a partial update with all records created or modified after the provided date.
      The date needs to be in ISO format, e.g. `--from=2021-10-08` or `--from=2021-10-08T12:15:00`
  3. `--sets=<setId1>,<setId2>` to start an update for one or more sets. The list of sets should be comma-separated
  4. `--setFile=<fileName>` to load a list of sets from file. Expects 1 set name or set id per line

At the moment the 4 options cannot be combined.

Optional extra command-line options are:
  * `--DELETE` to delete the existing Milvus data before starting the update
  * `--batch` to modify the batch size (overrides option in configuration file)
  * `--threads` to modify the number of threads (sets) that are updated in parallel (overrides option in configuration file)

## License

Licensed under the EUPL 1.2. For full details, see [LICENSE.md](LICENSE.md).
