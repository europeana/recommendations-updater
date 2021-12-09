# Europeana Recommendations Updater

Spring-Boot2 and Spring-Batch web application for generating updates to the Recommendation Engine

The software loads data from a record Mongo databases, extracts record data and sends it to the
Embeddings API. The Embeddings API returns vectors which are then saved into a Milvus cluster

## Prerequisites
 * Java 11
 * Maven<sup>*</sup> 
 * [Europeana parent pom](https://github.com/europeana/europeana-parent-pom)
 * Record Mongo database
 * Embeddings API
 * Milvus Recommendation Engine
 
 <sup>* A Maven installation is recommended, but you could use the accompanying `mvnw` (Linux, Mac OS) or `mvnw.cmd` (Windows) 
 files instead.
 
## Run

The application has a Tomcat web server that is embedded in Spring-Boot.

Either select the `RecommendUpdaterApplication` class in your IDE and 'run' it

or 

go to the application root where the pom.xml is located and excute  
`./mvnw spring-boot:run` (Linux, Mac OS) or `mvnw.cmd spring-boot:run` (Windows)

### Command-line parameters

Either the command-line option `--ALL` needs to be provided to start a full update covering all records
or the option `--from=<date>` to do a partial update with all records created or modified after the provided date.
The date needs to be in ISO format, e.g. `--from=2021-10-08` or `--from=2021-10-08T12:15:00`

## License

Licensed under the EUPL 1.2. For full details, see [LICENSE.md](LICENSE.md).
