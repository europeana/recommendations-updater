# Europeana Recommendations Updater

Spring-Boot2 and Spring-Batch web application for generating updates to the Recommendation Engine

The software retrieves set names from Solr and uses this to loads records (of a particular set) from a Mongo databases.
Part of the record data is then sends it to the Embeddings API which returns vectors. The vectors are saved into a Milvus
database. Since the used Milvus version doesn't support string keys, we store a mapping between recordIds and keys
in a local LMDB database.

## Prerequisites
 * Java 11
 * Maven<sup>*</sup> 
 * [Europeana parent pom](https://github.com/europeana/europeana-parent-pom)
 * Record Solr search engine
 * Record Mongo database
 * Embeddings API
 * Milvus Recommendation Engine
 
 <sup>* A Maven installation is recommended, but you could use the accompanying `mvnw` (Linux, Mac OS) or `mvnw.cmd` (Windows) 
 files instead.
 
## Run

The application is a command-line application. Either select the `RecommendUpdaterApplication` class in your IDE and 'run' it

or 

go to the application root where the pom.xml is located and excute  
`./mvnw spring-boot:run` (Linux, Mac OS) or `mvnw.cmd spring-boot:run` (Windows)

### Command-line parameters

The command-line option `--FULL` is required (to start a full update covering all records) or the option `--from=<date>`
should be provided to do a partial update with all records created or modified after the provided date.
The date needs to be in ISO format, e.g. `--from=2021-10-08` or `--from=2021-10-08T12:15:00`

Optionally the `--DELETE` option can be supplied which will delete existing Milvus (and Lmdb) data before
starting the update.

## License

Licensed under the EUPL 1.2. For full details, see [LICENSE.md](LICENSE.md).
