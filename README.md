## DDP Ingest

This is a prototype pipeline to prove out loading DDP data to TDR

### Status: WIP


### Building the Schema
Schemas are stored in `/schema/src/main/jade-tables`. Each table is specified in it's own schema file, i.e, `ddp_biosample.table.json`. 
We are using [this table specification syntax](https://github.com/DataBiosphere/ingest-utils/tree/master/ingest-sbt-plugins#table-specification-syntax). It
is specific to the monster tooling we're using to generate the lower-level BigQuery schema defintition.

Once table schemas are in place, the dataset schema can be generated using our scala tool. Run the schema generation tool like so:
1. [Install sbt](https://www.scala-sbt.org/download.html)
2. From the project root: `sbt generateJadeSchema`
3. The schema will be placed in `schema/target/schema.json`. This directory is under source control so we can track dataset schema changes. Placing a `target`
directory under source control is not a great practice, but the output path is hardcoded in our schema generation tooling. Until we implement a change to 
   parameterize this, we'll track the schema this way.
   

### Creating a Dataset
Submit the schema created above as your schema field when creating a dataset with the Jade `createDataset` (endpoint)[https://data.terra.bio/swagger-ui.html#/repository/createDataset].

This is an example payload for the endpoint:

```
{
    "name": <dataset_name>,
    "description": <description>,
    "defaultProfileId": <billing_profile_id>,
    "region": "us-central1",
    "cloudPlatform": "gcp",
    "schema": <schema from above>
}
```