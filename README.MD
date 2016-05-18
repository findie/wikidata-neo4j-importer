# Wikidata Neo4j Importer
_Imports WikiData JSON dumps into Neo4j in a meaningful way._

The importer takes JSON dumps from [WikiData](https://dumps.wikimedia.org/wikidatawiki/entities/)
 and imports entities / properties, then generates relations between each other and claims.

## Dependencies
- [Neo4j](http://neo4j.com/) 3.0 +
- [NodeJS](https://nodejs.org) 6.0 +
- [WikiData JSON File](https://dumps.wikimedia.org/wikidatawiki/entities/)

___

## Setup
### Schema
We recommend having the following schema before starting the importer
```
Indexes
  ON :Claim(id)     ONLINE (for uniqueness constraint)
  ON :Entity(label) ONLINE
  ON :Entity(id)    ONLINE (for uniqueness constraint)
  ON :Item(id)      ONLINE (for uniqueness constraint)
  ON :Property(id)  ONLINE (for uniqueness constraint)

Constraints
  ON (item:Item) ASSERT item.id IS UNIQUE
  ON (property:Property) ASSERT property.id IS UNIQUE
  ON (entity:Entity) ASSERT entity.id IS UNIQUE
  ON (claim:Claim) ASSERT claim.id IS UNIQUE
```
### Hardware
We recommend having:
- `4+ CPU cores`
- `16+ GB RAM`
- `~200GB or storage`
    - `~30GB` for the wikidata gzip file
    - `~70GB` for the wikidata JSON file
    - `~60GB` for neo4j database
    - `~11GB` for a compressed _(tar gz)_ backup files
### Runtime
Tested on a 4 Core, 16GB RAM VPS at around 16h

### Editing the configuration file:
`config.json` - Extra details for each property below
```javascript
{
  /* the wikidata JSON file */
  "file": "./wikidata-dump.json",

  /* neo4j connection details */
  "neo4j": {
    /* bolt protocol URI */
    "bolt": "bolt://localhost",
    "auth": {
      "user": "neo4j",
      "pass": "password"
    }
  },
  /* Stages */
  "do": {
    /* database cleanup */
    "0": true,
    /* importing items and properties */
    "1": true,
    /* linking entities and generating claims */
    "2": true
  },
  /* extra console output on stage 2 */
  "verbose": false,
  /* how many commands will be ran by the DB at a given time */
  "concurrency": 4,
  /* skip lines */
  "skip": 0,
  /* count of lines */
  "lines": 21225524,
  /* bucket size of entities sent to DB to process */
  "bucket": 1000
}
```

## Stages
### Stage 0 - Database clean-up
Runs a simple command to clean the database
```cypher
MATCH (n) DETACH DELETE n
```
_Recommended: `false`_

### Stage 1 - Importing items and properties
Reads the JSON file `16MB` at a time and extracts `bucket` lines, then processes `label`,
`descriptions`, `aliases`, `id` and `type` _( in `English` if available )_

_Recommended:_
- _`true` if database is empty_
- _`false` if database is already populated with all the entities_

### Stage 2 - Linking entities and generating claims
Reads the JSON file `16MB` at a time and extracts `bucket` lines, then processes links between
entities proxied by properties

## Concurrency
The amount of running queries on the DB at a time. Note that the queries have different
complexities and running time

_Recommended: `#CPUs on the database system`_

## Skip
The number of lines skipped from the start of the file.
If something goes wrong, instead of rerunning the stage to that point in the file, it just skips
that number of lines

Since the importer is `idempotent` keeping it to 0 has no other efect than the runtime

_Recommended: `0`_

## Lines
The number of lines the `JSON` file has
If not provided, the importer will run `wc -l <filename>` to count the number of lines at runtime

_Recommended: run `wc -l <filename>` or other command yourself to count the lines_

## Bucket
The amount of lines that will be processed per query transaction. The lower the number, the more
time is wasted in I/O, the bigger the number, the more RAM will be used by both `node` and `neo4j`

_Recommended: `1000`_

___

# Structure

## Nodes
Once finished importing, the database has 2 main node labels:
- Entity
- Claim

Entity nodes split into 2 types:
- Item: items like `boat` or `car`
- Property: properties like `start time` or `location`

Claims split into multiple types:
- String
- Commonsmedia
- ExternalId
- GlobeCoordinate
- Math
- Monolingualtext
- Quantity
- Time

A list of properties can be found [here](https://www.wikidata.org/wiki/Special:ListDatatypes)

## Relations
All relations have in common the property `by` which is the `id` if the property that binds the 2
 nodes together. Some will also have an `id` to keep the `idempotence` property.

___

# License
License can be found [here](https://github.com/findie/wikidata-neo4j-importer/blob/master/LICENSE.MD)
