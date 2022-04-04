# QuantumDB with PROV-IDEA Approach

QuantumDB is a method for evolving database schemas of SQL databases, while not impeding access to SQL clients or
requiring downtime. PROV-IDEA is presented as a PROV-Interoperable Database Evolution Approach aimed at integratin provenance
information into relational database schema evolution, using PROV as provenance framework.

In this repository we present our approach for making the QuantumDB tool provenance-aware by applying PROV-IDEA

For more information, please checkout the link:

* [PROV-IDEA. Supplementary material. (2022)](https://zenodo.org/record/6380743#.YjtpyerMKUk)

## Setup environment

For testing QuantumDB with PROV-IDEA you'll want to:


      * Have a PostgreSQL database server running to test and run against. PostgreSQL is used by QuantumDB as database to be evolved. It should be running 
      on the default  URL postgres://localhost:5432. A database <database> has to be created.
      
      *  Have a MongoDB database server running to test and run against. MongoDB is used by PROV-IDEA as database to store provenance data. It should be running 
      on the default  URL mongodb://localhost:27017, and with a 'PROVIDEA' database and a 'Evolution_Changes' collection.

### Building

To run the Maven build, do:

   mvn clean install -Dmaven.test.skip=true

### Running

To test QuantumDB with PROV-IDEA we suggest using the QuantumDB CLI tool (recommended and easiest option to manage databases with QuantumDB). For this task, we suggest executing class Cli in package io.quantumdb.cli of quantumdb-cli module. The changelog.xml file is under quantumdb-cli module, which includes a copy table change named 'changes', aimed at creating a new table, named 'technician', as a copy of the columns and data of a table called 'employee' (so, table 'employee' must exist in database <database>).

Use the followings commands:

- init --host=localhost:5432 --database=<database> --username=<username> --password=<password>
    
- changelog

- fork changes

