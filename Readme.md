This repository includes source code for Cascading and Scalding extensions for Accumulo.  This work was originally 
developed by talk3.  The original code is [here](https://github.com/airawat/cascading.accumulo). I recently modified for 
my company's use cases, specifically:
- A builder pattern to establish the Accumulo connection
- Support for multiple ranges
- Better iterator support
- Support for reading offline tables
- Support for bulk ingest
- Support for scalding

It's still a work in progress I hope to add test cases and documentation in the coming weeks.

Licensing
=========
cascading.accumulo is licensed under the Apache 2.0 open source license: 
http://opensource.org/licenses/Apache-2.0.

Examples for using the Cascading extensions for Accumulo are available at-
=========================================================================== 

Currently two scalding examples exist in the scalding examples subdirectory.

To run TsvToAccumulo you must has a tsv file in hdfs of the form:
```
row    columnFamily columnQualifier value
```

then use the command:
```
hadoop jar scalding-examples-1.0-hdpjar.jar com.talk3.cascading.scalding.examples.TsvToAccumulo --hdfs --user test 
--pass test --input tsv --table test --instance test --zookeepers localhost:2181
```

NB: you'll need the table test to exist as well as the user test.  Also, test should have the authorization "public".

You can run AccumuloToTsvJob with the command
```
hadoop jar scalding-examples-1.0-hdpjar.jar com.talk3.cascading.scalding.examples.AccumuloToTsv --hdfs --user test 
--pass test --output tsv.out --table test --instance test --zookeepers localhost:2181
```
