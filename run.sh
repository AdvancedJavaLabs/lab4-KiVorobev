#!/bin/bash

rm -rf ./calculated_dir
rm -rf ./result_dir

hadoop jar target/sales-analyzer-1.0-SNAPSHOT.jar org.itmo.SalesAnalyzer
