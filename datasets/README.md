# File description and dataset generation

## urls.txt
This file contains downloadable urls for NYC taxi data from 2009 to 2015. This file could be edited according to users’ interests. 

## filter.py
Code to produce a dataset that contains taxi drop off count at the location of interest during the course of a day. 

## datetime_count.csv
This dataset is produced by ```filter.py```, it contains 2 columns, ```Datetime_tag``` and ```count```. 
* ```Datetime_tag```: It contains the date and time range when each taxi trip ended. 
* ```Count```: The number of drop off in the date and time range indicated in ```Datetime_tag```.


To generate the desired data file, one must have access to NYU HPC, sample data file is provided to download here if NYU HPC is not accessible. To produce data file from raw data, please connect to you HPC virtual machine, and follow the instruction below:
1. ```module load python/gnu/3.6.5```
2. ```module load spark/2.4.0```
3. ```git clone https://github.com/0o0liver/Going_Back_to_Work.git```
4. ```cd Going_Back_to_Work/datasets```
5. ```cat urls.txt | xargs -n 1 -P 8 wget -c -P data/```
6. ```hfs -put data```
7. ```spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python filter.py data address```, an example value for ```address``` could be ```american museum of natural history```.
