# File description and dataset generation

## urls.txt
This file contains downloadable urls for NYC taxi data from 2009 to 2015. This file could be edited according to users’ interests. 

## filter.py
Code to produce a dataset that contains taxi drop off count at the location of interest during the course of a day. 

## dropoff_count_complete.csv
This dataset is produced by ```filter.py```, it contains 2 columns, ```tag``` and ```count```. 
* ```tag```: It contains the date and time range when each taxi trip ended. 
* ```count```: The number of drop off in the date and time range indicated in ```tag```.

## dropoff_count_compress.csv
This dataset is produced by ```filter.py```, it contains 2 columns, ```tag``` and ```count```. 
* ```tag```: It contains the time range when each taxi trip ended. 
* ```count```: The number of drop off in the date and time range indicated in ```tag```.


To produce data files from raw data, please connect to NYU HPC virtual machine, and follow the instruction below:
1. ```module load python/gnu/3.6.5```
2. ```module load spark/2.4.0```
3. ```git clone https://github.com/0o0liver/Going_Back_to_Work.git```
4. ```cd Going_Back_to_Work/datasets```
5. ```cat urls.txt | xargs -n 1 -P 8 wget -c -P data/```
6. ```hfs -put data```
7. ```spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python filter.py data address```, an example value for ```address``` could be ```american museum of natural history```.

Sample ```dropoff_count_complete.csv``` and ```dropoff_count_compress.csv``` are provided in current directory, it contains dropoff count at ```american museum of natural history``` during Jan 2009.
