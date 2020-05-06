# File description

## urls.txt
This file contains downloadable urls for NYC taxi data from 2010 to 2015. This file could be edited according to users’ interests. 

## filter.py
Code to produce a dataset that contains taxi drop off count at the location of interest during the course of a day. 

## generate_data.ipynb
Sample process for filtering taxi data. Demonstration purpose only, does not produce any data file. 

# Dataset generation
To produce data files from raw data, please connect to NYU HPC virtual machine, and follow the instruction below:
1. ```module load python/gnu/3.6.5```
2. ```module load spark/2.4.0```
3. ```git clone https://github.com/0o0liver/Going_Back_to_Work.git```
4. ```cd Going_Back_to_Work/datasets```
5. ```wget -i urls.txt -P data```
6. ```hfs -put data```
7. ```spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python filter.py data address```, an example value for ```address``` could be ```american museum of natural history```.
