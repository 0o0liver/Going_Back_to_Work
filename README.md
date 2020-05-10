# Going_Back_to_Work

|      Name| NetID|
|----------|------|
| David Chu|dfc296|
|Chuhan Jin|cj1436|
|Binghan Li|bl1890|

## Summary
In this project, we aim to develop a scheduling system for building management to operate optimally and safely after the COVID-19 pandemic. We will use New York City taxi data to analyze the demand for buildings and simulate requests to test our scheduler.

## Data filtering:
We used 2010 to 2013 New York City data for demand analyzing and simulating. For the scope of this project, we need to filter out taxi data for the specific buildings of interest, we used Goldman Sachs Building for demonstration purposes. We utilized the pyspark and geopy module in the filtering process as provided below: 
* Use geopy to calculate the bounding box coordinates. 
* Load data files into pyspark dataframe. 
* Filter out records which have the dropoff coordinates outside the target bounding box. 
* Drop unnecessary columns (only keep dropoff datetime, latitude and longitude). 

Detailed implementation and demonstration is provided [here](https://github.com/0o0liver/Going_Back_to_Work/blob/master/datasets/generate_data.ipynb). Instruction on how to generate resultant data files can be found [here](https://github.com/0o0liver/Going_Back_to_Work/tree/master/datasets#dataset-generation). Resultant data for Goldman Sachs Building is provided [here](https://github.com/0o0liver/Going_Back_to_Work/tree/master/datasets/resultant_data). 

## Demand model prediction:
To better understand the demand of buildings, we used [Statsmodels Exponential Smoothing](https://www.statsmodels.org/dev/examples/notebooks/generated/exponential_smoothing.html) module with the [Holt-Winters' multiplicative method](https://orangematter.solarwinds.com/2019/12/15/holt-winters-forecasting-simplified/) to accurately reflect the demand model of buildings. We grouped all drop offs into 30-minutes-range groups with the amount of drop offs during each time group over the course of a day (sample data provided below). Then we feed the grouped data into our machine learning model to produce demand prediction, which will be used by scheduler for decision making. Detailed implementation can be found [here](). Visualized prediction is provided below.

![Imgur](https://i.imgur.com/vtX1eqK.png)

