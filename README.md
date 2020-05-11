# Going_Back_to_Work

|      Name| NetID|
|----------|------|
| David Chu|dfc296|
|Chuhan Jin|cj1436|
|Binghan Li|bl1890|

## Summary
In this project, we aim to develop a scheduling system for building management to operate optimally and safely after the COVID-19 pandemic. We will use New York City taxi data to analyze the demand for buildings and simulate requests to test our scheduler.

## Assumptions

We simplified the problem signifigantly in the following ways:
- We don't take into account whether users will schedule another request if denied
- We don't take into account whether users may or may not show up
- We don't take into account different priority classes of user 

## Data filtering:
We used 2010 to 2013 New York City data for demand analyzing and simulating. For the scope of this project, we need to filter out taxi data for the specific buildings of interest, we used Goldman Sachs Building for demonstration purposes. We utilized the pyspark and geopy module in the filtering process as provided below: 
* Use geopy to calculate the bounding box coordinates. 
* Load data files into pyspark dataframe. 
* Filter out records which have the dropoff coordinates outside the target bounding box. 
* Drop unnecessary columns (only keep dropoff datetime, latitude and longitude). 

Detailed implementation and demonstration is provided [here](https://github.com/0o0liver/Going_Back_to_Work/blob/master/datasets/generate_data.ipynb). Instruction on how to generate resultant data files can be found [here](https://github.com/0o0liver/Going_Back_to_Work/tree/master/datasets#dataset-generation). Resultant data for Goldman Sachs Building is provided [here](https://github.com/0o0liver/Going_Back_to_Work/tree/master/datasets/resultant_data). 

## Demand model prediction:
To better understand the demand of buildings, we used [Statsmodels Exponential Smoothing](https://www.statsmodels.org/dev/examples/notebooks/generated/exponential_smoothing.html) module with the [Holt-Winters' multiplicative method](https://orangematter.solarwinds.com/2019/12/15/holt-winters-forecasting-simplified/) to accurately reflect the demand model of buildings. We grouped all drop offs into 30-minutes-range groups with the amount of drop offs during each time group over the course of a day (sample data provided below). Then we feed the grouped data into our machine learning model to produce demand prediction, which will be used by scheduler for decision making. Visualized prediction is provided below.

![Imgur](https://i.imgur.com/vtX1eqK.png)


## Scheduling strategy 1:
Our first strategy is a simple first come first serve strategy. There is no optimization, we simply accept requests as they come in until they no longer fit under the threshold.

### Results:

![Imgur](https://i.imgur.com/GXwZioG.png)

## Scheduling strategy 2:
The optimal policy for this scheduler is to approve as many requests as possible while under the safety threshold, which is determined by the building owner and passed in to the scheduler as a parameter. Other than the threshold parameter, the predicted demand model is passed in to the scheduler as well. The scheduler also has internal representation of the current state of the building. All of these attributes are used for the decision making process.

Users can send in requests by specify the desired arrival time and length of stay (number of 30-minutes block), the scheduler then validate the request using two factors:
* Availability: Simply check if the requested time slots still have space for one more person in the building, while the number of people in the building during these time slots are still under the safety threshold. 
* Priority: This factor enforces the optimal policy of this scheduler, which is approving as many requests as possible. To do so, we make sure that multiple short requests are prioritized over one long request when there is little space left for requested time slots. This is achieved by checking with the internal demand model and this policy is activated for time slots that have high demands (predicted number of requests is higher than the threshold).

### Results:

![Imgur](https://i.imgur.com/9qX98wl.png)

## Scheduling Strategy 3:

For this strategy we use a Greedy Hill Climbing algorithm to try and estimate the optimal policy based on our prediction of requests. We iterate over our predicted schedule, removing the appointment that has the largest number of time blocks over the threshold. We iterate until all time blocks are under the threshold. We then make approval and rejection decisions based on whether or not the requests match those in our optimal policy.

### Results:

![Imgur](https://i.imgur.com/NAEWr2F.png)

## Conclusion

We've simplified our problem signifigantly, and opted for simple optimization strategies. As a result, we didn't see too much difference in terms of which strategy we used, though Strategy 2 seemed to be the best for maximizing utilization, while Strategy 3 seemed better for acceptance rate. 

## Next Steps

There are a lot of things we could improve in our algorithms given more time. 
- All of our algorithms sidestep using Expected values by simply using discrete values in place of probabilities. A smarter algorithm would use the probabilities directly to maximize the reward function.
- Increase the complexity of our model by including things such as # of times a user would attempt to request a new time slot, priorities of users, account for no shows, etc...
- Look for a DP solution. We originally thought of this as a DP problem, but were not able to figure out what the states would be or what the recurrence would look like. However, it still seems like there might be a DP solution.
