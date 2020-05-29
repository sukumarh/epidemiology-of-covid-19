# epidemiology-of-covid-19
The project is aimed to study the spread of COVID-19 using Scala (Spark) on multiple time series datasets.


#### ABOUT
Analyzing the various time series data for different locations, after considering the impact of the population density and joining with datasets of external factors, we can retrieve certain factors that affect certain locations specifically.
These focused factors can then be highlighted, and this method can be used to predict which factors will likely govern the spread in locations where there is an onset of the disease.


#### ANALYSIS
To perform our analysis we used the Granger causality test which outputs a p-value that indicates a causal relationship.


#### DATASETS
| NAME                        | LINK     |
| --------------------------- | --------:|
| COVID -19 New York Times | https://github.com/nytimes/covid-19-data |
| SafeGraph Social Distancing | https://www.safegraph.com/dashboard/covid19-commerce-patterns |
| OpenSky COVID-19 Flight Dataset | https://opensky-network.org/datasets/covid-19 |
| Weather and Climate data powered by Dark Sky API | https://www.kaggle.com/vishalvjoseph/weather-dataset-for-covid19-predictions |


### Built With
* [IntelliJ IDEA 2020](https://www.jetbrains.com/idea/)
* [Apache Spark 2.3.0 - Hadoop 2.7](https://archive.apache.org/dist/spark/spark-2.4.3/)

