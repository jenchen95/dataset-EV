## src
task_ev_interp.py: interpolation for vehicle and vehicle_per_cap

task_ev_merge.py: merge gdp and vehicle per cap data

task_ev_fit.py: using Gompertz model fitting for gdp and vehicle, and using population to forecast future vehicle volume

## data

### data_raw

vehicle_in_use_2015_2020: just 2015 and 2020; [Vehicles in use | www.oica.net](https://www.oica.net/category/vehicles-in-use/)

vehicle_in_use_2005_2015: from 2005 to 2015; [Number of Vehicles in Use — KAPSARC Data Portal](https://datasource.kapsarc.org/explore/dataset/number-of-vehicles-in-use/information/?disjunctive.regions_countries)

pdf: [oica.net/wp-content/uploads/Total_in-use-All-Vehicles.pdf](https://www.oica.net/wp-content/uploads/Total_in-use-All-Vehicles.pdf)

### data_import
import data from other dvc repositories
pop_medium_r10.csv: population data from dataset-population, here r10 refers to category r10, not data from ar6
gdp_per_cap_r10.csv: gdp per capita data from dataset-population, here r10 refers to category r10, not data from ar6
gdp_per_cap_future.parquet: gdp per capita data from dataset-ar6

### data_task
vehicle_in_use_r10.csv: vehicle_in_use data in AR6 r10 category
vehicle_per_capita_r10.csv: vehicle_per_capita data (caculated)
