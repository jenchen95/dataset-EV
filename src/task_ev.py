# Import 
import polars as pl
import numpy as np
from scipy.interpolate import CubicSpline

# Interpolate historical data for vehicle-in-use
def fit_spline(series, years, parse=False):
    """
    Applies cubic spline interpolation to each column of the DataFrame.
    """
    df = pl.DataFrame([years,series]).filter(pl.exclude('year').is_not_null())

    # Beginning year is the first year with non-null value
    granu_years = np.arange(df['year'].item(0), years.max() + 1)

    # If parse is True, parse df's years are divisible by 10
    if parse is True:
        df = df.filter(pl.col('year') % 10 == 0)

    fit_years = df['year'].to_numpy()
    fit_values = df.select(pl.exclude('year')).to_numpy()

    spline = CubicSpline(fit_years, fit_values)
    # Predict for all years using the fitted spline interpolation
    granu_values = spline(granu_years)

    # Ensure all values are non-negative
    granu_values = np.maximum(granu_values, 0)
    granu_values = granu_values.ravel()  # Convert to 1D array

    # pad with 0s if the first year is not the beginning year
    granu_values = np.pad(granu_values, (granu_years[0] - years[0], 0), 'constant', constant_values=0)

    return granu_values

vehicle = (
    pl.read_excel('../data/data_man/vehicle_in_use_2005_2015_clean.xlsx', sheet_name='TOTAL')
    .filter(pl.col('r10_ar6').is_not_null())
    .rename({'r10_ar6': 'region'})
    .with_columns(pl.when(pl.col('2020').is_null()).then(pl.col('2015')).otherwise(pl.col('2020')).alias('2020'))
    .melt(id_vars=['country','region','unit'], variable_name='year')
    .cast({'year': pl.Int32})
)

vehicle_interp = []
for j,i in enumerate(vehicle['country'].unique()):
    country = vehicle.filter(pl.col('country') == i).select(pl.col('region','year','value','unit')).sort('year')
    region = country['region'].unique().to_list()[0]
    years = country['year']
    vehicle_in_use = country['value']
    vehicle_in_use = fit_spline(vehicle_in_use, years, parse=False)
    vehicle_interp.append(
        pl.DataFrame({'year': np.arange(years[0], years[-1]+1), 'value': vehicle_in_use})
        .with_columns(pl.lit(i).alias('country'), pl.lit(region).alias('region'), pl.lit('thousands').alias('unit'))
    )

vehicle_interp = (
    pl.concat(vehicle_interp).select(pl.col('country','region','year','value','unit'))
    .group_by(['region','year','unit'])
    .agg(pl.sum('value').alias('value'))
    .sort('region','year')
    .with_columns(value = pl.col('value') * 1000)
    .with_columns(unit = pl.lit('vehicle'))
)

# Read population data
pop = pl.read_csv('../data/data_import/pop_medium_r10.csv')

# Calculate vehicle per capita
vehicle_per_capita = (
    vehicle_interp.join(pop, on=['region', 'year'], how='inner', suffix='_pop')
    .with_columns(vehicle_per_cap = pl.col('value') / pl.col('population'))
    .with_columns(unit = pl.lit('vehicle_per_thousand'))
    .select(pl.col('region','year','vehicle_per_cap','unit'))
)

# Export
vehicle_interp.write_csv('../data/data_task/vehicle_in_use_r10.csv')
vehicle_per_capita.write_csv('../data/data_task/vehicle_per_capita_r10.csv')


