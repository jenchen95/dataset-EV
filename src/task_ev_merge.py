# Import
import polars as pl

# Join gdp and vehicle data
gdp_vehicle = (
    pl.read_csv('../data/data_import/gdp_per_cap_r10.csv')
    .filter(pl.col('year')<=2020)  # owing to no vehicle data in 2021, but gdp data exists
    .with_columns(model=pl.lit('historical'))
    .with_columns(scenario=pl.lit('historical'))
    .select(pl.col('model','scenario','region','year','gdp_per_cap','unit','population'))
    .with_columns(population=pl.col('population') * 1000)
    .cast({'population':pl.Int64})
    .with_columns(unit_pop=pl.lit('person'))
    .vstack(
        pl.read_parquet('../data/data_import/gdp_per_cap_future.parquet')
    )
    .with_columns(gdp_per_cap=pl.col('gdp_per_cap') / 10000) # dollar to 10^4 dollars
    .with_columns(unit=pl.lit('10^4 US$(2010)'))
    .join(
        pl.read_csv('../data/data_task/vehicle_per_capita_r10.csv'),
        on=['region','year'],
        how='left',
        suffix='_vehicle'
    )
    .with_columns(vehicle_per_cap=pl.col('vehicle_per_cap') / 1000) # vehicle per 1000 to vehicle per cap
    .with_columns(unit_vehicle=pl.lit('vehicle'))
    .join(
        pl.read_csv('../data/data_man/vehicle_saturation.csv'),
        on=['region'],
        how='left'
    )
    .with_columns(saturation=pl.col('saturation') / 1000)  # vehicle per 1000 to vehicle per cap
    .filter(pl.col('year')>=2005)  # earliest vehicle data from 2005
)    

gdp_vehicle.write_parquet('../data/data_task/gdp_vehicle.parquet')
