# Import
import polars as pl

# Join gdp and vehicle data
gdp_vehicle = (
    pl.read_csv('../data/data_import/gdp_per_cap_r10.csv')
    .filter(pl.col('year')<=2020)  # owing to no vehicle data in 2021, but gdp data exists
    .with_columns(model=pl.lit('historical'))
    .with_columns(scenario=pl.lit('historical'))
    .with_columns(unit=pl.lit('US$(2010)'))
    .select(pl.col('model','scenario','region','year','gdp_per_cap','unit'))
    .vstack(
        pl.read_parquet('../data/data_import/gdp_per_cap_future.parquet')
    )
    .join(
        pl.read_csv('../data/data_task/vehicle_per_capita_r10.csv'),
        on=['region','year'],
        how='left',
        suffix='_vehicle'
    )
    .join(
        pl.read_csv('../data/data_man/vehicle_saturation.csv'),
        on=['region'],
        how='left'
    )
    .filter(pl.col('year')>=2005)  # earliest vehicle data from 2005
)    

gdp_vehicle.write_parquet('../data/data_task/gdp_vehicle.parquet')
