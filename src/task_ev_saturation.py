import polars as pl

saturation_ev = (
    pl.scan_parquet('../data/data_import/trp_ele_future.parquet')
    .filter(pl.col('year')==2100)
    .select(pl.col('category','model','scenario','region','trp_ele_ev'))  # unit: EJ
    .join(
        pl.scan_parquet('../data/data_import/gdp_per_cap_future.parquet')
        .filter(pl.col('year')==2100)
        .select(pl.col('model','scenario','region','population')),  # population: person
        on=['model','scenario','region'],
    )
    .join(
        pl.scan_csv('../data/data_man/EV_penetration_rate.csv')
        .select(pl.col('region','2100')),
        on='region',
    )
    .with_columns(
        ev=pl.col('trp_ele_ev') * 2.778E11 / 365 / 15,
    )  # EJ to kwh, 365 days, 15 kwh per day
    .with_columns(
        ev_per_cap=pl.col('ev') / pl.col('population') * 1000,  # vehicle per 1000 person
    )
    .with_columns(
        ev_per_cap_by_region = pl.median('ev_per_cap').over(['category','region']),
    )
    .with_columns(
        vehicle_per_cap_by_region = (pl.col('ev_per_cap_by_region') / (pl.col('2100') / 100)).cast(pl.Int32),
    )
    .sort('region','category')
)

(
    saturation_ev.select(pl.col('category','region','vehicle_per_cap_by_region'))
    .rename({'vehicle_per_cap_by_region':'saturation'})
    .unique().sort('category','region')
    .collect()
    .write_csv('../data/data_man/vehicle_saturation.csv')
)