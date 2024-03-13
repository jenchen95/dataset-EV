# Import
import polars as pl
import proplot as pplt
# Read 
data = (
    pl.scan_parquet('../data/data_task/gdp_vehicle_fitting.parquet')
)
r10 = pl.read_csv('../data/data_man/r10_list.csv')

# Plot
fig = pplt.figure()
gs = pplt.GridSpec(nrows=2, ncols=5)
for j,i in enumerate(r10['region'].to_list()):
    ax = fig.subplot(gs[j])
    data_filter = data.filter(pl.col('region') == i).collect()
    ax.scatter(data_filter['gdp_per_cap'], data_filter['vehicle_per_cap'])


fig.save('../pic/test.pdf')