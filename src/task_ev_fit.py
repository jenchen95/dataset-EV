import numpy as np
import polars as pl
from scipy.optimize import curve_fit

# 步骤1: 定义模型函数
def model(pgdp, alpha, beta, s):
    return s * np.exp(alpha * np.exp(beta * pgdp))


data = pl.scan_parquet('../data/data_task/gdp_vehicle.parquet')
r10_list = pl.read_csv('../data/data_man/r10_list.csv')

data_concat = []
# 步骤2: 拟合模型
# 这里假设你已经有了实际的PGDP数据(pgdp_data)和对应的x{i, t}数据(x_data)
for k in r10_list['region'].to_list():
    data_hist = (
        data.filter(
        (pl.col('model')=='historical') &
        (pl.col('scenario')=='historical') &
        (pl.col('region')==k))
        .sort('year')
        .collect()
    )

    saturation = data_hist.select(pl.col('saturation')).to_numpy().ravel()[0] 
    pgdp_data = data_hist.select(pl.col('gdp_per_cap')).to_numpy().ravel() # 实际的PGDP数据
    pvehicle_data = data_hist.select(pl.col('vehicle_per_cap')).to_numpy().ravel()  # 对应的x{i, t}数据
    alpha = -5.58

    # 使用curve_fit拟合模型
    params, cov = curve_fit(lambda pgdp, beta: model(pgdp, alpha, beta, saturation),
                             pgdp_data, pvehicle_data, bounds=([-np.inf], [0]))

    # 打印拟合得到的参数
    beta = params[0]
    print("拟合参数: beta =", params[0])
    data_hist = (
        data_hist
        .with_columns(vehicle=(pl.col('population') * pl.col('vehicle_per_cap')).round(0))
        .with_columns(alpha=alpha, beta=beta)
        .lazy()
    )
    data_concat.append(data_hist)
# 步骤3: 进行预测
# 假设你有未来的PGDP数据future_pgdp_data
    for i,j in data.filter(pl.col('model')!='historical').select(pl.col('model','scenario')).unique().collect().rows():
        data_forecast = (
            data.filter(pl.col('model')==i)
            .filter(pl.col('scenario')==j)
            .filter(pl.col('region')==k)
            .filter(pl.col('year')>=2020)
            .sort('year')
            .with_columns(vehicle_per_cap=model(pl.col('gdp_per_cap'), alpha, beta, saturation))
            .with_columns(vehicle=(pl.col('population') * pl.col('vehicle_per_cap')).round(0))
            .with_columns(alpha=alpha, beta=beta)
            )
        data_concat.append(data_forecast)

# 步骤4：合并
data_concat = pl.concat(data_concat)

# 步骤5：Join EV普及率
data_concat = (
    data_concat.join(
        pl.scan_csv('../data/data_task/EV_penetration_rate_interp.csv'),
        on = ['region', 'year'],
        how = 'left',
        suffix = '_ev_rate'
    )
    .with_columns(ev=(pl.col('vehicle') * pl.col('ev_rate') / 100).round(0))
)

data_concat.sink_parquet('../data/data_task/gdp_vehicle_fitting.parquet')