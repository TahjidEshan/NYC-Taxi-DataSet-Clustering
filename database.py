import pandas as pd
from sqlalchemy import create_engine

if __name__ == "__main__":
    nyc_database = create_engine('sqlite:///nyc_database_may_2019.db')

    j, chunksize = 1, 100000
    for month in range(5,6):
        fp = "nyc_yellow_cab.2019-{0:0=2d}.csv".format(month)
        for df in pd.read_csv(fp, chunksize=chunksize, iterator=True):
            df = df.rename(columns={c: c.replace(' ', '_') for c in df.columns})
            df['pickup_hour'] = [x[11:13] for x in df['tpep_pickup_datetime']]
            df['dropoff_hour'] = [x[11:13] for x in df['tpep_dropoff_datetime']]
            df.index += j
            df.to_sql('yellow_cab', nyc_database, if_exists='append')
            j = df.index[-1] + 1
    del df

    j, chunksize = 1, 100000
    for month in range(5,6):
        fp = "nyc_for_hire_vehicles.2019-{0:0=2d}.csv".format(month)
        for df in pd.read_csv(fp, chunksize=chunksize, iterator=True):
            df = df.rename(columns={c: c.replace(' ', '_') for c in df.columns})
            df['pickup_hour'] = [x[11:13] for x in df['pickup_datetime']]
            df['dropoff_hour'] = [x[11:13] for x in df['dropoff_datetime']]
            df.index += j
            df.to_sql('for_hire_vehicles', nyc_database, if_exists='append')
            j = df.index[-1] + 1
    del df