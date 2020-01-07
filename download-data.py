import pandas as pd
import numpy as np
import urllib.request
import zipfile
import random
import itertools
import os.path


def savetocsv(df, cabdata):
    if cabdata:
        path = 'C:/Users/Tahjid Ashfaque/Documents/Fall 19/697/yellow_cab.csv'
    else:
        path = 'C:/Users/Tahjid Ashfaque/Documents/Fall 19/697/for_hire.csv'
    f = open(path, 'a+')
    df.to_csv(f, index=None, header=not os.path.exists(path))
    f.close()


def main():
    for month in range(1, 6):
        urllib.request.urlretrieve("https://s3.amazonaws.com/nyc-tlc/trip+data/" + \
                                   "yellow_tripdata_2019-{0:0=2d}.csv".format(month),
                                   "nyc_yellow_cab.2019-{0:0=2d}.csv".format(month))
        urllib.request.urlretrieve("https://s3.amazonaws.com/nyc-tlc/trip+data/" + \
                                   "fhv_tripdata_2019-{0:0=2d}.csv".format(month),
                                   "nyc_for_hire_vehicles.2019-{0:0=2d}.csv".format(month))

    # Download the location Data
    urllib.request.urlretrieve("https://s3.amazonaws.com/nyc-tlc/misc/taxi_zones.zip", "taxi_zones.zip")
    with zipfile.ZipFile("taxi_zones.zip", "r") as zip_ref:
        zip_ref.extractall("./shape")

    j, chunksize = 1, 100000
    for month in range(1, 6):
        fp = "nyc_yellow_cab.2019-{0:0=2d}.csv".format(month)
        for df in pd.read_csv(fp, chunksize=chunksize, iterator=True):
            df = df.rename(columns={c: c.replace(' ', '_') for c in df.columns})
            df['pickup_hour'] = [x[11:13] for x in df['tpep_pickup_datetime']]
            df['dropoff_hour'] = [x[11:13] for x in df['tpep_dropoff_datetime']]
            df['type'] = 'taxi'
            df.index += j
            savetocsv(df, cabdata=True)
            j = df.index[-1] + 1
    del df

    j, chunksize = 1, 100000
    for month in range(1, 6):
        fp = "nyc_for_hire_vehicles.2019-{0:0=2d}.csv".format(month)
        for df in pd.read_csv(fp, chunksize=chunksize, iterator=True):
            df = df.rename(columns={c: c.replace(' ', '_') for c in df.columns})
            df['pickup_hour'] = [x[11:13] for x in df['pickup_datetime']]
            df['dropoff_hour'] = [x[11:13] for x in df['dropoff_datetime']]
            df['type'] = 'fhv'
            df.index += j
            savetocsv(df, cabdata=False)
            j = df.index[-1] + 1
    del df


if __name__ == "__main__":
    main()
