import numpy as np
import pandas as pd
import fastavro
from river import time_series, linear_model, metrics, preprocessing, optim, compose
#import matplotlib.pyplot as plt
from sklearn.metrics import mean_squared_error, mean_absolute_error
import datetime
import pytz
import copy
import pickle

def read_avro(file_path):
    with open(file_path, 'rb') as f:
        avro_reader = fastavro.reader(f)
        records = [record for record in avro_reader]
    return pd.DataFrame(records)

def unix_to_hour_pol(time):
    poland_tz = pytz.timezone('Europe/Warsaw')
    return datetime.datetime.fromtimestamp(time, poland_tz).hour

def preprocess_data(df):
    n = df.shape[0] * 4 - 3
    df_extra = pd.DataFrame(np.nan, index=range(n), columns=df.columns)
    for i in range(df.shape[0]-1):
        df_extra.iloc[4*i] = df.iloc[i]
        diff = (df.iloc[i+1] - df.iloc[i]) / 4
        for j in range(1, 4):
            df_extra.iloc[4*i+j] = df_extra.iloc[4*i+j-1] + diff
    df_extra.iloc[-1] = df.iloc[-1]
    df_extra['dt'] = df_extra['dt'].astype(int)
    df_extra['pressure'] = df_extra['pressure'].round().astype(int)
    df_extra['humidity'] = df_extra['humidity'].round().astype(int)
    df_extra['wind_deg'] = df_extra['wind_deg'].round().astype(int)
    df_extra['clouds'] = df_extra['clouds'].round().astype(int)
    X = df_extra.drop(columns=["clouds"])
    X['dt'] = X['dt'].apply(unix_to_hour_pol)
    y = df_extra["clouds"]
    return X, y

def batch_train(X, y, p, d, q, sp, sd, sq, m, l2):

    Xtrain = X
    ytrain = y[24*4:]


    pipeline = compose.Pipeline(
        preprocessing.StandardScaler(),
        linear_model.LinearRegression(l2=l2)
    )

    model = time_series.SNARIMAX(
        p=p,
        d=d,
        q=q,
        sp=sp,
        sd=sd,
        sq=sq,
        m=m,
        regressor=pipeline,
    )

    forecasts = []
    for i in range(len(ytrain)):
        x1 = Xtrain.iloc[i].to_dict()
        
        target = ytrain.iloc[i]
        model.learn_one(target, x1)
        if i != len(ytrain)-1:
            x2 = Xtrain.iloc[i+1].to_dict()
            forecasts.append(model.forecast(1, [x2]))   

    forecasts = np.array(forecasts).flatten()
    return model, forecasts

def plot_batch_train(forecasts):
    plt.scatter(range(len(forecasts)), np.clip(forecasts, 0, 100))
    plt.show()

def train_model():
    file_path = 'weatherbatch.avro'
    df = read_avro(file_path)
    X,y = preprocess_data(df)
    p = 1
    d = 1
    q = 1
    sp = 1
    sd = 1
    sq = 1
    m = 4*24
    l2 = 1
    #model, forecasts = batch_train(X, y, p, d, q, sp, sd, sq, m, l2)
    with open('model.pkl', 'rb') as f:
        model = pickle.load(f)
    #plot_batch_train(forecasts)
    x_hist = X[-24*4:].to_dict(orient='records')
    timestamp = df['dt'].iloc[-1]
    model_data = {"model": model, "timestamp": timestamp, "x_hist": x_hist,}
    return model_data


