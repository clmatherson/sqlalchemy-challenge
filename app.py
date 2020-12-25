import datetime as dt
import numpy as np
import pandas as pd

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify

# Database setup
connection_string = "sqlite:///Resources/hawaii.sqlite"
engine = create_engine(connection_string)

Base = automap_base()
Base.prepare(engine, reflect=True)

Measurement = Base.classes.measurement
Station = Base.classes.station

# Create app
app = Flask(__name__)

# define endpoint
@app.route("/")
def index():
    return """
        <h3>Climate Analysis and Exploration-Hawaii</h3>
        /api/v1.0/station<br/>
        /api/v1.0/measurement<br/>
        /api/v1.0/measurement/station<br/>
        /api/v1.0/precipitation<br/>
        /api/v1.0/most_active_station/stats_tobs<br/>
        /api/v1.0/start-date<br/>
        /api/v1.0/start-date/end-date
        """

@app.route("/api/v1.0/station")
def station():

    session = Session(engine)
    results = session.query(Station).all()
    session.close()

    stations = []
    for item in results:
        station = {
            'id': item.id,
            'station': item.station,
            'name': item.name,
            'lat': item.latitude,
            'lng': item.longitude,
            'elevation': item.elevation
        }
        stations.append(station)

    return jsonify(stations)

@app.route("/api/v1.0/measurement")
def measurement():

    session = Session(engine)
    results = session.query(Measurement).all()
    session.close()

    measurements = []
    for item in results:
        measurement = {
            'id': item.id,
            'station': item.station,
            'date': item.date,
            'prcp': item.prcp,
            'tobs': item.tobs
        }
        measurements.append(measurement)

    return jsonify(measurements)

@app.route("/api/v1.0/measurement/<station>")
def measure(station):
    session = Session(engine)
    results = session.query(Measurement).filter(Measurement.station == station).all()
    session.close()

    measurements = []
    for item in results:
        measure = {
            'id': item.id,
            'station': item.station,
            'date': item.date,
            'prcp': item.prcp,
            'tobs': item.tobs
            }
        measurements.append(measure)

    return jsonify(measurements)

# Get the Latest Date
session = Session(engine)
latest_date = session.query(func.max(Measurement.date)).first()

# Convert Day, Month and Year to interger and assign to variant
ld_year = int(latest_date[0][:4])
ld_mth = int(latest_date[0][5:7])
ld_day = int(latest_date[0][8:10])

# Pass Variants through dt.date func and calculate 'Start' and 'End' dates
rpt_end_date = dt.date(ld_year, ld_mth, ld_day)
delta = dt.timedelta(days=365)
rpt_start_date = rpt_end_date - delta
print(rpt_start_date)

@app.route("/api/v1.0/precipitation")
def precipitation():
# Query last 12 months and assign results into a Pandas DataFrame sorted by date as index
    session = Session(engine)
    twlv_months = session.query(Measurement.date, Measurement.prcp).filter(Measurement.date >= rpt_start_date).all()
    # twlv_months =  pd.DataFrame(twlv_months).fillna(0).set_index('date').sort_index()
    session.close()

    return jsonify(twlv_months)


@app.route("/api/v1.0/most_active_station/stats_tobs")
def tobs():
    session = Session(engine)
    
    selected_cols = [
        Measurement.date,
        Measurement.prcp,
        Measurement.tobs,
        Station.station,
        Station.name,
        Station.latitude,
        Station.longitude,
        Station.elevation
        ]
        
    station_observe = session.query(*selected_cols).filter(Measurement.station == Station.station).all()
    
    stat_obs=[]
    for date, prcp, tobs, station, name, lat, lng, elevation in station_observe:
        stat_dict={
            'date': date,
            'prcp': prcp,
            'tobs': tobs,
            'station': station,
            'name': name,
            'lat': lat,
            'lng': lng,
            'elevation': elevation
        }
        stat_obs.append(stat_dict)

    stat_obs = pd.DataFrame(stat_obs)
    
    active_stations = pd.Series(stat_obs['station'].value_counts())

    station_low = []
    most_active = active_stations.head(1).index.item()
    no_stations = pd.Series(stat_obs['station'].unique())

    for s in no_stations:
        station_ltemp = stat_obs.loc[stat_obs['station'] == s]
        stat_low = {
            'Station': s,
            'Lowest Temp' : station_ltemp['tobs'].min(),
            'Highest Temp' : station_ltemp['tobs'].max(),
            'Average Temp' : station_ltemp['tobs'].mean(),
            'Observations' :  station_ltemp['tobs'].count()
            }
        station_low.append(stat_low)
    
    station_low_df = pd.DataFrame(station_low)
    station_low_df.fillna(value=0, inplace=True)
    station_low_df.set_index('Station', inplace=True)
    station_low_df.sort_values('Observations', ascending=False)
    # print("Most Active Station:")
    most_active_stat = station_low_df.loc[[most_active]].to_dict()
        
    act_station_twlv_months = session.query(Measurement.station, Measurement.date, Measurement.tobs)\
                            .filter(Measurement.date >= rpt_start_date, Measurement.station == most_active)\
                            .all()
    
    act_stat_sdt = []
    for rec in act_station_twlv_months:
        stdt = {
            'station': rec.station,
            'date': rec.date,
            'tobs': rec.tobs
        }
        act_stat_sdt.append(stdt)

    # act_station_twlv_months = pd.DataFrame(act_station_twlv_months)
    # act_station_twlv_months = act_station_twlv_months.to_dict()

    session.close()

    return jsonify(most_active_stat, act_stat_sdt)

@app.route("/api/v1.0/<sdate>")
def startDate(sdate):
    session = Session(engine)
    results = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs))\
                .filter(Measurement.date >= sdate)\
                .all()
    
    summary_stats_one=[]

    summary_stats_one.append({'starting': sdate})
    summary_stats_one.append({'ending': latest_date[0]})

    for tobs_min, tobs_avg, tobs_max in results:
        stat_dict={
            'tobs_min': tobs_min,
            'tobs_avg': tobs_avg,
            'tobs_max': tobs_max,
        }
        summary_stats_one.append(stat_dict)

    session.close()

    return jsonify(summary_stats_one)

@app.route("/api/v1.0/<sdate>/<edate>")
def startDateEndDate(sdate,edate):
    session = Session(engine)
    results = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs))\
                .filter(Measurement.date >= sdate)\
                .filter(Measurement.date <= edate)\
                .all()
    
    summary_stats_two=[]

    summary_stats_two.append({'starting': sdate})
    summary_stats_two.append({'ending': edate})

    for tobs_min, tobs_avg, tobs_max in results:
        stat_dict={
            'tobs_min': tobs_min,
            'tobs_avg': tobs_avg,
            'tobs_max': tobs_max,
        }
        summary_stats_two.append(stat_dict)

    session.close()

    return jsonify(summary_stats_two)

if __name__ == '__main__':
    app.run(debug=True)