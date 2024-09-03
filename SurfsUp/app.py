# Import the dependencies.
import numpy as np
import pandas as pd
import datetime as dt

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, text, inspect

from flask import Flask, jsonify


#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///C:/Users/molly/OneDrive/Data Analytics Bootcamp/sqlalchemy-challenge/Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()

# reflect the tables
Base.prepare(autoload_with=engine)

# Save references to each table
measurement = Base.classes.measurement
station = Base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)


#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################

# API SQLite Connection & Landing Page
@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation/enter_date/<enter_date><br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/tobs/start_date/<start_date><br/>"
        f"/api/v1.0/tobs/start_date/<start_date>/end_date/<end_date><br/><br/>"
        f"Enter dates in the following format: 2017-08-23"
    )

# Precipitation Route
@app.route("/api/v1.0/precipitation/enter_date/<enter_date>")
def precipitation(enter_date):
    """Returns jsonified precipitation data for the last year in the database using date as the key"""

    # Get the date a year ago
    past_year = dt.datetime.strptime(enter_date, '%Y-%m-%d') - dt.timedelta(days=365)

    # Get all precipitation data for all dates
    prcp_data = session.query(measurement.date, measurement.prcp).\
    filter(measurement.date >= past_year, measurement.date <= enter_date).all()

    # Convert query results to a dictionary
    prcp_dict = {date: prcp for date, prcp in prcp_data}

    return jsonify(prcp_dict)


# Stations Route
@app.route("/api/v1.0/stations")
def stations():
    """Returns jsonified data of all of the stations in the database"""

    # Get all stations
    all_stations = session.query(station.station).all()

    # Convert to a list
    station_names = [row[0] for row in all_stations]

    return jsonify(station_names)


# TOBS Route
@app.route("/api/v1.0/tobs")
def tobs():
    """Returns jsonified data for the most active station"""
    
    # Get most recent date and calculate the latest date
    latest_date = session.query(measurement.date).order_by(measurement.date.desc()).first()[0]
    past_year = dt.datetime.strptime(latest_date, '%Y-%m-%d') - dt.timedelta(days=365)

    # Get the most active station ID
    most_active_station = session.query(measurement.station, func.count(measurement.station)).\
        group_by(measurement.station).order_by(func.count(measurement.station).desc()).first()[0]

    # Query to get temperature observations data and station information for the most active station
    most_active_data = session.query(measurement.date, measurement.tobs, station.name, station.latitude, station.longitude, station.elevation).\
        filter(measurement.station == most_active_station).\
        filter(measurement.date >= past_year, measurement.date <= latest_date).\
        join(station, measurement.station == station.station).all()
    
    # Convert query results to a list of dictionaries
    station_data = [{"date": row[0], "temperature": row[1], "name": row[2], "latitude": row[3], "longitude": row[4], "elevation": row[5]} for row in most_active_data]

    return jsonify(station_data)


# Start Route
@app.route("/api/v1.0/tobs/start_date/<start_date>")
def tobs_start(start_date):
    """Returns the min, max, and average temperatures calculated from the given start date to the end of the dataset"""

    # Get the min, max, and average from the start date to end of dataset
    tobs_stdt = session.query(func.min(measurement.tobs), func.max(measurement.tobs), func.avg(measurement.tobs)).\
        filter(measurement.date >= start_date).all()

    # Convert query results to a list of dictionaries
    start_date_data = [{"min_temp": row[0], "max_temp": row[1], "avg_temp": row[2]} for row in tobs_stdt]

    return jsonify(start_date_data)


# Start/End Route
@app.route("/api/v1.0/tobs/start_date/<start_date>/end_date/<end_date>")
def tobs_start_end(start_date, end_date):
    """Returns the min, max, and average temperatures calculated from the given start date to the given end date"""

    # Get the min, max, and average for the duration of start date to end date
    tobs_stdt_edt = session.query(func.min(measurement.tobs), func.max(measurement.tobs), func.avg(measurement.tobs)).\
        filter(measurement.date >= start_date, measurement.date <= end_date).all()

    # Convert query results to a list of dictionaries
    start_end_data = [{"min_temp": row[0], "max_temp": row[1], "avg_temp": row[2]} for row in tobs_stdt_edt]

    return jsonify(start_end_data)


if __name__ == '__main__':
    app.run(debug=True)