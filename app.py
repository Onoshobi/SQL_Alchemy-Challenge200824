
# Import the dependencies.
# Python SQL toolkit and Object Relational Mapper
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

#Importing pandas
import pandas as pd

#if you don't have flask install it using "pip install Flask"
#Importing the Flask
from flask import Flask, render_template
#Converting the dictionary object to json format
from flask import jsonify

# for logging in the terminal if anything goes wrong
import logging

#################################################
# Database Setup
#################################################

# Create engine using the `hawaii.sqlite` database file
engine = create_engine("sqlite:///Resources/hawaii.sqlite")
engine.connect()
# Declare a Base using `automap_base()`
Base = automap_base()
# Use the Base class to reflect the database tables
Base.prepare(autoload_with=engine)

# Assign the measurement class to a variable called `Measurement` and
Measurement = Base.classes.measurement
# the station class to a variable called `Station`
Station = Base.classes.station

# Create a session
session = Session(engine)

#################################################
# Flask Setup
#################################################
app = Flask(__name__)

#################################################
# Flask Routes
#################################################

#displaying the home page with all the APIS
@app.route('/')
def home_page():
    return render_template('index.html')

###################################################################
#query to retrieve the last 12 months of precipitation data. 
def get_precipitation_data_from_last_12_months():
    # Find the most recent date in the data set.
    most_recent_date = session.query(func.max(Measurement.date)).scalar()
    # query to retrieve the last 12 months of precipitation data and plot the results.
    one_year_ago = pd.to_datetime(most_recent_date) - pd.DateOffset(years=1)
    one_year_ago_str = one_year_ago.strftime('%Y-%m-%d')
    #print(f"Date one year ago from the most recent date is: {one_year_ago_str}")

    # Perform a query to retrieve the data and precipitation scores for the last 12 months
    precipitation_data = session.query(Measurement.date, Measurement.prcp).filter(Measurement.date >= one_year_ago_str).all()
    #Storing the data as dictionary by iterating over each. Where  key-> date and value->precipitation 
    #creating the dictionary
    precipitation_dict = {}
    for each in precipitation_data:
        precipitation_dict[each[0]] = each[1]

    #formating the dictionary to json format.
    resp = jsonify(precipitation_dict)
    
    return resp

@app.route('/api/v1.0/precipitation')
def precipitation():
    return get_precipitation_data_from_last_12_months()

######################################################################################
def get_all_station():
    
    station_name_rows_list = []
    #Here we are using station as primary key from Station table to find the matching relationship with the Measurement Table

    #Getting all the station
    for each_station in session.query(Station).all():
    #printing all the variables from each station
    #print(vars(each_station))
    #getting station name for each station
    #print(each_station.station)

        matched_measurements = session.query(Measurement).filter(Measurement.station == each_station.station).all()
        station_name_rows_list.append((each_station.station, len(matched_measurements)))
    return station_name_rows_list



@app.route('/api/v1.0/station')
def station():
    station_name_rows_list = get_all_station()
    #converting the station name and rows to dictionary key value pair
    station_name_rows_dict =  {station_name_row[0]:station_name_row[1] for station_name_row in station_name_rows_list}
    return jsonify(station_name_rows_dict)


########################################################################

def get_most_active_stations_last_year_data():
    #Getting all the stations
    station_name_row_list = get_all_station()
    #Now creating the dataframe    
    station_name_rows_dataframe = pd.DataFrame(station_name_row_list,columns=("station","Number of Rows"))
    sorted_station_name_rows_dataframe = station_name_rows_dataframe.sort_values("Number of Rows",ascending=False)
    
    #Getting the first row as it is the most active station
    most_active_station = sorted_station_name_rows_dataframe.station.iloc[0]
    most_recent_date = session.query(func.max(Measurement.date)).scalar()
    one_year_ago = pd.to_datetime(most_recent_date) - pd.DateOffset(years=1)
    one_year_ago_str = one_year_ago.strftime('%Y-%m-%d')


    # Perform a query to retrieve 12 months of temperature observation data for most active station 
    temperature_data_for_most_active_station_12_months = session.query(Measurement.date,Measurement.tobs).filter((Measurement.station==most_active_station) & (Measurement.date >= one_year_ago_str)).all()
    
    #app.logger.warning('%s is the most active station', most_active_station)

    
    #Making the dictionary from the list to jsonfiy later.
    date_temp_dict = {each_date_temp[0]:each_date_temp[1] for each_date_temp in temperature_data_for_most_active_station_12_months}



    return date_temp_dict


@app.route('/api/v1.0/tobs')
def tobs():
    return jsonify(get_most_active_stations_last_year_data())






############################################################################################
#This will take the date,temperature sublist and returns the min,max,avg temperature.
def get_max_min_avg_dic_from_temperature_list(date_temperature_list):
    temperature_only = [data[1] for data in date_temperature_list]

    dict_min_max_avg_temp = {}
    dict_min_max_avg_temp['maximum temperature'] = max(temperature_only)
    dict_min_max_avg_temp['minimum temperature'] = min(temperature_only)
    dict_min_max_avg_temp['avgerage temperature'] = sum(temperature_only)/len(temperature_only)
    return dict_min_max_avg_temp


def get_temp_from_start_date_to_end_date(start_date=None,end_date=None):
    
    try:
        date_temperature_data_for_start_date_to_end_of_db=None
        
        if end_date==None:
            # Getting all the dates,temperature from start date to end of the date present in the database
            #@app.route('/api/v1.0/start/<start_date>') this will be called
            date_temperature_data_for_start_date_to_end_of_db = session.query(Measurement.date,Measurement.tobs).filter((Measurement.date >= start_date)).all()
        else:
            # Getting all the dates,temperature from start date to end date provided by the parameter.
            #@app.route('/api/v1.0/<start_date>/<end_date>') this will be called
            date_temperature_data_for_start_date_to_end_of_db = session.query(Measurement.date,Measurement.tobs).filter((Measurement.date >= start_date) & (Measurement.date<=end_date)).all()
        #log to verify the dates
        app.logger.warning(date_temperature_data_for_start_date_to_end_of_db)
        dict_min_max_avg_temp =  get_max_min_avg_dic_from_temperature_list(date_temperature_data_for_start_date_to_end_of_db)

    except Exception as e:
        return "Somthing is wrong ! please check the date format."
    return dict_min_max_avg_temp

###################################################################################
#Start Date to end of the db dates avg,max and min temperature finder.
@app.route('/api/v1.0/start/<start_date>')
def get_from_start_date(start_date):
    temperature_list_from_start_date_to_end_date = get_temp_from_start_date_to_end_date(start_date=start_date)
    return jsonify(temperature_list_from_start_date_to_end_date)

#Just shows the message how to pass the parameter
@app.route('/api/v1.0/start')
def start_date_message():
    return "<h1> Please enter the date on the url like api/v1.0/start/yyyy-mm-dd format.</h1>"

#########################################
#Start Date to end date provided as url parameter finding avg,max and min temperature.
@app.route('/api/v1.0/<start_date>/<end_date>')
def get_start_end_date_data(start_date=None,end_date=None):
    dict_min_max_avg_temp= get_temp_from_start_date_to_end_date(start_date=start_date,end_date=end_date)
    return jsonify(dict_min_max_avg_temp)


#Just shows the message how to pass the parameter
@app.route('/api/v1.0/start/end')
def get_from_start_end_date():
    return "<h1> Please enter the date on the url like api/v1.0/yyyy-mm-dd/yyyy-mm-dd format.</h1>"

#Running the flask App
#To run the flask app do this "flask --app app_name  run" where app_name is app.py in our program.
if __name__ == '__main__':
    app.run(debug=True)