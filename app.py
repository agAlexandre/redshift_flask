from flask import Flask, render_template, request
import sqlalchemy as sa
import pandas as pd
import requests


app = Flask(__name__)


@app.route('/', methods=['GET','POST'])
def home():
    return render_template("index.html")

@app.route('/passenger.html',methods=['GET','POST'])
def insertPassenger():
    if request.method == 'POST':
        name_passenger = request.form['name_passenger']
        cpf_passenger = request.form['cpf_passenger']
        email_passenger = request.form['email_passenger']
        phone_passenger = request.form['phone_passenger']
        engine = sa.create_engine('redshift+psycopg2://awsuser:2020Inmetrics@inmetrics.c7adyubj9dlv.us-east-2.redshift.amazonaws.com:5439/airport')
        query = \
        "INSERT INTO dim_passenger \
        (name_passenger,cpf_passenger,email_passenger,phone_passenger)\
        VALUES('%s','%s','%s','%s')"\
        %(name_passenger,cpf_passenger,email_passenger,phone_passenger) 
        with engine.begin() as conn:
            conn.execute(query)
        conn.close()
    return render_template("passenger.html")

@app.route('/reservation.html',methods=['GET','POST'])
def insertAirReservation():
    if request.method == 'POST':
        number_reservation = request.form['number_reservation']
        number_armchair = request.form['number_armchair']
        date_leaving = request.form['date_leaving']
        date_back = request.form['date_back']
        engine = sa.create_engine('redshift+psycopg2://awsuser:2020Inmetrics@inmetrics.c7adyubj9dlv.us-east-2.redshift.amazonaws.com:5439/airport')
        query ="START TRANSACTION;\
           INSERT INTO dim_air_reservation (number_reservation,number_armchair ,date_leaving,date_back,reservation_created_at )\
	        VALUES('%s','%s','%s','%s',CURRENT_TIMESTAMP);\
           INSERT INTO dim_time_reservation (reservation_created_at) \
                VALUES (CURRENT_TIMESTAMP);\
           COMMIT;"%(number_reservation,number_armchair,date_leaving,date_back) 
        with engine.begin() as conn:
            conn.execute(query)
        conn.close()
    return render_template("reservation.html")

@app.route('/airline.html',methods=['GET','POST'])
def insertAirline():
    if request.method == 'POST':
        code_airline = request.form['code_airline']
        name_airline = request.form['name_airline']
        email_airline = request.form['email_airline']
        engine = sa.create_engine('redshift+psycopg2://awsuser:2020Inmetrics@inmetrics.c7adyubj9dlv.us-east-2.redshift.amazonaws.com:5439/airport')
        query = \
        "INSERT INTO dim_airline (code_airline, name_airline, email_airline)\
        VALUES('%s','%s','%s');"\
        %(code_airline, name_airline, email_airline) 
        with engine.begin() as conn:
            conn.execute(query)
        conn.close()
    return render_template("airline.html")

@app.route('/airport.html',methods=['GET','POST'])
def insertAirport():
    if request.method == 'POST':
        #name_airport = request.form['name_airport']
        code_airport = request.form['code_airport']
        adress_airport = request.form['adress_airport']
        phone_airport = request.form['phone_airport']
        state_airport = request.form['state_airport']
        city_airport = request.form['city_airport']
        zip_code_airport= request.form['zip_code_airport']
        engine = sa.create_engine('redshift+psycopg2://awsuser:2020Inmetrics@inmetrics.c7adyubj9dlv.us-east-2.redshift.amazonaws.com:5439/airport')
        query = \
        "INSERT INTO dim_airport (code_airport,adress_airport,phone_airport,state_airport,city_airport,zip_code_airport)\
        VALUES('%s','%s','%s','%s','%s','%s');"\
        %(code_airport,adress_airport,phone_airport,state_airport,city_airport,zip_code_airport)      
        with engine.begin() as conn:
            conn.execute(query)
        conn.close()
    return render_template("airport.html")


@app.route('/dstPlace.html',methods=['GET','POST'])
def insertDstPlace():
    if request.method == 'POST':
        adress_destiny = request.form['adress_destiny']
        state_destiny = request.form['state_destiny']
        city_destiny = request.form['city_destiny']
        engine = sa.create_engine('redshift+psycopg2://awsuser:2020Inmetrics@inmetrics.c7adyubj9dlv.us-east-2.redshift.amazonaws.com:5439/airport')
        query = \
        "INSERT INTO dim_destiny_place (adress_destiny,state_destiny,city_destiny)\
        VALUES('%s','%s','%s');"\
        %(adress_destiny,state_destiny,city_destiny)  
        with engine.begin() as conn:
            conn.execute(query)
        conn.close()
    return render_template("dstPlace.html")

@app.route('/orgPlace.html',methods=['GET','POST'])
def insertOrgPlace():
    if request.method == 'POST':
        adress_origin = request.form['adress_origin']
        state_origin = request.form['state_origin']
        city_origin = request.form['city_origin']
        engine = sa.create_engine('redshift+psycopg2://awsuser:2020Inmetrics@inmetrics.c7adyubj9dlv.us-east-2.redshift.amazonaws.com:5439/airport')
        query = \
        "INSERT INTO dim_origin_place (adress_origin,state_origin,city_origin)\
        VALUES('%s','%s','%s');"\
        %(adress_origin,state_origin,city_origin)   
        with engine.begin() as conn:
            conn.execute(query)
        conn.close()
    return render_template("orgPlace.html")

@app.route('/plane.html',methods=['GET','POST'])
def insertPlane():
    if request.method == 'POST':
        code_plane = request.form['code_plane']
        model_plane = request.form['model_plane']
        acount_seats = request.form['acount_seats']
        max_seats = request.form['max_seats']
        engine = sa.create_engine('redshift+psycopg2://awsuser:2020Inmetrics@inmetrics.c7adyubj9dlv.us-east-2.redshift.amazonaws.com:5439/airport')
        query = \
        "INSERT INTO dim_plane (code_plane,model_plane,acount_seats,max_seats)\
        VALUES('%s','%s','%s','%s');"\
        %(code_plane,model_plane,acount_seats,max_seats)  
        with engine.begin() as conn:
            conn.execute(query)
        conn.close()
    return render_template("plane.html")

@app.route('/takeoff.html', methods=['GET','POST'])
def insertTakeOff():
    if request.method == 'POST':
        code_flight = request.form['code_flight']
        number_flight = request.form['number_flight']
        tariff_flight = request.form['tariff_flight']
        engine = sa.create_engine('redshift+psycopg2://awsuser:2020Inmetrics@inmetrics.c7adyubj9dlv.us-east-2.redshift.amazonaws.com:5439/airport')
        query = f"START TRANSACTION;\
		INSERT INTO dim_time_flight (flight_created_at) VALUES (CURRENT_TIMESTAMP);\
		INSERT INTO fact_flights(code_flight,\
             		acount_flights,\
            		tariff_flight ,\
            		number_flight ,\
            		fk_dim_air_reservation,\
            		fk_dim_airline,\
           		fk_dim_airport,\
            		fk_dim_destiny,\
            		fk_dim_origin_place,\
            		fk_dim_passenger,\
            		fk_dim_plane,\
            		fk_dim_time_flight,\
            		flight_created_at)\
        	SELECT {code_flight},\
            		COUNT(DISTINCT number_flight ),\
            		{tariff_flight},\
            		{number_flight},\
            		MAX(sk_dim_air_reservation),\
            		MAX(sk_dim_airline),\
            		MAX(sk_dim_airport),\
            		MAX(sk_dim_destiny_place),\
           		MAX(sk_dim_origin_place),\
            		MAX(sk_dim_passenger),\
            		MAX(sk_dim_plane),\
            		MAX(sk_dim_time_flight),\
            		CURRENT_TIMESTAMP\
        	FROM fact_flights,\
            		dim_time_flight,\
            		dim_air_reservation,\
            		dim_airline,\
            		dim_airport,\
            		dim_destiny_place,\
            		dim_origin_place,\
            		dim_passenger,\
            		dim_plane \
		COMMIT; "
        with engine.begin() as conn:
            conn.execute(query)
        conn.close()
    return render_template('takeoff.html')
