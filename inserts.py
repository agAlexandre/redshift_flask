import sqlalchemy as sa
import pandas as pd


engine=sa.create_engine('redshift+psycopg2://awsuser:2020Inmetrics@inmetrics.c7adyubj9dlv.us-east-2.redshift.amazonaws.com:5439/airport')


def insertPassenger(name_passenger,cpf_passenger,email_passenger,phone_passenger):

    query = \
        "INSERT INTO dim_passenger \
        (name_passenger,cpf_passenger,email_passenger,phone_passenger)\
        VALUES('%s','%s','%s','%s');"\
        %(name_passenger,cpf_passenger,email_passenger,phone_passenger)     
    
    print('Starting Query: ')
    with engine.begin() as conn:
        conn.execute(query)
        print('passenger successfully inserted')
    conn.close()

def insertAirReservation(number_reservation,number_armchair,date_leaving,date_back,fk_time_reservation):

    query = \
        f"INSERT INTO dim_air_reservation\
        (number_reservation ,number_armchair,date_leaving,date_back,fk_time_reservation)\
        VALUES('%s','%s','%s','%s',{fk_time_reservation});"\
        %(number_reservation,number_armchair,date_leaving,date_back)     
    
    print('Starting Query: ')
    with engine.begin() as conn:
        conn.execute(query)
        print('Air reservation successfully inserted')
    conn.close()

def insertAirline(code_airline, name_airline, email_airline):

    query = \
        "INSERT INTO dim_airline (code_airline, name_airline, email_airline)\
        VALUES('%s','%s','%s');"\
        %(code_airline, name_airline, email_airline)     
    
    print('Starting Query: ')
    with engine.begin() as conn:
        conn.execute(query)
        print('Airline successfully inserted')
    conn.close()

def insertDestinyPlace(adress_destiny,state_destiny,city_destiny):

    query = \
        "INSERT INTO dim_destiny_place (adress_destiny,state_destiny,city_destiny)\
        VALUES('%s','%s','%s');"\
        %(adress_destiny,state_destiny,city_destiny)     
    
    print('Starting Query: ')
    with engine.begin() as conn:
        conn.execute(query)
        print('Destiny place successfully inserted')
    conn.close()

def insertOriginPlace(adress_destiny,state_destiny,city_destiny):

    query = \
        "INSERT INTO dim_origin_place (adress_origin,state_origin,city_origin)\
        VALUES('%s','%s','%s');"\
        %(adress_destiny,state_destiny,city_destiny)     
    
    print('Starting Query: ')
    with engine.begin() as conn:
        conn.execute(query)
        print('Origin place successfully inserted')
    conn.close()

def insertPlane(code_plane,model_plane,acount_seats,max_seats):

    query = \
        "INSERT INTO dim_plane (code_plane,model_plane,acount_seats,max_seats)\
        VALUES('%s','%s','%s','%s');"\
        %(code_plane,model_plane,acount_seats,max_seats)     
    
    print('Starting Query: ')
    with engine.begin() as conn:
        conn.execute(query)
        print('Plane successfully inserted')
    conn.close()

def insertTimeFlight():
    query ="INSERT INTO dim_time_flight (message) VALUES('Ocorrido em: ');" 
    
    print('Starting Query: ')
    with engine.begin() as conn:
        conn.execute(query)
        print('Time Flight successfully inserted')
    conn.close()

def insertTimeReservation():
    query ="INSERT INTO dim_time_reservation (message) VALUES('Ocorrido em: ');" 
    
    print('Starting Query: ')
    with engine.begin() as conn:
        conn.execute(query)
        print('Time Reservation successfully inserted')
    conn.close()

def insertAirport(code_airport,adress_airport,phone_airport,state_airport,city_airport,zip_code_airport):

    query = \
        "INSERT INTO dim_airport (code_airport,adress_airport,phone_airport,state_airport,city_airport,zip_code_airport)\
        VALUES('%s','%s','%s','%s','%s','%s');"\
        %(code_airport,adress_airport,phone_airport,state_airport,city_airport,zip_code_airport)     
    
    print('Starting Query: ')
    with engine.begin() as conn:
        conn.execute(query)
        print('Airport successfully inserted')
    conn.close()

def insertFactFlights(code_flight,number_flight,acount_flights,tariff_flight,fk_dim_time_flight,fk_dim_airline,fk_dim_plane,fk_dim_airport,fk_dim_origin_place,fk_dim_destiny,fk_dim_air_reservation,fk_dim_passenger):
    query = \
        "INSERT INTO dim_airport (code_flight,number_flight,acount_flights,tariff_flight,fk_dim_time_flight,fk_dim_airline,fk_dim_plane,fk_dim_airport,fk_dim_origin_place,fk_dim_destiny,fk_dim_air_reservation,fk_dim_passenger)\
        VALUES\
        f({code_flight},{number_flight},{acount_flights},{tariff_flight},{fk_dim_time_flight},{fk_dim_airline},{fk_dim_plane},{fk_dim_airport},{fk_dim_origin_place},{fk_dim_destiny},{fk_dim_air_reservation},{fk_dim_passenger});"     
    
    print('Starting Query: ')
    with engine.begin() as conn:
        conn.execute(query)
        print('Facts Flights successfully inserted')
    conn.close()

    