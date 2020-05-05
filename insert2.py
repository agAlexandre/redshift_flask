from flask import Flask, request
import sqlalchemy as sa
import requests

def insertPassenger():
    name_passenger = request.form['name_passenger']
    cpf_passenger = request.form['cpf_passenger']
    email_passenger = request.form['email_passenger']
    phone_passenger = request.form['phone_passenger']
    engine = sa.create_engine('redshift+psycopg2://awsuser:2020Inmetrics@inmetrics.c7adyubj9dlv.us-east-2.redshift.amazonaws.com:5439/airport')
    query = \
        "INSERT INTO dim_passenger \
        (name_passenger,cpf_passenger,email_passenger,phone_passenger)\
        VALUES('%s','%s','%s','%s');"\
        %(name_passenger,cpf_passenger,email_passenger,phone_passenger) 
    with engine.begin() as conn:
        conn.execute(query)
        conn.close()
    return 'passenger successfully inserted'