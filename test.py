from inserts import *
'''
insertPassenger('Alexandre','00000000000','ale@mail.com','(11)99709-9227')

insertAirline ('2003','InAirlines','in@airline.com')

insertAirport ('2003','Inmetrics','(11)1111-1111','SP','Barueri','00000-000')

insertTimeReservation()

insertAirReservation ('1','ARM99','2020-01-14:09:00:00','NULL',1)

insertDestinyPlace ('Av. Tambore','SP','Barueri')

insertOriginPlace ('R. Marieta','SP','SP')

insertPlane ('2000','boeing747','600','605')

insertTimeFlight()

insertFactFlights (1,1,1,20.4,1,1,1,1,1,1,1,1)
'''
def selectTest():
    query = \
        "SELECT* FROM dim_passenger;"     
    
    print('Starting Query: ')
    with engine.begin() as conn:
        result=conn.execute(query)
        print(result)
    conn.close()
selectTest()