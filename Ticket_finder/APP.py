import psycopg2
from flask import Flask, render_template, url_for, request, jsonify
from amadeus import Client
import pandas as pd


uneseno = []
init = 0

app = Flask(__name__)
amadeus = Client(
    client_id='CkXlqhQJEBsoMsGLTuUu5dDLVXAiEQtt',
    client_secret='ecuB67LC1CGKBJDI'
)
df = pd.DataFrame()

def get_db_connection():
    conn = psycopg2.connect(host='localhost',
                            database='proba',
                            user='postgres',
                            password='Xeixoodi4050')
    return conn

@app.route('/map')
def root():
    bonds_tmp = []
    cnt = 0
    
    for iata, latlon in df_near.itertuples(index=False):
        if cnt < 100:
            bonds_tmp.append({'lat': eval(latlon)[0], 'lon': eval(latlon)[1], 'popup': f'IATA:{iata}'})
            cnt += 1
    result = bonds_tmp

    return render_template('map.html',markers=result )

@app.route('/', methods=['GET', 'POST'])
def home():
    global polazni_map
    global lat
    global long
    global df
    global init
    global df_near
    global uneseno

    polazni_map = ""
    dif2 = 'none'
    diuf2 = 'none'
    dit1 = 'none'
    dif1 = 'block'
    dif3 = 'none'
    dif4 = 'none'
    Polazni_aerodrom = ''
    Odredisni_aerodrom = ''
    Broj_odraslih_putnika = ''
    Datum_polaska = ''
    Datum_povratka = ''
    response = ''
    podaci = []
    cijene = []
    p_aer = []
    d_aer = []
    d_pol = []
    br_ad = []
    presj = []
    d_dol = []
    d_pov = []
    valuta = []
    v_pol = []
    v_dol = []
    v_pov = []
    lat_lon_near_name = []
    lat_lon_near_latlon = []

    cnt = 0
    lock = 0

    df = pd.read_csv('IATA_CODES.txt', usecols = ['iata','airport','latitude','longitude'])
    if init == 0:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""CREATE table IF NOT EXISTS letovi2(
                        id SERIAL NOT NULL PRIMARY KEY,
                        pol_aer VARCHAR(50) NOT NULL,
                        odr_aer VARCHAR(50) NOT NULL,
                        dat_pol DATE NOT NULL,
                        dat_dol DATE NOT NULL,
                        dat_pov DATE NOT NULL,
                        vri_pol TIME NOT NULL,
                        vri_dol TIME NOT NULL,
                        vri_pov TIME NOT NULL,
                        bro_pre SMALLINT NOT NULL,
                        bro_put SMALLINT NOT NULL,
                        val VARCHAR(5) NOT NULL,
                        uku_cij FLOAT NOT NULL,
                        loc POINT NOT NULL
                        )
                    """)
        cur.execute("""CREATE table IF NOT EXISTS iata_tab(
                        id SERIAL NOT NULL PRIMARY KEY,
                        iata VARCHAR(10) NOT NULL,
                        airport VARCHAR(100) NOT NULL,
                        loc POINT NOT NULL
                        )
                        
                    """)
        cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
        cur.execute("ALTER TABLE iata_tab ADD COLUMN IF NOT EXISTS geom geometry(Point, 4326);")
        postgres_insert_query = """INSERT INTO iata_tab(iata,airport,loc) VALUES(%s,%s,%s) ON CONFLICT DO NOTHING;"""                    
        for index, row in df.iterrows():
            record_to_insert = (row['iata'],row['airport'],f"({float(row['latitude'])},{float(row['longitude'])})")
    
            cur.execute(postgres_insert_query, record_to_insert)
            if cnt < 1:
                lat = float(row['latitude'])
                long = float(row['longitude'])
            cnt += 1
        cur.execute("UPDATE iata_tab SET geom = ST_SetSRID(ST_MakePoint(loc[1], loc[0]), 4326);")
        conn.commit()
        cur.close()
        conn.close()
        init = 1                             

    if request.method == 'POST':
                if request.form.get('action1') == 'Pretraži letove':
                    dif1 = 'none'
                    dif2 = 'block'                    
                    dif3 = 'none'
                    dif4 = 'none'
                if request.form.get('submit_button') == 'submit':
                    dif3 = 'block'
                    dif4 = 'block'

                    Polazni_aerodrom = request.form.get('Field1_name')
                    uneseno.append(Polazni_aerodrom)
                    polazni_map = Polazni_aerodrom
                    Odredisni_aerodrom = request.form.get('Field2_name')
                    uneseno.append(Odredisni_aerodrom)
                    Datum_polaska = request.form.get('Field3_name')
                    uneseno.append(Datum_polaska)
                    Datum_povratka = request.form.get('Field32_name')
                    uneseno.append(Datum_povratka)
                    Broj_odraslih_putnika = request.form.get('Field4_name')
                    uneseno.append(Broj_odraslih_putnika)

                    conn = get_db_connection()
                    cur = conn.cursor()
                    df3 = pd.read_sql_query('select * from "letovi2" LIMIT1;',con=conn)
                    conn.close()
                    if not df3.empty:
                        a = [Polazni_aerodrom,Odredisni_aerodrom,Datum_polaska,Datum_povratka,Broj_odraslih_putnika]
                        b = [df3['pol_aer'][0],df3['odr_aer'][0],df3['dat_pol'][0],df3['dat_pov'][0],df3['bro_put'][0]]
                        cnt = 0
                        for i, j in zip(a, b):
                            if str(i) == str(j):
                                cnt += 1
                        if cnt == len(a):
                            lock = 1
                        else:
                            lock = 0

                    json_object = {
                    "originDestinations": [
                        {
                            "id": "1",
                            "originLocationCode": f'{Polazni_aerodrom}',
                            "destinationLocationCode": f'{Odredisni_aerodrom}',
                            "departureDateTimeRange": {
                                "date": f'{Datum_polaska}'
                            }
                        },
                        {
                            "id": "2",
                            "originLocationCode": f'{Odredisni_aerodrom}',
                            "destinationLocationCode": f'{Polazni_aerodrom}',
                            "departureDateTimeRange": {
                                "date": f'{Datum_povratka}'
                            }
                        },
                        
                            ],
                            "travelers": [
                                {
                                    "id": "1",
                                    "travelerType": "ADULT",
                                    "fareOptions": [
                                        "STANDARD"
                                    ]
                                }
                            ],
                            "sources": [
                                "GDS"
                            ],
                            "searchCriteria": {
                                "maxFlightOffers": 200
                            }
                        }
                    response = amadeus.shopping.flight_offers_search.post(json_object)
                    for i in range(len(response.data)):
                        p_aer.append(Polazni_aerodrom)
                        d_aer.append(Odredisni_aerodrom)
                        d_pol.append(Datum_polaska)
                        v_pol.append(response.data[i]["itineraries"][0]["segments"][0]["departure"]["at"].split('T')[1])
                        d_dol.append(response.data[i]["itineraries"][0]["segments"][0]["arrival"]["at"].split('T')[0])
                        v_dol.append(response.data[i]["itineraries"][0]["segments"][0]["arrival"]["at"].split('T')[1])
                        br_ad.append(Broj_odraslih_putnika)
                        d_pov.append(response.data[i]["itineraries"][-1]["segments"][-1]["arrival"]["at"].split('T')[0])
                        v_pov.append(response.data[i]["itineraries"][-1]["segments"][-1]["arrival"]["at"].split('T')[1])
                        cijene.append(response.data[i]["price"]["total"])
                        presj.append(len(response.data[i]["itineraries"][0]["segments"]))
                        valuta.append(response.data[i]["price"]["currency"])                       
                    
                    conn = get_db_connection()
                    cur = conn.cursor()

                    postgres_insert_query = """INSERT INTO letovi2(pol_aer, odr_aer, dat_pol, dat_dol,
                                            dat_pov,vri_pol ,vri_dol, vri_pov, bro_pre, bro_put, val, uku_cij, loc) VALUES(%s,%s,
                                            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);                    
                                            """
                    for i in range(len(response.data)):
                        record_to_insert = (p_aer[i], d_aer[i], d_pol[i], d_dol[i], d_pov[i], v_pol[i], v_dol[i], v_pov[i], presj[i], br_ad[i], valuta[i], cijene[i], '(45.815399,15.966568)')
                        cur.execute(postgres_insert_query, record_to_insert)
                                
                    cur.execute(f"SELECT loc[0]::double precision AS lat, loc[1]::double precision AS lon FROM iata_tab WHERE iata = '{Polazni_aerodrom}';")
                    for row in cur:
                        lat_lon = row 

                    command ="""SELECT iata, loc
                                    FROM iata_tab
                                    ORDER BY geom <-> st_setsrid(st_makepoint({0},{1}),4326)
                                    LIMIT 10;
                                    """
                    cur.execute(command.format(lat_lon[1], lat_lon[0]))
                    for row in cur:
                        lat_lon_near_name.append(row[0])
                        lat_lon_near_latlon.append(row[1])

                    conn.commit()
                    cur.close()
                    conn.close()
                    df_near = pd.DataFrame()
                    df_near = pd.DataFrame(list(zip(lat_lon_near_name, lat_lon_near_latlon)), columns=["iata","latlon"])
                    df0 = pd.DataFrame(list(zip(p_aer, d_aer, d_pol, d_dol, d_pov, v_pol, v_dol, v_pov, br_ad, cijene, presj, valuta)), columns=["p_aer","d_aer","d_pol","d_dol","d_pov","v_pol","v_dol","v_pov","br_ad","cijene","presj","valuta"])
                    if lock == 1:
                        podaci = df3.to_html(classes='table table-stripped table-dark')
                    else:
                        podaci = df0.to_html(classes='table table-stripped table-dark')
                    
                    dif2 = 'none'
                    diuf2 = 'block'
                    dit1 = 'block'
                if request.form.get('action2') == 'Po cijeni silazno':
                    conn = get_db_connection()
                    df2 = pd.read_sql_query('select * from "letovi2" order by uku_cij desc;',con=conn)
                    conn.close()
                    podaci=df2.to_html(classes='table table-stripped table-dark')
                    dit1 = 'block'
                    dif3 = 'block'
                    dif4 = 'block'
                    dif2 = 'none'
                if request.form.get('action2') == 'Po cijeni uzlazno':
                    conn = get_db_connection()
                    df2 = pd.read_sql_query('select * from "letovi2" order by uku_cij asc;',con=conn)
                    conn.close()
                    podaci=df2.to_html(classes='table table-stripped table-dark')
                    dit1 = 'block'
                    dif3 = 'block'
                    dif4 = 'block'
                    dif2 = 'none'
                if request.form.get('action2') == 'Po broju presjedanja uzlazno':
                    conn = get_db_connection()
                    df2 = pd.read_sql_query('select * from "letovi2" order by bro_pre asc;',con=conn)
                    conn.close()
                    podaci=df2.to_html(classes='table table-stripped table-dark')
                    dit1 = 'block'
                    dif3 = 'block'
                    dif4 = 'block'
                    dif2 = 'none'
                if request.form.get('action2') == 'Po broju presjedanja silazno':
                    conn = get_db_connection()
                    df2 = pd.read_sql_query('select * from "letovi2" order by bro_pre desc;',con=conn)
                    conn.close()
                    podaci=df2.to_html(classes='table table-stripped table-dark')
                    dit1 = 'block'
                    dif3 = 'block'
                    dif4 = 'block'
                    dif2 = 'none'


    return render_template('boot4.html',podaci=podaci, response=response, dif1=dif1, dif2=dif2, dif3=dif3, dif4=dif4, diuf2=diuf2,\
                           dit1=dit1, Polazni_aerodrom=Polazni_aerodrom, Odredisni_aerodrom=Odredisni_aerodrom,\
                           Datum_polaska=Datum_polaska, Datum_povratka=Datum_povratka, Broj_odraslih_putnika=Broj_odraslih_putnika)
   

if __name__ == '__main__':
   app.run(debug = False)