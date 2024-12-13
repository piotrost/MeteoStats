# Author: Piotr Ostaszewski (325697)
# Created:  2024-11-28T23:49:00.779Z

"""
	wprawdzie to się nie nada,
    ale może jakieś schematy postępowania stąd będą pomocne.
"""

import requests, zipfile, io
import pandas as pd
import geopandas as gpd
import astral as ast
from astral.sun import sun
import datetime
import os
import redis
import pathlib
import shapely

r = redis.Redis(host="127.0.0.1", port=6379, db=0)

meteo_param_codes = {
    "B00300S":"Temperatura powietrza (oficjalna)",
    "B00305A":"Temperatura gruntu (czujnik)",
    "B00202A":"Kierunek wiatru (czujnik)",
    "B00702A":"Średnia prędkość wiatru czujnik 10 minut",
    "B00703A":"Prędkość maksymalna (czujnik)",
    "B00608S":"Suma opadu 10 minutowego",
    "B00604S":"Suma opadu dobowego",
    "B00606S":"Suma opadu godzinowego",
    "B00802A":"Wilgotność względna powietrza (czujnik)",
    "B00714A":"Największy poryw w okresie 10min ze stacji Synoptycznej",
    "B00910A":"Zapas wody w śniegu (obserwator)"
    }

astral_polsih_timezone = ast.zoneinfo.ZoneInfo("UTC")   # meteodane są w tym czasie
def astral_sunlist(dateob, lat=52.067080, lon=19.479506):
    li = ast.LocationInfo(latitude=lat, longitude=lon)
    s = sun(li.observer, date=dateob, tzinfo=astral_polsih_timezone)

    return [s['dawn'].timestamp(), s['sunrise'].timestamp(), s['noon'].timestamp(), s['sunset'].timestamp(), s['dusk'].timestamp()]

def download_meteo(year, month):
    response = requests.get(f"https://danepubliczne.imgw.pl/datastore/getfiledown/Arch/Telemetria/Meteo/{year}/Meteo_{year}-{month:02d}.zip")
    sundict = {}
    with zipfile.ZipFile(io.BytesIO(response.content)) as zip:
        for filename in zip.namelist():
            with zip.open(filename) as csv:
                # filename = filename.decode("utf-8")             # needed ?
                filename = filename.split(".")[0]               # no extension
                filename = filename.split("_")[0]               # param_code
                pipe = r.pipeline()                             # redis pipeline
                last_day = None                                 # woks only assumed csv is sorted by station and the station block is sorted by date

                for line in csv:              
                    line = line.decode("utf-8")                 # needed ?
                    line = line.split(";")

                    datetime_object = datetime.datetime.strptime(line[2], "%Y-%m-%d %H:%M")
                    unix_time = int(datetime_object.timestamp())
                    
                    day = line[2][8:10]
                    if last_day == day:
                        sunlist = sundict[last_day]
                    else:
                        last_day = day
                        sunlist = astral_sunlist(datetime_object.date())
                        sundict[last_day] = sunlist
                    
                    if unix_time < sunlist[0]:
                        tod = "n"
                    elif unix_time < sunlist[1]:
                        tod = "d"
                    elif unix_time < sunlist[2]:
                        tod = "m"
                    elif unix_time < sunlist[3]:
                        tod = "a"
                    elif unix_time < sunlist[4]:
                        tod = "e"
                    else:
                        tod = "n"
                                                    # param_code -> date_id_ddhhmm                         :station  :tod  :value           ->           unix_time
                    pipe.zadd(f"{year}:{month:02d}:{filename}", {f"{day + line[2][11:13] + line[2][14:16]}:{line[0]}:{tod}:{line[3].replace(",", ".")}": unix_time})
                
                pipe.execute()
    
def load(year, month, param_code):
    if r.exists(f"{year}:{month:02d}:{param_code}"):
        pass
    else:
        for pcd in meteo_param_codes:
            if r.exists(f"{year}:{month:02d}:{pcd}"):
                print(f"Brak danych dla parametru {param_code} w {year}/{month:02d}.")
                return

        download_meteo(year, month)

def local_2_redis(r=r):
    local_dir = "/home/piotr/Documents/pw/5/pag/NoSqlProject/data/local/"
    for filename in os.listdir(local_dir):
        if filename.endswith(".geojson") or filename.endswith(".shp") or filename.endswith(".csv"):
            file_to_redis(local_dir + filename)

def file_to_redis(filename, r=r):
    path = pathlib.Path(filename)
    name = path.stem
    
    pipe = r.pipeline()
    if filename.endswith(".csv"):
        pgdf = pd.read_csv(filename, sep=';')
    else:
        pgdf = gpd.read_file(filename)

    pgdf.rename(columns={pgdf.columns[0]: 'id'}, inplace=True)

    columns = pgdf.columns.tolist()
    colstring = " ".join(columns)
    pipe.set("columns:" + name, colstring)

    for row in pgdf.itertuples():
        d = {k: str(v) for k, v in row._asdict().items() if v is not None}
        pipe.hset(name + ':' + str(d["id"]), mapping=d)
    
    pipe.execute()

def stations_in_polys(polylist=["woj", "powiaty"],r=r):
    station_keys = r.keys("effacility:*")
    sids = []; sgeoms = []
    for station in station_keys:
        sgeoms.append(shapely.wkt.loads(r.hget(station, "geometry").decode("utf-8")))
        sids.append(station.decode("utf-8").split(":")[-1])

    pipe = r.pipeline()
    for poly_key in polylist:
        polys = r.keys(f"{poly_key}:*")
        for poly in polys:
            geom = shapely.wkt.loads(r.hget(poly, "geometry").decode("utf-8"))
            id = poly.decode("utf-8").split(":")[-1]
            for i, station in enumerate(sgeoms):
                if station.within(geom):
                    pipe.set(f"lok:{poly_key}:{sids[i]}", id)
        
    pipe.execute()

if __name__ == "__main__":
    local_2_redis()
    stations_in_polys()