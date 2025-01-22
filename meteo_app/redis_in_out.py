# Author: Piotr Ostaszewski (325697)
# Created:  2025-01-22T20:03:18.645Z

import requests, zipfile, io
import astral as ast
from astral.sun import sun
import datetime
import os
import redis
import json
from pathlib import Path
from mongo_download import download_all_stations
import pandas as pd

# current working directory
cwd_obj = Path.cwd()
cwd = str(cwd_obj)

# authentication 
auth = json.load(open(cwd + r"\data\keys.json"))

# redis connection
r = redis.Redis(
    host=auth["redis_host"],
    port=auth["redis_port"],
    decode_responses=True,
    username=auth["redis_username"],
    password=auth["redis_password"],
)

# słowniczek
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

def refresh_redis():
    # clean
    r.flushdb()

    # load
    meteo_dir = cwd_obj / "data" / "meteo"
    for folder in os.listdir(meteo_dir):
        if folder.is_dir():
            foldername = folder.name
            for file in os.listdir(meteo_dir / folder):
                filename = file.name
                filename = filename.split(".")[0]
                r.sadd(foldername, filename)
    

astral_polsih_timezone = ast.zoneinfo.ZoneInfo("UTC")   # meteodane są w tym czasie
def astral_sunlist(dateob, latlon=[52.067080, 19.479506]):
    li = ast.LocationInfo(latitude=latlon[0], longitude=latlon[1])
    s = sun(li.observer, date=dateob, tzinfo=astral_polsih_timezone)

    return [s['dawn'].timestamp(), s['sunrise'].timestamp(), s['noon'].timestamp(), s['sunset'].timestamp(), s['dusk'].timestamp()]

# the reducement of sun objects works for files sorted by station and then by date (a single station block is sorted by date)
def download_meteo(year, month):
    # get data
    response = requests.get(f"https://danepubliczne.imgw.pl/datastore/getfiledown/Arch/Telemetria/Meteo/{year}/Meteo_{year}-{month:02d}.zip")
    sundict = {}
    stationdict = download_all_stations()
    
    # open in csv
    with zipfile.ZipFile(io.BytesIO(response.content)) as zip:
        os.makedirs(cwd + f"\\data\\meteo\\{year}_{month:02d}")
        for filename in zip.namelist():
            with zip.open(filename) as in_csv:
                # filename = filename.decode("utf-8")             # needed ?
                filename = filename.split(".")[0]               # no extension
                filename = filename.split("_")[0]               # param_code
                
                # open out csv
                with open(cwd + f"\\data\\meteo\\{year}_{month:02d}\\" + filename + ".csv", "w") as out_csv:
                    last_day = None                                 # woks only assumed csv is sorted by station and the station block is sorted by date
                    
                    # line: station;param_code;date;value
                    for line in in_csv:              
                        line = line.decode("utf-8-sig")                 # needed ?
                        line = line.split(";")

                        # time
                        datetime_object = datetime.datetime.strptime(line[2], "%Y-%m-%d %H:%M")
                        unix_time = int(datetime_object.timestamp())
                        
                        # sun
                        day = line[2][8:10]                        # moving on to the next statoion changes th day also (it was last, now it's first)
                        if last_day == day:
                            sunlist = sundict[last_day]
                        else:
                            try:
                                sunlist = astral_sunlist(datetime_object.date(), stationdict[line[0]])
                            except:
                                pass
                            sundict[day] = sunlist
                            last_day = day
                        
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
                        
                        out_csv.write(f"{line[0]},{tod},{unix_time},{line[3].replace(",", ".")}\n")  # station;time_of_the_day;unix_timestamp;value
                
                r.sadd(f"{year}_{month:02d}", filename)
    
def load(year, month, param_code):
    if r.exists(f"{year}_{month:02d}"):
        pass
    else:
        download_meteo(year, month)
    
    if r.sismember(f"{year}_{month:02d}", param_code):
        return pd.read_csv(cwd + f"\\data\\meteo\\{year}_{month:02d}\\" + param_code + ".csv", header=None, names=["station", "tod", "unix_time", "value"])
    else:
        return f"Brak danych dla parametru {param_code} w {year}/{month:02d}."

if __name__ == "__main__":
    import time
    t1 = time.time()
    load(2023, 7, "B00300S")
    t2 = time.time()

    print(f"Time: {t2 - t1}")