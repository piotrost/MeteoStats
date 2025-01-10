from pymongo.mongo_client import MongoClient, InsertOne
from pymongo.server_api import ServerApi
import pandas as pd
import geopandas as gpd
import os
import atexit

# MongoDb
mUri = "https://www.youtube.com/watch?v=5xr_iIb23as"
mLocal = "mongodb://localhost:27017"
mClient = MongoClient(mUri, server_api=ServerApi('1'))
m = mClient["PAG"] # database

def on_exit():
    if "mClient" in globals():
        mClient.close()
        print("MongoDB connection closed.")
        
atexit.register(on_exit)

def local_2_mongo(m=m):
    local_dir = "/home/piotr/Documents/pw/5/pag/NoSqlProject/data/local/"
    for filename in os.listdir(local_dir):
        if filename.endswith(".geojson") or filename.endswith(".shp") or filename.endswith(".csv"):
            path = os.path.join(local_dir, filename)
            file_to_mongo(path)

def file_to_mongo(filename, m=m):
    name = os.path.basename(filename).split('.')[0]
    
    if filename.endswith(".csv"):
        pgdf = pd.read_csv(filename, sep=';')
    else:
        pgdf = gpd.read_file(filename)

    pgdf.rename(columns={pgdf.columns[0]: 'id'}, inplace=True)

    columns = pgdf.columns.tolist()
    colstring = " ".join(columns)
    m.info.insert_one({ "name": name, "columns": colstring })
    
    operations = []
    for row in pgdf.itertuples():
        d = {k: str(v) for k, v in row._asdict().items() if v is not None}
        operations.append(InsertOne(d))
    collection = m[name]
    result = collection.bulk_write(operations)

if __name__ == "__main__":
    local_2_mongo()
