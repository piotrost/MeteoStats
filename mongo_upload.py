from pymongo.mongo_client import MongoClient, InsertOne
from pymongo.server_api import ServerApi
from pyproj import Transformer
import pandas as pd
import geopandas as gpd
import os
import atexit
import json
from pathlib import Path

# current working directory
cwd_obj = Path.cwd()
cwd = str(cwd_obj)

# authentication 
auth = json.load(open(cwd + r"\data\keys.json"))

# MongoDb
mUri = auth["mongo_uri"]
mLocal = "mongodb://localhost:27017"
mClient = MongoClient(mUri, server_api=ServerApi('1'))
m = mClient["PAG"]  # database


def on_exit():
    if "mClient" in globals():
        mClient.close()
        print("MongoDB connection closed.")


atexit.register(on_exit)


def convert_point(coordinates):
    transformer = Transformer.from_crs("EPSG:2180", "EPSG:4326", always_xy=True)
    return transformer.transform(coordinates[0], coordinates[1])


def local_2_mongo(local_dir, m=m):
    for filename in os.listdir(local_dir):
        if filename.endswith(".geojson") or filename.endswith(".shp") or filename.endswith(".csv"):
            path = os.path.join(local_dir, filename)
            file_to_mongo(path)


def file_to_mongo(filename, m=m):
    name = os.path.basename(filename).split('.')[0]

    if filename.endswith(".geojson"):
        with open(filename, "r", encoding='utf-8') as f:
            geojson_data = json.load(f)

        for feature in geojson_data["features"]:
            geometry = feature["geometry"]
            # not needed for effacility from kody_stacji.csv
            # if geometry["type"] == "Point":
            #     geometry["coordinates"] = convert_point(geometry["coordinates"])

        collection = m[name]

        if "features" in geojson_data:
            collection.insert_many(geojson_data["features"])
        else:
            collection.insert_one(geojson_data)

        collection.create_index([("geometry", "2dsphere")])
    else:
        if filename.endswith(".csv"):
            pgdf = pd.read_csv(filename, sep=';')
        else:
            pgdf = gpd.read_file(filename)

        pgdf.rename(columns={pgdf.columns[0]: 'id'}, inplace=True)
        print(pgdf)

        columns = pgdf.columns.tolist()
        colstring = " ".join(columns)
        m.info.insert_one({"name": name, "columns": colstring})

        operations = []
        for row in pgdf.itertuples():
            d = {k: str(v) for k, v in row._asdict().items() if v is not None}
            operations.append(InsertOne(d))
        collection = m[name]
        result = collection.bulk_write(operations)


if __name__ == "__main__":
    local_2_mongo("data/local")
