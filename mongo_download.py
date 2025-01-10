from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import atexit
from shapely.wkt import loads
from pyproj import Transformer
from shapely.geometry import mapping, Polygon

# MongoDb
mUri = "https://youtu.be/pZ_3-6n68sc?si=3H4m01TOD2YOC-Pn"
mLocal = "mongodb://localhost:27017"
mClient = MongoClient(mUri, server_api=ServerApi('1'))
m = mClient["PAG"]  # database


def on_exit():
    if "mClient" in globals():
        mClient.close()
        print("MongoDB connection closed.")


atexit.register(on_exit)


def convert_polygon(polygon):
    transformer = Transformer.from_crs("EPSG:2180", "EPSG:4326", always_xy=True)
    transformed_exterior = [transformer.transform(x, y) for x, y in polygon.exterior.coords]
    return Polygon(transformed_exterior)


def download_stations(name_w, name_p, m=m):
    results = m.woj.find({'name': name_w}, {"name": 1, "national_c": 1})
    for woj in results:
        teryt_woj = woj["national_c"]
        print(f'Województwo: {woj}')

    results = m.powiaty.find({'name': name_p}, {"name": 1, "national_c": 1, "geometry": 1})
    for powiat in results:
        if powiat["national_c"].startswith(teryt_woj):
            polygon_powiat = powiat["geometry"]
            print(f'Powiat: {powiat["national_c"]}')

    geojson_polygon = mapping(convert_polygon(loads(polygon_powiat)))

    query = {"geometry": {"$geoWithin": {"$geometry": geojson_polygon}}}
    results = m.effacility.find(query, {"properties.name": 1, "geometry": 1})

    stacje = []
    for result in results:
        stacje.append(result)
    print(stacje)

    return stacje


if __name__ == "__main__":
    download_stations("lubuskie", "krośnieński")
