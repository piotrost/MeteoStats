from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import atexit
from shapely.wkt import loads
from pyproj import Transformer
from shapely.geometry import mapping, Polygon

# MongoDb
mUri = "mongodb+srv://remigiuszszewczak:ZXuo8bMXDvt3aawp@clusterpag.slpe7.mongodb.net/?retryWrites=true&w=majority&appName=ClusterPAG"
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


from bson import ObjectId


def download_stations(name_w, name_p, m=m):
    results = m.woj.find({'name': {'$regex': f'^{name_w}$', '$options': 'i'}}, {"name": 1, "national_c": 1})
    teryt_woj = None  # Domyślna wartość dla teryt_woj

    for woj in results:
        teryt_woj = woj.get("national_c")
        print(f'Województwo: {woj}')

    if not teryt_woj:
        print(f'Nie znaleziono województwa o nazwie: {name_w}')
        return []

    results = m.powiaty.find({'name': {'$regex': f'^{name_p}$', '$options': 'i'}},
                             {"name": 1, "national_c": 1, "geometry": 1})
    polygon_powiat = None

    for powiat in results:
        if powiat["national_c"].startswith(teryt_woj):
            polygon_powiat = powiat.get("geometry")
            print(f'Powiat: {powiat["national_c"]}')
            break  # Znaleziono odpowiedni powiat, można przerwać pętlę

    if not polygon_powiat:
        print(f'Nie znaleziono powiatu o nazwie: {name_p} w województwie: {name_w}')
        return []

    geojson_polygon = mapping(convert_polygon(loads(polygon_powiat)))

    query = {"geometry": {"$geoWithin": {"$geometry": geojson_polygon}}}
    results = m.effacility.find(query, {"properties.name": 1, "geometry": 1})

    stacje = []
    for result in results:
        # Konwersja ObjectId na string
        result["_id"] = str(result["_id"])
        stacje.append(result)
    print(stacje)

    return stacje


if __name__ == "__main__":
    download_stations("lubuskie", "krośnieński")