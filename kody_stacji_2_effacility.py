import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

def csv_to_geojson_geopandas(csv_file, geojson_file):
    def dms_to_decimal(dms):
        degrees = float(dms[0])
        minutes = float(dms[1])
        seconds = float(dms[2])
        decimal = degrees + (minutes / 60) + (seconds / 3600)
        return decimal

    # Read the CSV file into a Pandas DataFrame
    df = pd.read_csv(csv_file, sep=';')

    # Convert latitude and longitude strings to decimal degrees
    df['lat'] = df['Szerokość geograficzna'].str.split().apply(dms_to_decimal)
    df['lon'] = df['Długość geograficzna'].str.split().apply(dms_to_decimal)

    # Create a geometry column
    geometry = [Point(xy) for xy in zip(df['lon'], df['lat'])]
    df['geometry'] = geometry

    # compatibility
    df['name'] = df['ID']

    # Create a GeoDataFrame
    gdf = gpd.GeoDataFrame(df, geometry='geometry', crs='EPSG:4326')

    # Save the GeoDataFrame to GeoJSON
    gdf.to_file(geojson_file, driver='GeoJSON')

if __name__ == '__main__':
    csv_file = r'data\kody_stacji.csv'
    geojson_file = r'data\local\effacility.geojson'
    csv_to_geojson_geopandas(csv_file, geojson_file)