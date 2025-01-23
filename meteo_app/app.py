from flask import Flask, render_template, request, Response, jsonify, render_template_string
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
import io
import numpy as np
import os
from mongo_download import download_stations
from redis_in_out import refresh_redis, load, meteo_param_codes
import mpld3
import datetime

app = Flask(__name__)

# create datastore if doesn't exist
if not os.path.exists('data'):
    os.makedirs('data')
if not os.path.exists('data/meteo'):
    os.makedirs('data/meteo')

# Sample meteorological data. Replace this with actual data
data = {
    "Date": pd.date_range(start="2023-01-01", periods=365, freq="D"),
    "Temperature": pd.Series(range(365)).apply(lambda x: 20 + 5 * np.sin(x * 2 * np.pi / 365)),
    "Precipitation": pd.Series(range(365)).apply(lambda x: np.random.random()),
    "WindSpeed": pd.Series(range(365)).apply(lambda x: 10 + np.random.random() * 5),
}
df = pd.DataFrame(data)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/plot', methods=['POST'])
def plot():
    print("Funkcja /plot została wywołana")
    start_date_str = request.form['start_date']
    end_date_str = request.form['end_date']
    woj = request.form['woj']
    pow = request.form['pow']
    print(f'Wybrano przedział od {start_date_str} do {end_date_str} w woj. {woj} w pow. {pow}.')

    # ***************************
    meteo_param = "B00300S" # B00300S, B00305A, B00202A, B00702A, B00703A, B00608S, B00604S, B00606S, B00802A, B00714A, B00910A
    aggregataion = "daily" # daily, hourly
    tod = ["m", "a"]       # n - night, d - dawn, m - morning, a - afternoon, e - evening
    # ***************************

    stations = download_stations(woj, pow)
    start, end = start_date_str.split('-'), end_date_str.split('-')
    df = load(int(start[0]), int(start[1]), meteo_param)

    # Convert str to datetime
    pd_start = pd.Timestamp(start_date_str)
    pd_end = pd.Timestamp(end_date_str)

    # Debuguj dane wejściowe
    print(f"Dostępne dane: {df}")

    try:
        df['datetime'] = pd.to_datetime(df['datetime'], unit='s')
        filtered_df = df[(df['datetime'] >= pd_start) & (df['datetime'] <= pd_end)]
        filtered_df = filtered_df[filtered_df['tod'].isin(tod)]

        NIE FILTRUJĘ PO STACJI XD
        print(f"Przefiltrowane dane:\n{filtered_df}")

        if filtered_df.empty:
            print("Brak danych w wybranym zakresie dat.")
            return Response("Brak danych w wybranym zakresie dat.", mimetype='text/plain')

        # Generowanie wykresu
        fig, axs = plt.subplots(1, 1, figsize=(7.3, 4))
        axs.plot(filtered_df['datetime'], filtered_df['value'], label=meteo_param_codes[meteo_param], color='red')
        axs.set_title(meteo_param_codes[meteo_param] + " w czasie")
        axs.set_xlabel('Data')
        axs.set_ylabel(meteo_param_codes[meteo_param])
        axs.legend()

        # Convert plot to HTML
        plot_html = mpld3.fig_to_html(fig)

        # Render the plot in a simple HTML template
        return render_template_string("""
            <html>
                <head>
                    <title>Plot</title>
                    <script src="https://cdnjs.cloudflare.com/ajax/libs/d3/3.5.17/d3.min.js"></script>
                    <script src="https://mpld3.github.io/js/mpld3.v0.3.js"></script>
                </head>
                <body>
                    <h1>Plot</h1>
                    {{ plot_html|safe }}
                </body>
            </html>
        """, plot_html=plot_html)

    except Exception as e:
        print(f"An error occurred: {e}")
        return Response(f"An error occurred: {e}", mimetype='text/plain', status=500)

@app.route('/stations', methods=['POST'])
def get_stations():
    woj = request.json.get('woj')
    pow = request.json.get('pow')
    print(f'Pobieranie stacji dla województwa: {woj.lower()}, powiatu: {pow}.')
    stations = download_stations(woj, pow)
    return jsonify(stations)

if __name__ == '__main__':
    app.run(debug=True)
