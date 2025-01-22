from flask import Flask, render_template, request, Response, jsonify
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
import io
import numpy as np
import os
from mongo_download import download_stations
from redis_in_out import refresh_redis, load, meteo_param_codes
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
    start_date = request.form['start_date']
    end_date = request.form['end_date']
    woj = request.form['woj']
    pow = request.form['pow']
    print(f'Wybrano przedział od {start_date} do {end_date} w woj. {woj} w pow. {pow}.')

    stations = download_stations(woj, pow)

    # Debuguj dane wejściowe
    print(f"Dostępne dane: {df}")

    try:
        filtered_df = df[(df['Date'] >= start_date) & (df['Date'] <= end_date)]
        print(f"Przefiltrowane dane:\n{filtered_df}")

        if filtered_df.empty:
            print("Brak danych w wybranym zakresie dat.")
            return Response("Brak danych w wybranym zakresie dat.", mimetype='text/plain')

        # Generowanie wykresu
        fig, axs = plt.subplots(1, 1, figsize=(7.3, 9))
        axs.plot(filtered_df['Date'], filtered_df['Temperature'], label='Temperature (C)', color='red')
        axs.set_title('Temperature Over Time')
        axs.set_xlabel('Date')
        axs.set_ylabel('Temperature (C)')
        axs.legend()

        # axs[1].plot(filtered_df['Date'], filtered_df['Precipitation'], label='Precipitation (mm)', color='blue')
        # axs[1].set_title('Precipitation Over Time')
        # axs[1].set_xlabel('Date')
        # axs[1].set_ylabel('Precipitation (mm)')
        # axs[1].legend()

        # axs[2].plot(filtered_df['Date'], filtered_df['WindSpeed'], label='Wind Speed (km/h)', color='green')
        # axs[2].set_title('Wind Speed Over Time')
        # axs[2].set_xlabel('Date')
        # axs[2].set_ylabel('Wind Speed (km/h)')
        # axs[2].legend()

        plt.tight_layout()

        canvas = FigureCanvas(fig)
        output = io.BytesIO()
        canvas.print_png(output)
        plt.close(fig)

        return Response(output.getvalue(), mimetype='image/png')

    except Exception as e:
        print(f"Błąd generowania wykresu: {e}")
        return Response(f"Błąd: {e}", mimetype='text/plain', status=500)

@app.route('/stations', methods=['POST'])
def get_stations():
    woj = request.json.get('woj')
    pow = request.json.get('pow')
    print(f'Pobieranie stacji dla województwa: {woj.lower()}, powiatu: {pow}.')
    stations = download_stations(woj, pow)
    return jsonify(stations)

if __name__ == '__main__':
    app.run(debug=True)
