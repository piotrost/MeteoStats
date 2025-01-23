from flask import Flask, render_template, request, Response, jsonify, render_template_string
import pandas as pd
import matplotlib.pyplot as plt
import os
from mongo_download import download_stations
from redis_in_out import refresh_redis, load, meteo_param_codes
import mpld3

# the Flask app
app = Flask(__name__)

# create datastore if doesn't exist
if not os.path.exists('data'):
    os.makedirs('data')
if not os.path.exists('data/meteo'):
    os.makedirs('data/meteo')

# refresh redis
refresh_redis()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/plot', methods=['POST'])
def plot():
    # user input
    print("Funkcja /plot została wywołana")
    start_date_str = request.form['start_date']
    end_date_str = request.form['end_date']
    woj = request.form['woj']
    pow = request.form['pow']
    print(f'Wybrano przedział od {start_date_str} do {end_date_str} w woj. {woj} w pow. {pow}.')

    # ***************************
    meteo_param = "B00300S" # B00300S, B00305A, B00202A, B00702A, B00703A, B00608S, B00604S, B00606S, B00802A, B00714A, B00910A
    agg_freq = "D" # D - daily, H - hourly, T
    agg_val = "mean" # mean
    tod = ["m", "a"]       # n - night, d - dawn, m - morning, a - afternoon, e - evening
    # ***************************

    # get station names int list
    station_id_list = []
    stations = download_stations(woj, pow)
    for station in stations:
        station_id_list.append(int(station['properties']['name']))
    print(f"Stacje w wybranym obszarze: {station_id_list}")

    # get date elements
    start, end = start_date_str.split('-'), end_date_str.split('-')
    m_start, m_end = int(start[1]), int(end[1])
    y_start, y_end = int(start[0]), int(end[0])

    if m_start != m_end or y_start != y_end:
        df_list = []
        y_range = range(y_start, y_end+1)
        m = m_start
        for y in y_range:
            while y != y_end or m != m_end+1:
                df_part = load(y, m, meteo_param)
                df_list.append(df_part)
                print(f"\nZaładowano dane z {y}_{m:02d}.\n")
                if m == 12:
                    break
                else:
                    m += 1
        df = pd.concat(df_list, ignore_index=True)
    else:
        df = load(int(start[0]), int(start[1]), meteo_param)

    # Convert str to datetime
    pd_start = pd.Timestamp(start_date_str)
    int_start = int(pd_start.timestamp())
    pd_end = pd.Timestamp(end_date_str)
    int_end = int(pd_end.timestamp())

    # Debuguj dane wejściowe
    print(f"Dostępne dane:\n{df}")

    try:
        # Filter data
        df = df[(  df['datetime'] >= int_start) 
                & (df['datetime'] <= int_end)
                & (df['station'].isin(station_id_list))
                & (df['tod'].isin(tod))]
        
        # Aggregating data
        df['datetime'] = pd.to_datetime(df['datetime'], unit='s')
        df = df.groupby(pd.Grouper(key='datetime', freq=agg_freq)).agg({
            'value': agg_val
        }).reset_index()

        # Debuguj przefiltrowane dane
        print(f"Przefiltrowane dane:\n{df}")
        if df.empty:
            print("Brak danych w wybranym zakresie dat.")
            return Response("Brak danych w wybranym zakresie dat.", mimetype='text/plain')

        # Generowanie wykresu
        fig, axs = plt.subplots(1, 1, figsize=(7.3, 4))
        axs.plot(df['datetime'], df['value'], label=meteo_param_codes[meteo_param], color='red')
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

    # handle exceptions
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
