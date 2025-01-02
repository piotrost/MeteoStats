from flask import Flask, render_template, request, Response
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
import io
import numpy as np

app = Flask(__name__)

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
    start_date = request.form['start_date']
    end_date = request.form['end_date']
    woj = request.form['woj']
    pow = request.form['pow']
    print(f'Wybrano przedział od {start_date} do {end_date} w woj. {woj} w pow. {pow}.')

    filtered_df = df[(df['Date'] >= start_date) & (df['Date'] <= end_date)]

    fig, axs = plt.subplots(3, 1, figsize=(7.3, 9))

    axs[0].plot(filtered_df['Date'], filtered_df['Temperature'], label='Temperature (C)', color='red')
    axs[0].set_title('Temperature Over Time')
    axs[0].set_xlabel('Date')
    axs[0].set_ylabel('Temperature (C)')
    axs[0].legend()

    axs[1].plot(filtered_df['Date'], filtered_df['Precipitation'], label='Precipitation (mm)', color='blue')
    axs[1].set_title('Precipitation Over Time')
    axs[1].set_xlabel('Date')
    axs[1].set_ylabel('Precipitation (mm)')
    axs[1].legend()

    axs[2].plot(filtered_df['Date'], filtered_df['WindSpeed'], label='Wind Speed (km/h)', color='green')
    axs[2].set_title('Wind Speed Over Time')
    axs[2].set_xlabel('Date')
    axs[2].set_ylabel('Wind Speed (km/h)')
    axs[2].legend()

    plt.tight_layout()

    canvas = FigureCanvas(fig)
    output = io.BytesIO()
    canvas.print_png(output)
    plt.close(fig)
    return Response(output.getvalue(), mimetype='image/png')


if __name__ == '__main__':
    app.run(debug=True)
