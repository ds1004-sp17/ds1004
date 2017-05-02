from datetime import datetime
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.plotly as py
import plotly.graph_objs as go
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot
import folium


def plot_num_agency_day(file, save_path = '../../figs/', save_name = 'num_agency_day'):
	agency_day = np.loadtxt(file, delimeter = ',')

	num_agency_day = pd.DataFrame(agency_day, columns=['date', 'type', 'num'])
	num_agency_day = num_agency_day.sort_values(['date'])

	x= np.unique(num_agency_day['date']).astype(np.str)

	agency = np.unique(num_agency_day['type'])
	y_org = []
	for i in agency:
	    y_org.append(list(num_agency_day[num_agency_day.type == i]['num']))


	# Add data to create cumulative stacked values
	y_stack = []
	for i in range(len(agency)):
	    temp = y_org[0]
	    for j in range(i):
	        temp = [k1 + k2 for k1, k2 in zip(temp, y_org[j+1])]
	    y_stack.append(temp)


	# Make original values strings and add % for hover text
	y_txt = y_org

	data = []

	for i in range(len(agency)):
	    data.append( go.Scatter(
	        x=x,
	        y=y_stack[i],
	        text=y_txt[i],
	        mode='lines',
	        fill='tonexty',
	        name = agency[i]
	    ))


	fig = go.Figure(data=data)
	save_path = os.path.join(save_path, save_name) 
	plot(fig, filename=save_path + '.html', auto_open=False)


if __name__ == '__main__':
	file = '../../out/num_agency_day.txt'
	plot_num_agency_day(file)