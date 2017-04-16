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

	num_agency_day = pd.read_csv(file, names= ['date', 'type', 'num'])
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

# fig2

def plot_box_agency_day(file, save_path = '../../figs/', save_name = 'box_agency_year'):
	num_agency_day_box = pd.read_csv(file, names= ['date', 'agency', 'list'])

	num_agency_day_box = num_agency_day_box.sort_values(['date','agency'])

	agency = np.unique(num_agency_day_box['agency'])

	data = []
	for i in agency:
	    data.append(go.Box(
	        x = list(num_agency_day_box.loc[num_agency_day_box.agency == i,'list']),
	        y = list(num_agency_day_box.loc[num_agency_day_box.agency == i,'date']),
	        name = i,
	        boxmean = False,
	        orientation = 'h',
	        ))

	layout = go.Layout(
	    xaxis=dict(
	        title='distribution of complaints each year of different type',
	        zeroline=False
	    ),
	    boxmode='group'
	)
	fig = go.Figure(data=data, layout=layout)
	save_path = os.path.join(save_path, save_name) 
	plot(fig, filename=save_path + '.html', auto_open=False)


## fig3

def plot_bar_agency_month(file, save_path = '../../figs/', save_name = 'bar_agency_month'):
	month_agency_bar = pd.read_csv(file, names= ['date', 'agency','type','num'])

	month_agency_bar = month_agency_bar.sort_values(['date','agency','type'])

	month_agency_bar_agency = month_agency_bar.groupby(by = ['date','agency'])['num'].sum()
	month_agency_bar_agency = month_agency_bar_agency.reset_index()

	agency = np.unique(month_agency_bar_agency['agency'])

	data = []
	for i in agency:
	    data.append(go.Bar(
	        x = list(month_agency_bar_agency.loc[month_agency_bar_agency.agency == i,'date']),
	        y = list(month_agency_bar_agency.loc[month_agency_bar_agency.agency == i,'num']),
	        name = i
	        ))

	layout = go.Layout(
	    barmode='stack'
	)

	fig = go.Figure(data=data, layout=layout)
	save_path = os.path.join(save_path, save_name)
	plot(fig, filename=save_path + '.html', auto_open=False)


## fig4

def plot_type_year_pie(file, save_path = '../../figs/', save_name = 'type_year_pie'):

	type_year_pie = pd.read_csv(file, names= ['year','type','num'])
	type_year_pie = type_year_pie.sort_values(['year','type'])

	types = np.unique(type_year_pie.type)
	years = np.unique(type_year_pie.year)
	num = len(years)
	gap = 0.08
	step = (1.0 - gap*(num / 2)) / (num / 2 + 1)
	ys = [[i*(step + gap)  , i*(step + gap) + step  ] for i in range((num/2 + 1))]
	xs = [[0, .48],[.52, 1]]
	data = []
	annotations = []
	for i,j in enumerate(years):
	    data.append(go.Pie(
	        values = list(type_year_pie.loc[type_year_pie.year == j, 'num']),
	        labels = list(type_year_pie.loc[type_year_pie.year == j, 'type']),
	        domain = dict(
	                x = xs[i % 2], 
	                y = ys[i / 2]),
	        name = j,
	        textinfo ='none' , 
	        hole = .4
	        ))
	    
	    annotations.append(dict(
	        font = dict(
	            size = 20
	            ),
	        showarrow = False,
	        text = j,
	        x = (sum(xs[i % 2]) - gap )/ 2.0,
	        y = sum(ys[i / 2]) / 2.0)
	        )

	layout = go.Layout(
	    title = 'Complaint type distribution every year',
	    annotations = annotations
	)

	fig = go.Figure(data=data, layout=layout)
	save_path = os.path.join(save_path, save_name)
	plot(fig, filename=save_path + '.html', auto_open=False)

if __name__ == '__main__':
	num_agency_day = 'num_agency_day.txt'
	plot_num_agency_day(num_agency_day)

	box_agency_year = 'num_agency_day_box.txt'
	plot_box_agency_day(box_agency_year)

	bar_agency_month = 'month_agency_bar.txt'
	plot_bar_agency_month(bar_agency_month)

	type_year_pie = 'type_year_pie.txt'
	plot_type_year_pie(type_year_pie)




