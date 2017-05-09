from datetime import datetime
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.plotly as py
import plotly.graph_objs as go
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot
import plotly.figure_factory as ff
import folium
import json


def plot_num_agency_day(file, save_path = '../../figs/', save_name = 'num_agency_day'):

	if(not os.path.isfile(file)):
		print('file not exist')
		return None

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
	        name = agency[i],
	        hoverinfo='text+name',
	        line=dict(
	            shape='spline'
	        )
	    ))


	fig = go.Figure(data=data)
	save_path = os.path.join(save_path, save_name) 
	plot(fig, filename=save_path + '.html', auto_open=False)

# fig2

def plot_box_agency_day(file, save_path = '../../figs/', save_name = 'box_agency_year'):
	if(not os.path.isfile(file)):
		print('file not exist')
		return None	

	num_agency_day_box = pd.read_csv(file, names= ['date', 'agency', 'list'])

	num_agency_day_box = num_agency_day_box.sort_values(['date','agency'])

	agency = np.unique(num_agency_day_box['agency'])

	data = []
	for i in agency:
	    data.append(go.Box(
	        y = list(num_agency_day_box.loc[num_agency_day_box.agency == i,'list']),
	        x = list(num_agency_day_box.loc[num_agency_day_box.agency == i,'date']),
	        name = i,
	        boxmean = False,
	        orientation = 'v',
	        ))

	layout = go.Layout(
	    xaxis=dict(
	        title='distribution of complaints each year of different type',
	        zeroline=False
	    ),
	    boxmode='group',
	    title = 'Complaints distribution of each agency by each year'
	)
	fig = go.Figure(data=data, layout=layout)
	save_path = os.path.join(save_path, save_name) 
	plot(fig, filename=save_path + '.html', auto_open=False)


## fig3

def plot_bar_agency_month(file, save_path = '../../figs/', save_name = 'bar_agency_month'):
	if(not os.path.isfile(file)):
		print('file not exist')
		return None	

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
	    barmode='stack',
	    title = 'Complaints distribution by different Agency'
	)

	fig = go.Figure(data=data, layout=layout)
	save_path = os.path.join(save_path, save_name)
	plot(fig, filename=save_path + '.html', auto_open=False)


## fig4

def plot_type_year_pie(file, save_path = '../../figs/', save_name = 'type_year_pie'):
	if(not os.path.isfile(file)):
		print('file not exist')
		return None

	type_year_pie = pd.read_csv(file, names= ['year','type','num'])
	type_year_pie = type_year_pie.sort_values(['year','type'])

	types = np.unique(type_year_pie.type)
	years = np.unique(type_year_pie.year)
	num = len(years)
	gap = 0.008
	step = (1.0 - gap*(num / 2)) / ((num+1)/2)
	ys = [[i*(step + gap)  , i*(step + gap) + step  ] for i in range((num+1)/2)]
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
	        x = np.array([0, 0.5])[i % 2],
	        y = sum(ys[i / 2]) / 2.0)
	        )

	layout = go.Layout(
	    title = 'Complaint type distribution every year',
	    annotations = annotations
	)

	fig = go.Figure(data=data, layout=layout)
	save_path = os.path.join(save_path, save_name)
	plot(fig, filename=save_path + '.html', auto_open=False)

# fig5
def plot_board_agency_year(file, save_path = '../../figs/', save_name = 'board_agency_year'):
	if(not os.path.isfile(file)):
		print('file not exist')
		return None
	board_agency_year = pd.read_csv(file, names= ['year','agency','board','num'])

	board_agency_2017 = board_agency_year.loc[board_agency_year.year == 2017]
	board_agency_2017 = board_agency_2017.pivot(index = 'agency',columns = 'board',values='num').reset_index()
	labels_agency = list(board_agency_2017.agency)

	board_agency_2017 = board_agency_2017.iloc[:,2:]
	board_agency_2017 = board_agency_2017.fillna(0)

	labels_board = list(board_agency_2017.columns)

	# Initialize figure by creating upper dendrogram
	data = [
	    go.Heatmap(
	        z=np.array(board_agency_2017),
	        x=labels_board,
	        y=labels_agency,
	        colorscale='Viridis',
	    )
	]

	layout = go.Layout(
	    title='Numbers of complaints of different Brought ',
	    xaxis = dict(ticks='', nticks=36),
	    yaxis = dict(ticks='' )
	)

	fig = go.Figure(data=data, layout=layout)

	save_path = os.path.join(save_path, save_name)
	plot(fig, filename=save_path + '.html', auto_open=False)


# fig 6

def plot_scatter_matrix(file, save_path = '../../figs/', save_name = 'scatter_matrix'):
	if(not os.path.isfile(file)):
		print('file not exist')
		return None
	scatter_matrix = pd.read_csv(file, names= ['year','brought','num'])
	scatter_matrix.num = scatter_matrix.num / 10000.0
	scatter_matrix = scatter_matrix.pivot(index = 'year', columns ='brought', values = 'num').reset_index()
	scatter_matrix.year = scatter_matrix.year.astype(np.str)
	scatter_matrix.fillna(0)

	fig = ff.create_scatterplotmatrix(scatter_matrix, diag='box', index='year',
	                                  height=800, width=800,title = 'Distribution of every brought each year')
	save_path = os.path.join(save_path, save_name)
	plot(fig, filename=save_path + '.html', auto_open=False)


# fig 7

def plot_bubble_average_agency(file, save_path = '../../figs/', save_name = 'bubble_average_agency'):
	if(not os.path.isfile(file)):
		print('file not exist')
		return None
	bubble_average_agency = pd.read_csv(file, names= ['year','agency','total','average'])

	years = np.unique(bubble_average_agency.year)
	agency = np.unique(bubble_average_agency.agency)
	data = []
	for i in agency:
		data.append(go.Scatter(
	        x = list(bubble_average_agency.loc[bubble_average_agency.agency == i ]['total']),
	        y = list(bubble_average_agency.loc[bubble_average_agency.agency == i ]['average']),
	        name = i,
	        mode = 'markers',
	        
	        ))

	layout = dict(title = 'Average and total number of complaints in each agency',
	              yaxis = dict(zeroline = False),
	              xaxis = dict(zeroline = False)
	             )

	fig = dict(data=data, layout=layout)


	save_path = os.path.join(save_path, save_name)

	plot(fig, filename=save_path + '.html', auto_open=False)


# fig 8 

def plot_map_zip_amount(file, save_path = '../../figs/', save_name = 'map_zip_amount'):
	if(not os.path.isfile(file)):
		print('file not exist')
		return None
	map_zip_amount = pd.read_csv(file, names= ['year','zip','num'])
	map_zip_amount.zip = map_zip_amount.zip.astype(np.str)
	map_zip_amount = map_zip_amount.loc[map_zip_amount.year == 2016]

	with open('../../data/nyc-zip-code-tabulation-areas-polygons.json') as f:
	    geo_data = json.load(f)
	m = folium.Map([40.7,-73.9], zoom_start=11)

	m.choropleth(
	    geo_str=geo_data,
	    data=map_zip_amount,
	    columns=['zip', 'num'],
	    key_on='feature.properties.postalCode',
	    fill_color='BuPu',
	    )

	save_path = os.path.join(save_path, save_name)
	m.save(save_path+'.html')

# fig 9 

def plot_weekday_agency(file, save_path = '../../figs/', save_name = 'weekday_agency'):
	if(not os.path.isfile(file)):
		print('file not exist')
		return None
	weekday_agency = pd.read_csv(file, names= ['year','week','hour','num'])
	years = np.unique(weekday_agency.year)

	def combine(x):
	    weekdays = ['Mon','Tue','Wed','Tur','Fri','Sat','Sun']
	    return weekdays[int(x['week'])] +'_' + str(x['hour'])

	weekday_agency['week_hour'] = weekday_agency.apply(lambda x: combine(x), axis = 1)
	weekday_agency = weekday_agency.sort_values(['week','hour']).loc[:,['year','week_hour','num']]

	data = []
	for i in years:
	    data.append(go.Scatter(
	        x = weekday_agency.loc[weekday_agency.year == i]['week_hour'],
	        y = weekday_agency.loc[weekday_agency.year == i]['num'],
	        mode='lines',
	        name= i ,
	        hoverinfo='name',
	        line=dict(
	            shape='spline'
	        )))

	layout = dict(
	    legend=dict(
	        y=0.5,
	        traceorder='reversed',
	        font=dict(
	            size=16
	        )
	    ),
	    title = 'Number of complaits distribution on each day every year',
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

	board_agency_year = 'board_agency_year.txt'
	plot_board_agency_year(board_agency_year)


	scatter_matrix = 'scatter_matrix.txt'
	plot_scatter_matrix(scatter_matrix)

	bubble_average_agency = 'bubble_average_agency.txt'
	plot_bubble_average_agency(bubble_average_agency)

	map_zip_amount = 'map_zip_amount.txt'
	plot_map_zip_amount(map_zip_amount)


	weekday_agency = 'weekday_agency.txt'
	plot_weekday_agency(weekday_agency)
