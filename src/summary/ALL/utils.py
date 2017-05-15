from datetime import datetime

# Fliter for incident zip
def filter_zip(zipcode):
		try:
			zipcode_int = int(zipcode)
			zipcode_str = str(zipcode_int)
			if len(zipcode_str) == 5:
				if zipcode_str.startswith('1') or zipcode_str == '00085':
					zipcode = zipcode_str
				return zipcode
		except ValueError:
			pass

# Filter for borough 
def filter_borough(borough):
		borough = borough.lower()
		if borough == 'brooklyn' or borough == 'manhattan' or borough == 'bronx' or borough == 'queens' or borough == 'staten island':
			borough = borough.upper()
			return borough



def check_datetime(dt):
    try:
        d = datetime.strptime(dt, "%m/%d/%Y %I:%M:%S %p")
        if d.year < 2009 or d.year > 2017:
            return False
    except ValueError:
        return False

    return True

def get_date(dt):
    d = datetime.strptime(dt, "%m/%d/%Y %I:%M:%S %p")
    return str(d.year), str(d.month), str(d.day)


def get_date2(dt):
    d = datetime.strptime(dt, "%m/%d/%Y %I:%M:%S %p")
    return str(d.date())

def solve_time(created, closed):
    created = datetime.strptime(created, "%m/%d/%Y %I:%M:%S %p")
    try:
        closed = datetime.strptime(closed, "%m/%d/%Y %I:%M:%S %p")
    except ValueError:
        closed = datetime.now()
    return (closed - created).days

def get_week(dt):
    d = datetime.strptime(dt, "%m/%d/%Y %I:%M:%S %p")
    return d.year, d.weekday(), d.hour


def sum_none(x):
    s = 0
    for i in x:
        if i == None:
            s = s
        else:
            s += i
    return s
