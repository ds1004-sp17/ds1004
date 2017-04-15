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