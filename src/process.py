import numpy as NP
import matplotlib.pyplot as PLT
import os


# create result folder to store figures if not exists
result_dir = "result"
if not os.path.exists(result_dir):
     os.makedirs(result_dir)

def plot_nums_in_month_by_agency_year():
    """
    plot number of daily complaints per month, grouped by agency, 
    saved at @result_dir
    """
    filename = "nums_in_month_by_agency_year.txt"
    dic = {} # years * months
    min_year = 2010
    max_year = 2017
    NMONTH = 12

    with open(filename, 'r') as f:
        for line in f:
            k, v = line.strip().split("\t")
            agency, y = k.split(", ")
            if int(y) <= 2009:
                continue

            m, n, avg = v.split(", ")
            if agency not in dic:
                dic[agency] = NP.zeros((max_year-min_year+1, NMONTH))
            dic[agency][int(y)-min_year][int(m)-1] = float(avg)

    print("data loaded.")

    for k, v in dic.items():
        agency = k.split(", ")[0]
        years, months = v.shape
        
        # set colors and line styles to read clearer
        colors = ("#FF0000", "#FF7F00", "#FFFF00", "#7FFF00", 
                "#00FF00", "#00FF7F", "#00FFFF", "#007FFF", 
                "#0000FF", "#7F00FF", "#FF00FF", "#FF007F"
                )
        line_styles = ("solid", "dashed", "-", "--", "-.", ":")

        PLT.figure(figsize=(15, 10)) # set figure size
        month_array = range(1, NMONTH+1)
        for y in range(years):
            PLT.plot(month_array, v[y], 
                    c=colors[y], 
                    ls=line_styles[y%len(line_styles)])

        PLT.legend([i+2010 for i in range(years)], 
                loc="center left", bbox_to_anchor=(1, 0.5))
        PLT.ylabel("Daily Complaints")
        PLT.xlabel("Month")
        PLT.grid()
        PLT.savefig(result_dir + "/" + agency + ".png")
        PLT.close()

        print(agency + " exported.")


if __name__ == "__main__":
    plot_nums_in_month_by_agency_year()

