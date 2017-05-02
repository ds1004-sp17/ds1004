import numpy as NP
import matplotlib
import matplotlib.pyplot as PLT
import os
import operator


# create result folder to store figures if not exists
result_dir = "../fig"
if not os.path.exists(result_dir):
     os.makedirs(result_dir)

def plot_nums_in_month_by_year():
    """
    plot number of daily complaints per month, grouped by agency, 
    saved at @result_dir
    """
    filename = "water_sys_year.txt"
    min_year = 2010
    max_year = 2016
    NMONTH = 12
    stat = NP.zeros([max_year-min_year+1, NMONTH])

    with open(filename, 'r') as f:
        for line in f:
            k, v = line.strip().split("\t")
            y, m = k.split(", ")
            y = int(y)
            m = int(m)
            c = int(v)
            
            stat[y-min_year][m-1] = c

    print("data loaded.")

    years = stat.shape[0]
    
    # set colors and line styles to read clearer
    colors = ("#FF0000", "#FF7F00", "#FFFF00", "#7FFF00", 
            "#00FF00", "#00FF7F", "#00FFFF", "#007FFF", 
            "#0000FF", "#7F00FF", "#FF00FF", "#FF007F"
            )
    line_styles = ("solid", "dashed", "-", "--", "-.", ":")

    PLT.figure(figsize=(15, 10)) # set figure size
    month_array = range(1, NMONTH+1)
    for y in range(years):
        PLT.plot(month_array, stat[y], 
                c=colors[y], 
                ls=line_styles[y%len(line_styles)])

    PLT.legend([i+2010 for i in range(years)], 
            loc="center left", bbox_to_anchor=(1, 0.5))
    PLT.ylabel("Number of Water System Complaints")
    PLT.xlabel("Month")
    PLT.grid()
    PLT.savefig(result_dir + "/waterSys.png")
    PLT.close()


def plt_water_sys_desc():
    filename = "water_sys_desc.txt"
    stat = {}

    with open(filename, 'r') as f:
        for line in f:
            k, v = line.strip().split("\t")
            stat[k] = int(v)

    top10 = sorted(stat.items(), key=operator.itemgetter(1), reverse=True)[:10]

    keys = [x[0] for x in top10]
    values = [x[1] for x in top10]

    matplotlib.rc('xtick', labelsize=20) 
    matplotlib.rc('ytick', labelsize=20) 

    # Set the font dictionaries (for plot title and axis titles)
    title_font = {'fontname':'Sans', 'size':'26', 'color':'black', 'weight':'normal',
                          'verticalalignment':'bottom'} # Bottom vertical alignment for more space
    axis_font = {'fontname':'Sans', 'size':'30'}

    PLT.figure(figsize=(30, 15))
    for i in range(len(keys)):
        PLT.bar(i+0.5, values[i], align="center")
    PLT.legend(keys, prop={"size":20}, 
            loc="upper right")
    PLT.xlabel("Description", **axis_font)
    PLT.ylabel("Counts", **axis_font)
    PLT.savefig(result_dir + "/waterSysDistr.png")
    PLT.close()


def plot_correlation():
    weather_filename = "weather.txt"
    complaint_filename = "water_sys_sorted.txt"

    high_temp_data = []
    low_temp_data = []
    complaint_data = []

    with open(weather_filename, "r") as f:
        for line in f:
            if "," in line:
                data = line.strip()[1:-1].split(", ")
                high_temp_data.append(int(data[1]))
                low_temp_data.append(int(data[-2]))

    with open(complaint_filename, "r") as f:
        for line in f:
            line = line.strip()
            c = int(line.split("\t")[1])
            complaint_data.append(c)

    high_temp_data = NP.array(high_temp_data) / max(high_temp_data)
    low_temp_data = NP.array(low_temp_data) / max(low_temp_data)
    complaint_data = NP.array(complaint_data) / max(complaint_data)

    matplotlib.rc('xtick', labelsize=20) 
    matplotlib.rc('ytick', labelsize=20) 

    # Set the font dictionaries (for plot title and axis titles)
    title_font = {'fontname':'Sans', 'size':'26', 'color':'black', 'weight':'normal',
                          'verticalalignment':'bottom'} # Bottom vertical alignment for more space
    axis_font = {'fontname':'Sans', 'size':'30'}

    PLT.figure(figsize=(30, 15))

    x = list(range(1, 13)) * 7
    PLT.plot(range(len(x)), high_temp_data)
    PLT.plot(range(len(x)), low_temp_data)
    PLT.plot(range(len(x)), complaint_data)
    legend = ["Monthly average of max temperature", 
            "Monthly average of min temperature", 
            "Monthly complaints"]
    PLT.legend(legend, prop={"size":20},  ncol=3,
            loc="upper center", bbox_to_anchor=(0.5, 1.05))

    PLT.xticks(range(len(x)), x)
    PLT.xlabel("Month (from year 2010 to 2016)", **axis_font)
    PLT.ylabel("Number of Water System Complaints", **axis_font)
    PLT.savefig(result_dir + "/tempCorr.png")
    PLT.close()

    """
    compute correlation
    """
    high_corr = NP.corrcoef(NP.array(high_temp_data, dtype=NP.float32), 
            NP.array(complaint_data, dtype=NP.float32))

    low_corr = NP.corrcoef(NP.array(low_temp_data, dtype=NP.float32), 
            NP.array(complaint_data, dtype=NP.float32))

    print(high_corr[0,1], low_corr[0,1])

if __name__ == "__main__":
    #plot_nums_in_month_by_year()
    #plt_water_sys_desc()
    plot_correlation()

