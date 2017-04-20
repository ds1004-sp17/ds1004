import numpy as NP
import matplotlib
import matplotlib.pyplot as PLT
import os


# create result folder to store figures if not exists
result_dir = "../fig/agency_hourly_per_month"
if not os.path.exists(result_dir):
     os.makedirs(result_dir)

def plot_nums_in_month_by_agency():
    """
    plot number of hourly complaints per month, grouped by agency, 
    saved at @result_dir
    """
    filename = "type_in_hour_monthly.txt"
    dic = {} # months * hours
    NMONTH = 12
    NHOUR = 24

    with open(filename, 'r') as f:
        for line in f:
            k, v = line.strip().split("\t")
            agency, h = k.split(", ")

            m, num = v.split(", ")
            if agency not in dic:
                dic[agency] = NP.zeros((NMONTH, NHOUR))
            dic[agency][int(m)-1][int(h)] = int(num)

    print("data loaded.")

    matplotlib.rc('xtick', labelsize=20) 
    matplotlib.rc('ytick', labelsize=20) 

    for k, v in dic.items():
        agency = k
        
        # set colors and line styles to read clearer
        colors = ("#FF0000", "#FF7F00", "#FFFF00", "#7FFF00", 
                "#00FF00", "#00FF7F", "#00FFFF", "#007FFF", 
                "#0000FF", "#7F00FF", "#FF00FF", "#FF007F"
                )
        line_styles = ("solid", "dashed", "-", "--", "-.", ":")

        PLT.figure(figsize=(25, 15)) # set figure size
        hour_array = range(NHOUR)
        for m in range(NMONTH):
            PLT.plot(hour_array, v[m], 
                    c=colors[m], 
                    ls=line_styles[m%len(line_styles)])

        # Set the font dictionaries (for plot title and axis titles)
        title_font = {'fontname':'Sans', 'size':'26', 'color':'black', 'weight':'normal',
                              'verticalalignment':'bottom'} # Bottom vertical alignment for more space
        axis_font = {'fontname':'Sans', 'size':'20'}

        PLT.legend(range(1, 1+NMONTH), prop={"size":15}, 
                loc="center left", bbox_to_anchor=(1, 0.5))
        PLT.ylabel("Number of Complaints in a Month", **axis_font)
        PLT.xlabel("Hour", **axis_font)

        PLT.grid()
        PLT.savefig(result_dir + "/" + agency + ".png")
        PLT.close()

        print(agency + " exported.")


if __name__ == "__main__":
    plot_nums_in_month_by_agency()

