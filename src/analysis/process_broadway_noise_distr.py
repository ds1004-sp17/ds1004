import numpy as NP
import matplotlib.pyplot as PLT
import os


# create result folder to store figures if not exists
result_dir = "result/"
if not os.path.exists(result_dir):
     os.makedirs(result_dir)

def plot_broadway_noise_distr():
    """
    plot number of daily complaints per month, grouped by agency, 
    saved at @result_dir
    """
    filename = "type_broadway_noise.txt"
    min_year = 2010
    max_year = 2016
    NDAY = 7
    stat = NP.zeros((max_year-min_year+1, NDAY))

    with open(filename, 'r') as f:
        for line in f:
            k, v = line.strip().split("\t")
            y = int(k)
            if y < min_year or y > max_year:
                continue

            day, n = v.split(", ")
            stat[y-min_year][int(day)-1] = int(n)

    print("data loaded.")

    # set colors and line styles to read clearer
    colors = ("#FF0000", "#FF7F00", "#FFFF00", "#7FFF00", 
            "#00FF00", "#00FF7F", "#00FFFF", "#007FFF", 
            "#0000FF", "#7F00FF", "#FF00FF", "#FF007F"
            )
    line_styles = ("solid", "dashed", "-", "--", "-.", ":")

    PLT.figure(figsize=(15, 10)) # set figure size
    day_array = range(1, NDAY+1)
    for y in range(len(stat)):
        PLT.plot(day_array, stat[y], 
                c=colors[y], 
                ls=line_styles[y%len(line_styles)])

    PLT.legend([i+2010 for i in range(len(stat))], 
            loc="center left", bbox_to_anchor=(1, 0.5))
    PLT.ylabel("Complaints in a Year")
    PLT.xlabel("Weekday")
    PLT.grid()
    PLT.savefig(result_dir + "broadway_noise.png")
    PLT.close()


if __name__ == "__main__":
    plot_broadway_noise_distr()

