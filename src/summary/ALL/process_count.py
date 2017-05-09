import numpy as NP
import matplotlib
import matplotlib.pyplot as PLT


result_dir = "../fig"

def plot_count():
    filename = "dca_201211_3.txt"
    NDAYS = 30
    counts = NP.zeros(NDAYS)

    with open(filename, 'r') as f:
        for line in f:
            k, v = line.strip().split("\t")
            d = int(k)
            c = int(v)

            counts[d-1] = c

    #matplotlib.rc('xtick', labelsize=10) 
    #matplotlib.rc('ytick', labelsize=10) 
    PLT.figure(figsize=(25, 15)) # set figure size

    y_pos = 0.5 + NP.arange(NDAYS)
    PLT.bar(y_pos, counts, align='center', alpha=0.8)

    # Set the font dictionaries (for plot title and axis titles)
    title_font = {'fontname':'Sans', 'size':'26', 'color':'black', 'weight':'normal',
                          'verticalalignment':'bottom'} # Bottom vertical alignment for more space
    axis_font = {'fontname':'Sans', 'size':'20'}

    PLT.legend(range(1, 1+NDAYS), prop={"size":15}, 
            loc="center left", bbox_to_anchor=(1, 0.5))
    PLT.ylabel("DCA complaint numbers in November, 2012", **axis_font)
    PLT.xlabel("Day", **axis_font)

    PLT.grid()
    PLT.savefig(result_dir + "/201211daydistr.png")
    PLT.close()


if __name__ == "__main__":
    plot_count()
