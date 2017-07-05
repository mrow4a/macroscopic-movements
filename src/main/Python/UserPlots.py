

import random
import numpy as np
import csv
import matplotlib.pyplot as plt

def GraphPlot(count,cluster,outlier,title):
    Cl = np.array(cluster).astype(np.int)
    Outl = np.array(outlier).astype(np.int)

    fig, ax = plt.subplots()

    index = np.arange(count)
    bar_width = 0.35

    opacity = 0.4
    error_config = {'ecolor': '0.3'}

    fig = plt.bar(index, Cl, bar_width,
                  alpha=opacity,
                  color='b',
                  error_kw=error_config,
                  label='Clusters')

    ax = plt.bar(index + bar_width, Outl, bar_width,
                 alpha=opacity,
                 color='r',
                 error_kw=error_config,
                 label='Outliers')

    plt.xlabel('User')
    plt.ylabel('Count')
    plt.title(title)
    plt.xticks(index + bar_width / 2 )#('A', 'B', 'C', 'D', 'E'))
    plt.legend()

    plt.tight_layout()
    plt.show()

  #  stack_bar_plot = category_group.unstack().plot(kind='bar',stacked=True,title="Total Sales by Customer",figsize=(9, 7))
  #  stack_bar_plot.set_xlabel("Customers")
  #  stack_bar_plot.set_ylabel("Sales")
  #  stack_bar_plot.legend(["Total","Belts","Shirts","Shoes"], loc=9,ncol=4)
  #  plt.show()


def main():
    ClusterGr1 = []
    ClusterGr2=[]
    ClusterGr3 = []
    OutlierGr1=[]
    OutlierGr2=[]
    OutlierGr3 = []
    UserGr1=[]
    UserGr2=[]
    UserGr3 = []
    count1 =0
    count2=0
    count3 =0
    filepath = 'resources/python/ClusterVSOutlier.csv'
    with open(filepath) as f:
        lines = f.readlines()

        for line in lines:
            current_line = line.strip().split(',')
            if(current_line[1] == '0'):
                count1 += 1
                UserGr1.append(current_line[0])
                ClusterGr1.append(current_line[1])    #.astype(np.float)
                OutlierGr1.append(current_line[2])    #.astype(np.float)
            elif(current_line[1] >= current_line[2]):
                count2 += 1
                UserGr2.append(current_line[0])
                ClusterGr2.append(current_line[1])
                OutlierGr2.append(current_line[2])
            #elif(current_line[1]!=0 and current_line[1] < current_line[2]):
            else:
                count3 += 1
                UserGr3.append(current_line[0])
                ClusterGr3.append(current_line[1])
                OutlierGr3.append(current_line[2])
    print "Here"
    print count1
    print count2
    print count3
    GraphPlot(count1,ClusterGr1,OutlierGr1,'For number of Clusters = 0')
    GraphPlot(count2,ClusterGr2,OutlierGr2,'For number of Clusters > Outliers')
    GraphPlot(count3,ClusterGr3,OutlierGr3,'For number of Clusters(!=0) < Outliers')

    #plot of different counts

    count = []
    a = np.arange(2000).astype(np.int)
    count.append(count1)
    count.append(count2)
    count.append(count3)
    plt.bar(count,a)




if __name__ == '__main__':
    main()
