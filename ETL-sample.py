import sys,os
os.system('cls')

import pandas as pd
import math
import functools
import matplotlib as mpl
import matplotlib.pyplot as plt

dat = pd.read_csv('ETL-sample-CSV.csv')

### Finding the difference between timestamp entries ###

dat["ts"] = pd.to_datetime(dat.ts, format="%m/%d/%Y")

dat['day_intrvl'] =dat["ts"].diff()

### Breaking a string into parts, converting the first part to an integer, and assigning 0 to a non-numeric value ###

dat['day_intrvl'] = dat["ts"].diff().astype(str).apply(lambda x: x.split()[0]).apply(lambda x: 0 if x == 'NaT' else int(x)) 

### Dividing the Data into 5 Bins and Finding the 95th Percentile ###

dat['intrvl_bins']=pd.qcut(dat['day_intrvl'],5,labels=False)
dat['intrvl95']=dat['day_intrvl'].quantile(0.95 )

### Normalizing variations to a common value, Dealing with missing values, Stripping off line feeds ###

def regexf(arg1, arg2):
    import re
    checker=re.compile('m?tor city')
    if re.search(checker,arg2):
        #print("matched!")
        return(checker.sub(arg2,arg1))
    else:
        return(arg2)

dat['city']=dat['city'].apply(lambda x: x.strip()).apply(lambda x:'unknown' if x=='None' else x)
dat['city']=dat['city'].apply(lambda x: x.replace('\\n',''))
dat['name']=dat['name'].apply(lambda x: x.strip()).apply(lambda x:'unknown' if x=='None' else x)
dat['name']=dat['name'].apply(lambda x: x.replace('\\n',''))
dat['city']=dat['city'].apply(lambda x: regexf('Detroit',x))

### Normalizing a value that is too short ###

dat['name'] = dat['name'].apply(lambda x: x if len(x) > 1 else 'invalid')

### Dealing with non-numeric values ###


dat['someval'] = dat['someval'].apply(lambda x: 10 if math.isnan(float(x)) else x)

### Synthesizing a new field based on the contents of two existing fields ###

def tax (arg1,arg2):
    if arg1<2:
        return .8 * arg2
    if arg1<5:
        return .75 * arg2    
    else:
        return .65 * arg2
dat['net']=dat.apply(lambda x: tax(x['industry_type'], float(x['someval'])),axis=1)


### Creating a Goal: The 95th Percentile ###

dat['n95'] = dat['net'].quantile(0.95) 

### Chained recalculating after a value change ###
dat['someval']=dat['someval'].apply(lambda x: 1.1 * float(x))
dat['net']=dat.apply(lambda x: tax(x['industry_type'], float(x['someval'])),axis=1)
dat['n95']=dat['net'].quantile(0.95)

### Calculating a Column Total ###

dat['totalnet'] = functools.reduce(lambda a,b: a+b,dat['net'])

### Checking entries versus a dictionary ###

statedict = {'Detroit': 'Michigan'}

dat['state'] = dat['city'].apply(lambda x: statedict[x] if x in statedict else 'unknown') 

### String Manipulation and Classification ###

def lang(somester):
    buff=''
    for x in somester:
        if x==buff:
            return ('Dutch')
        else:
            buff=x
    return('Not Dutch')

dat['lang']=dat['name'].apply(lambda x: lang(x))

### Plotting versus a Goal ###

plt.style.use('classic')
plt.figure()
plt.title('Net vs N95')
plt.ylabel('Net')
plt.xlim(0,9)
plt.ylim(0,32)
plt.grid(True)
plt.text(1,20,r'Did You enjoy $\lambda?$')
xaxis=[x for x in range(10)]
plt.plot(xaxis,dat['net'],dat['n95'])

plt.show()

