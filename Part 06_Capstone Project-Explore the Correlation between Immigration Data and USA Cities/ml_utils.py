
## for data
import numpy as np
import pandas as pd

## for plotting
import matplotlib.pyplot as plt
import seaborn as sns
import itertools

## for statistical tests
import scipy
import statsmodels.formula.api as smf
import statsmodels.api as sm
import ppscore

## for machine learning
# from sklearnex import patch_sklearn
# patch_sklearn() # Dynamically patches scikit-learn estimators to, much faster

from sklearn import preprocessing, impute, utils, linear_model, feature_selection, model_selection, metrics, decomposition, cluster, ensemble
# import imblearn

## for deep learning
from tensorflow.keras import models, layers
import minisom

## for explainer
from lime import lime_tabular
import shap

## for geospatial
import folium
import geopy



###############################################################################
#                       DATA ANALYSIS                                         #
###############################################################################

'''
Get a general overview of a dataframe.
:parameter
    :param dtf: dataframe - input data
    :param max_cat: num - mininum number of recognize column type
'''
def dtf_overview(dtf, max_cat=20, figsize=(10,5)):
    ## recognize column type
    dic_cols = {col:utils_recognize_type(dtf, col, max_cat=max_cat) for col in dtf.columns}
        
    ## print info
    len_dtf = len(dtf)
    print("Shape:", dtf.shape)
    print("-----------------")
    for col in dtf.columns:
        info = col+" --> Type:"+dic_cols[col]
        info = info+" | Nas: "+str(dtf[col].isna().sum())+"("+str(int(dtf[col].isna().mean()*100))+"%)"
        if dic_cols[col] == "cat":
            info = info+" | Categories: "+str(dtf[col].nunique())
        else:
            info = info+" | Min-Max: "+"({x})-({y})".format(x=str(int(dtf[col].min())), y=str(int(dtf[col].max())))
        if dtf[col].nunique() == len_dtf:
            info = info+" | Possible PK"
        print(info)
                
    ## plot heatmap
    fig, ax = plt.subplots(figsize=figsize)
    heatmap = dtf.isnull()
    for k,v in dic_cols.items():
        if v == "num":
            heatmap[k] = heatmap[k].apply(lambda x: 0.5 if x is False else 1)
        else:
            heatmap[k] = heatmap[k].apply(lambda x: 0 if x is False else 1)
    sns.heatmap(heatmap, vmin=0, vmax=1, cbar=False, ax=ax).set_title('Dataset Overview')
    #plt.setp(plt.xticks()[1], rotation=0)
    plt.show()
    
    ## add legend
    print("\033[1;37;40m Categerocial \033[m", "\033[1;30;41m Numerical \033[m", "\033[1;30;47m NaN \033[m")



'''
Plots the frequency distribution of a dtf column.
:parameter
    :param dtf: dataframe - input data
    :param x: str - column name
    :param max_cat: num - max number of uniques to consider a numerical variable as categorical
    :param top: num - plot setting
    :param show_perc: logic - plot setting
    :param bins: num - plot setting
    :param quantile_breaks: tuple - plot distribution between these quantiles (to exclude outilers)
    :param box_logscale: logic
    :param figsize: tuple - plot settings
'''
def freqdist_plot(dtf, x, max_cat=20, top=20, show_perc=True, bins=100, quantile_breaks=(0,10), box_logscale=False, figsize=(10,5)):
    try:
        ## cat --> freq
        if utils_recognize_type(dtf, x, max_cat) == "cat":   
            ax = dtf[x].value_counts().head(top).sort_values().plot(kind="barh", figsize=figsize)
            totals = []
            for i in ax.patches:
                totals.append(i.get_width())
            if show_perc == False:
                for i in ax.patches:
                    ax.text(i.get_width()+.3, i.get_y()+.20, str(i.get_width()), fontsize=10, color='black')
            else:
                total = sum(totals)
                for i in ax.patches:
                    ax.text(i.get_width()+.3, i.get_y()+.20, str(round((i.get_width()/total)*100, 2))+'%', fontsize=10, color='black')
            ax.grid(axis="x")
            plt.suptitle(x, fontsize=20)
            plt.show()
            
        ## num --> density
        else:
            fig, ax = plt.subplots(nrows=1, ncols=2, sharex=False, sharey=False, figsize=figsize)
            fig.suptitle(x, fontsize=20)
            ### distribution
            ax[0].title.set_text('distribution')
            variable = dtf[x].fillna(dtf[x].mean())
            breaks = np.quantile(variable, q=np.linspace(0, 1, 11))
            variable = variable[ (variable > breaks[quantile_breaks[0]]) & (variable < breaks[quantile_breaks[1]]) ]
            sns.distplot(variable, hist=True, kde=True, kde_kws={"shade":True}, ax=ax[0])
            des = dtf[x].describe()
            ax[0].axvline(des["25%"], ls='--')
            ax[0].axvline(des["mean"], ls='--')
            ax[0].axvline(des["75%"], ls='--')
            ax[0].grid(True)
            des = round(des, 2).apply(lambda x: str(x))
            box = '\n'.join(("min: "+des["min"], "25%: "+des["25%"], "mean: "+des["mean"], "75%: "+des["75%"], "max: "+des["max"]))
            ax[0].text(0.95, 0.95, box, transform=ax[0].transAxes, fontsize=10, va='top', ha="right", 
                       bbox=dict(boxstyle='round', facecolor='white', alpha=1))
            ### boxplot 
            if box_logscale == True:
                ax[1].title.set_text('outliers (log scale)')
                tmp_dtf = pd.DataFrame(dtf[x])
                tmp_dtf[x] = np.log(tmp_dtf[x])
                tmp_dtf.boxplot(column=x, ax=ax[1])
            else:
                ax[1].title.set_text('outliers')
                dtf.boxplot(column=x, ax=ax[1])
            plt.show()   
        
    except Exception as e:
        print("--- got error ---")
        print(e)



'''
Plots a bivariate analysis.
:parameter
    :param dtf: dataframe - input data
    :param x: str - column
    :param y: str - column
    :param max_cat: num - max number of uniques to consider a numerical variable as categorical
'''
def bivariate_plot(dtf, x, y, max_cat=20, figsize=(10,5)):
    try:
        ## num vs num --> stacked + scatter with density
        if (utils_recognize_type(dtf, x, max_cat) == "num") & (utils_recognize_type(dtf, y, max_cat) == "num"):
            ### stacked
            dtf_noNan = dtf[dtf[x].notnull()]  #can't have nan
            breaks = np.quantile(dtf_noNan[x], q=np.linspace(0, 1, 11))
            groups = dtf_noNan.groupby([pd.cut(dtf_noNan[x], bins=breaks, duplicates='drop')])[y].agg(['mean','median','size'])
            fig, ax = plt.subplots(figsize=figsize)
            fig.suptitle(x+"   vs   "+y, fontsize=20)
            groups[["mean", "median"]].plot(kind="line", ax=ax)
            groups["size"].plot(kind="bar", ax=ax, rot=45, secondary_y=True, color="grey", alpha=0.3, grid=True)
            ax.set(ylabel=y)
            ax.right_ax.set_ylabel("Observazions in each bin")
            plt.show()
            ### joint plot
            sns.jointplot(x=x, y=y, data=dtf, dropna=True, kind='reg', height=int((figsize[0]+figsize[1])/2) )
            plt.show()

        ## cat vs cat --> hist count + hist %
        elif (utils_recognize_type(dtf, x, max_cat) == "cat") & (utils_recognize_type(dtf, y, max_cat) == "cat"):  
            fig, ax = plt.subplots(nrows=1, ncols=2,  sharex=False, sharey=False, figsize=figsize)
            fig.suptitle(x+"   vs   "+y, fontsize=20)
            ### count
            ax[0].title.set_text('count')
            order = dtf.groupby(x)[y].count().index.tolist()
            sns.countplot(x=x, hue=y, data=dtf, order=order, ax=ax[0])
            ax[0].grid(True)
            ### percentage
            ax[1].title.set_text('percentage')
            a = dtf.groupby(x)[y].count().reset_index()
            a = a.rename(columns={y:"tot"})
            b = dtf.groupby([x,y])[y].count()
            b = b.rename(0).reset_index()
            b = b.merge(a, how="left")
            b["%"] = b[0] / b["tot"] *100
            sns.barplot(x=x, y="%", hue=y, data=b, ax=ax[1]).get_legend().remove()
            ax[1].grid(True)
            ### fix figure
            plt.close(2)
            plt.close(3)
            plt.show()
    
        ## num vs cat --> density + stacked + boxplot 
        else:
            if (utils_recognize_type(dtf, x, max_cat) == "cat"):
                cat,num = x,y
            else:
                cat,num = y,x
            fig, ax = plt.subplots(nrows=1, ncols=3,  sharex=False, sharey=False, figsize=figsize)
            fig.suptitle(x+"   vs   "+y, fontsize=20)
            ### distribution
            ax[0].title.set_text('density')
            for i in sorted(dtf[cat].unique()):
                sns.distplot(dtf[dtf[cat]==i][num], hist=False, label=i, ax=ax[0])
            ax[0].grid(True)
            ### stacked
            dtf_noNan = dtf[dtf[num].notnull()]  #can't have nan
            ax[1].title.set_text('bins')
            breaks = np.quantile(dtf_noNan[num], q=np.linspace(0,1,11))
            tmp = dtf_noNan.groupby([cat, pd.cut(dtf_noNan[num], breaks, duplicates='drop')]).size().unstack().T
            tmp = tmp[dtf_noNan[cat].unique()]
            tmp["tot"] = tmp.sum(axis=1)
            for col in tmp.drop("tot", axis=1).columns:
                tmp[col] = tmp[col] / tmp["tot"]
            tmp.drop("tot", axis=1)[sorted(dtf[cat].unique())].plot(kind='bar', stacked=True, ax=ax[1], legend=False, grid=True)
            ### boxplot   
            ax[2].title.set_text('outliers')
            sns.boxplot(x=cat, y=num, data=dtf, ax=ax[2], order=sorted(dtf[cat].unique()))
            ax[2].grid(True)
            ### fix figure
            plt.close(2)
            plt.close(3)
            plt.show()
        
    except Exception as e:
        print("--- got error ---")
        print(e)