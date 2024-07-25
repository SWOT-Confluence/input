#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan 15 13:09:29 2021

@author: mtd
"""

from numpy import array,diff,ones,reshape,empty,nan,isnan,where,logical_not,shape,transpose,logical_and,delete,logical_or,sum,nonzero,arange,linspace
from netCDF4 import Dataset
import pandas as pd
import datetime

class HWS_IO:
    # def __init__(self,IOtype,obsFname):
    def __init__(self, swot_dataset, nt):             
        self.ObsData={}    
        self.TruthData={}
                     
        self.swot_dataset=swot_dataset
        self.nt = nt
        self.ReadConfluenceObs()
    
    def SubSelectData(self,iUse):
       self.ObsData["nt"]=sum(iUse)           
       self.ObsData["h"]= self.ObsData["h"][:,iUse]
       self.ObsData["w"]= self.ObsData["w"][:,iUse]
       self.ObsData["S"]= self.ObsData["S"][:,iUse]
       self.ObsData["t"]= self.ObsData["t"][iUse]

        
        
    def ReadConfluenceObs(self):

       #this file is set up to read one reach at a time! variables to be parsed in from SWORD are assigned nan for now

    #    swot_dataset = Dataset(self.obsFname)
        swot_dataset = self.swot_dataset

        #obs data is an empty dict
        self.ObsData["nR"]=1
        self.ObsData["xkm"]=nan
        self.ObsData["L"]=nan
        self.ObsData["nt"]= self.nt


        ts = swot_dataset["time"].values
        epoch = datetime.datetime(2000,1,1,0,0,0)
        tall = []
        for t in ts:
            if t > 0:
                delta = epoch + datetime.timedelta(seconds=t)
                tall.append(delta.timestamp())
            else:
                tall.append(0)

        # tall = [ epoch + datetime.timedelta(seconds=t) if t > 0 else 0 for t in ts]

        self.ObsData["t"]=array(tall)
        self.ObsData["dt"]=reshape(diff(self.ObsData["t"]).T*86400 * ones((1,self.ObsData["nR"])),(self.ObsData["nR"]*(self.ObsData["nt"]-1),1))

        self.ObsData["h"]=empty(  (self.ObsData["nR"],self.ObsData["nt"]) ) #water surface elevation (wse), [m]
        self.ObsData["h0"]=empty( (self.ObsData["nR"],1)  ) #initial wse, [m]
        self.ObsData["S"]=empty(  (self.ObsData["nR"],self.ObsData["nt"]) ) #water surface slope, [-]
        self.ObsData["w"]=empty(  (self.ObsData["nR"],self.ObsData["nt"]) ) #river top width, [m]     



        for i in range(0,self.ObsData["nR"]):
            # self.ObsData["h"][i,:]=swot_dataset["reach/wse"][0:self.ObsData["nt"]].filled(nan)
            # self.ObsData["w"][i,:]=swot_dataset["reach/width"][0:self.ObsData["nt"]].filled(nan)
            # self.ObsData["S"][i,:]=swot_dataset["reach/slope2"][0:self.ObsData["nt"]].filled(nan)
            self.ObsData["h"][i,:]=swot_dataset.iloc[0:self.ObsData["nt"]]["wse"]
            self.ObsData["w"][i,:]=swot_dataset.iloc[0:self.ObsData["nt"]]["width"]
            self.ObsData["S"][i,:]=swot_dataset.iloc[0:self.ObsData["nt"]]["slope2"]

        self.ObsData["sigh"]=0.1
        self.ObsData["sigw"]=10.0
        self.ObsData["sigS"]=1.7e-5

        #try cutting out data that are fill value 
        #iUse= logical_not(isnan(self.ObsData['h'][0,:]))

        # iDelete is used to add in missing data in final step. rest of FLaPE-Byrd uses a different convention
        self.ObsData["iDelete"]=where(isnan(self.ObsData['w'][0,:]) | \
                            isnan(self.ObsData['h'][0,:]))

        iUse = logical_not(isnan(self.ObsData['w'][0,:]) | \
                            isnan(self.ObsData['h'][0,:]))

        self.SubSelectData(iUse)

        #close dataset
        # swot_dataset.close()




