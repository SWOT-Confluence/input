# Standard imports
from datetime import date

# Third-party imports
import asyncio
import dataretrieval.nwis as nwis
import numpy as np
import pandas as pd

# Local imports
from src.gage_pull.GageRead import GageRead

class GagePull:
    """Class that pulls USGS Gage data and appends it to the SoS.
    
    Attributes
    ----------
    end_date: str
        Date to end search for
    start_date: str
        Date to start search for 
    usgs_dict: dict 
        Dictionary of USGS data  
    usgs_targets: Path
        Path to USGS targets file

    Methods
    -------
    gather_records(sites)
        Creates and returns a list of dataframes for each NWIS record
    get_record(site)
        Get NWIS record
    pull() 
        Pulls USGS data and flags and stores in usgs_dict
    """

    def __init__(self, usgs_targets, start_date, end_date):
        """
        Parameters
        ----------
        usgs_targets: Path
            Path to USGS targets file
        start_date: str
            Date to start search for
        end_date: str
            Date to end search for
        """
        
        self.usgs_targets = usgs_targets
        self.start_date = start_date
        self.end_date = end_date
        self.usgs_dict = {}

    async def get_record(self, site):
        """Get NWIS record.
        
        Parameter
        ---------
        site: str
            Site identifier
        """

        return nwis.get_record(sites=site, service='dv', start= self.start_date, end= self.end_date)

    async def gather_records(self, sites):
        """Creates and returns a list of dataframes for each NWIS record.
        
        Parameters
        ----------
        sites: dict
            Dictionary of USGS data needed to download a record
        """

        records = await asyncio.gather(*(self.get_record(site) for site in sites))
        return records

    def pull(self):
        """Pulls USGS data and flags and stores in usgs_dict."""

        #define date range block here
        ALLt=pd.date_range(start=self.start_date,end=self.end_date)
        gage_read = GageRead(self.usgs_targets)
        dataUSGS, reachID = gage_read.read()
        dataUSGS = dataUSGS[:10]
        reachID = reachID[:10]
        
        # Download records and gather a list of dataframes
        df_list = asyncio.run(self.gather_records(dataUSGS))

        # generate empty arrays for nc output
        EMPTY=np.nan
        MONQ=np.full((len(dataUSGS),12),EMPTY)
        Qmean=np.full((len(dataUSGS)),EMPTY)
        Qmin=np.full((len(dataUSGS)),EMPTY)
        Qmax=np.full((len(dataUSGS)),EMPTY)
        FDQS=np.full((len(dataUSGS),20),EMPTY)
        TwoYr=np.full(len(dataUSGS),EMPTY)
        Twrite=np.full((len(dataUSGS),len(ALLt)),EMPTY)
        Qwrite=np.full((len(dataUSGS),len(ALLt)),EMPTY)

        # Extract data from NWIS dataframe records
        for i in range(len(dataUSGS)):
            if df_list[i].empty is False and '00060_Mean' in df_list[i] :        
                # create boolean from quality flag       
                Mask=gage_read.flag(df_list[i]['00060_Mean_cd'])
                # pull in Q
                Q=df_list[i]['00060_Mean']
                Q=Q[Mask]
                if Q.empty is False:
                    print(i)
                    Q=Q.to_numpy()
                    Q=Q*0.0283168#convertcfs to meters        
                    T=df_list[i].index.values        
                    T=pd.DatetimeIndex(T)
                    T=T[Mask]
                    moy=T.month
                    yyyy=T.year
                    moy=moy.to_numpy()       
                    thisT=np.zeros(len(T))
                    for j in range((len(T))):
                        thisT=np.where(ALLt==np.datetime64(T[j]))
                        Qwrite[i,thisT]=Q[j]
                        Twrite[i,thisT]=date.toordinal(T[j])+1
                    # with df pulled in run some stats
                    #basic stats
                    Qmean[i]=np.nanmean(Q)
                    Qmax[i]=np.nanmax(Q)
                    Qmin[i]=np.nanmin(Q)
                    #monthly means
                    Tmonn={}    
                    for j in range(12):
                        Tmonn=np.where(moy==j+1)
                        if not np.isnan(Tmonn).all() and Tmonn: 
                            MONQ[i,j]=np.nanmean(Q[Tmonn])
                            
                    #flow duration curves (n=20)
                        
                    p=np.empty(len(Q))  
                    
                    for j in range(len(Q)):
                        p[j]=100* ((j+1)/(len(Q)+1))           
                    
                    
                    thisQ=np.flip(np.sort(Q))
                    FDq=thisQ
                    FDp=p
                    FDQS[i]=np.interp(list(range(1,99,5)),FDp,FDq)
                    #FDPS=list(range(0,99,5))
                    # Two year recurrence flow
                    
                    Yy=np.unique(yyyy); 
                    Ymax=np.empty(len(Yy))  
                    for j in range(len(Yy)):
                        Ymax[j]=np.nanmax(Q[np.where(yyyy==Yy[j])]);
                
                    MAQ=np.flip(np.sort(Ymax))
                    m = (len(Yy)+1)/2
                    
                    TwoYr[i]=MAQ[int(np.ceil(m))-1]

        Mt=list(range(1,13))
        P=list(range(1,99,5))

        self.usgs_dict = {
            "dataUSGS": dataUSGS,
            "reachId": reachID,
            "Qwrite": Qwrite,
            "Twrite": Twrite,
            "Qmean": Qmean,
            "Qmax": Qmax,
            "Qmin": Qmin,
            "MONQ": MONQ,
            "Mt": Mt,
            "P": P,
            "FDQS": FDQS,
            "TwoYr": TwoYr
        }