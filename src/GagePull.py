# Standard imports
from datetime import date, datetime

# Third-party imports
import asyncio
import dataretrieval.nwis as nwis
from netCDF4 import Dataset
import numpy as np
import pandas as pd

class GagePull:
    """Class that pulls USGS Gage data and appends it to the SoS.
    
    Attributes
    ----------
    end_date: str
        Date to end search for
    sos_dir: Path
        Path to SoS directory
    start_date: str
        Date to start search for
    usgs_targets: Path
        Path to USGS targets file    

    Methods
    -------
    append()
        Appends USGS data to the SoS
    flag()
        Flag USGS data
    read()
        Reads USGS flags and data
    pull() 
        Pulls USGS data and flags
    """

    def __init__(self, sos_dir, usgs_targets, start_date, end_date):
        """
        Parameters
        ----------
        sos_dir: Path
            Path to SoS directory
        usgs_targets: Path
            Path to USGS targets file
        start_date: str
            Date to start search for
        end_date: str
            Date to end search for
        """

        self.sos_dir = sos_dir
        self.usgs_targets = usgs_targets
        self.start_date = start_date
        self.end_date = end_date

    def flag(self, In):
        """Flag USGS data.
        
        Parameters
        ----------
        In: ?type
            ?description
        """
        In = In.replace(np.nan,'*', regex=True)

        # e   Value has been edited or estimated by USGS personnel and is write protected
        # &     Value was computed from affected unit values
        # E     Value was computed from estimated unit values.
        # A     Approved for publication -- Processing and review completed.
        # P     Provisional data subject to revision.
        # <     The value is known to be less than reported value and is write protected.
        # >     The value is known to be greater than reported value and is write protected.
        # 1     Value is write protected without any remark code to be printed
        # 2     Remark is write protected without any remark code to be printed
        #       No remark (blank)
        M={}
        for i in range(len(In)):
            if 'Ice' in In[i] or '*' in In[i]:
                M[i]=False
            else:
                M[i]=True

        Mask=pd.array(list(M.values()),dtype="boolean")
        return Mask

    async def get_record(self , site):
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

        records = await asyncio.gather(*(self.get_record(site) for site in sites.values()))
        return records

    def pull(self):
        """Pulls USGS data and flags."""

        #define date range block here
        ALLt=pd.date_range(start=self.start_date,end=self.end_date)
        dataUSGS, reachID = self.read()
        data = {
            1: dataUSGS[1],
            2: dataUSGS[2],
            3: dataUSGS[3],
            4: dataUSGS[4],
            5: dataUSGS[5],
        }
        
        # Download records and gather a list of dataframes
        df_list = asyncio.run(self.gather_records(data))
        
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
        for i in range(len(data)):
            if df_list[i].empty is False and '00060_Mean' in df_list[i] :        
                # create boolean from quality flag       
                Mask=self.flag(df_list[i]['00060_Mean_cd'])
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
                    FDp=p;
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
        
        print(reachID)
        print(reachID.shape)
        print(Qmean.shape)
        # W2cdf.write(dataUSGS,reachID,Qwrite,Twrite,Qmean,Qmax,Qmin,MONQ,Mt,P,FDQS,TwoYr)

    def read(self):
        """Read USGS data."""
       
        ncf = Dataset(self.usgs_targets)
        #
        dataUSGS=np.ma.getdata(ncf.variables['STAID'][:])
        dataUSGS=np.char.decode(dataUSGS)
        #
        reachID = np.ma.getdata(ncf.variables['reach_id'][:])
        reachID=np.char.decode(reachID)
        USt={}
        Rt={}
        for i in range(len(reachID)):
            US=','.join(dataUSGS[i,:])
            USt[i]=US.replace(',','')
            #
            R=','.join( reachID[i,:])
            Rt[i]=R.replace(',','')
            
        dataUSGS = USt
        reachID = Rt
        return dataUSGS, reachID
