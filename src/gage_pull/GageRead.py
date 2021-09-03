# Third-party imports
from netCDF4 import Dataset
import numpy as np
import pandas as pd

class GageRead:
    """Class that reads in USGS site data needed to download NWIS records.
    
    Attributes
    ----------
    usgs_targets: Path
        Path to USGS targets file    

    Methods
    -------
    flag()
        Flag USGS data
    read()
        Reads USGS flags and data
    """

    def __init__(self, usgs_targets):
        """
        Parameters
        ----------
        usgs_targets: Path
            Path to USGS targets file
        """

        self.usgs_targets = usgs_targets

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