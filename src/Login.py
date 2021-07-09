# Standard imports
from http.cookiejar import CookieJar
import requests
from urllib import request

# Third-party imports
import s3fs

# Application imports
from src.input_conf import token_data, sos_creds

class Login:
    """
    A class that represents login operations.
    
    A user can log into the PO.DAAC Earthdata system and/or PO.DAAC S3 and SoS
    S3 buckets.

    Attributes
    ----------
    sos_fs: S3FileSystem
        references SWORD of Science S3 bucket
    swot_fs: S3FileSystem
        references PO.DAAC SWOT S3 bucket
    SOS_S3: str
        string to SOS S3 location
    SWOT_S3: str
        string URL to access SWOT S3 credentials
    URS: str   
        string URL to Earthdata login
    Methods
    -------
    login()
        logs into Earthdata, SOS_S3, and SWOT_S3 services
    login_earthdata()
        logs into Earthdata service
    """

    SOS_S3 = "swordofscience"
    SWOT_S3 = "https://archive.podaac.earthdata.nasa.gov/s3credentials"
    URS = ""

    def __init__(self):
        self.sos_fs = None
        self.swot_fs = None

    def login(self):
        """Logs into Earthdata and accesses SWOT S3 bucket and SOS S3 bucket.
        
        Sets references to sos_fs and swot_fs attributes.
        """

        # SWOT data
        self.login_earthdata()
        response = requests.get(self.SWOT_S3).json()
        self.swot_fs = s3fs.S3FileSystem(key=response["accessKeyId"],
                                         secret=response["secretAccessKey"],
                                         token=response["sessionToken"],
                                         client_kwargs={"region_name": "us-west-2"})

        # SoS data
        self.sos_fs = s3fs.S3FileSystem(key=sos_creds["key"],
                                        secret=sos_creds["secret"],
                                        client_kwargs={"region_name": sos_creds["region"]})

    def login_earthdata(self):
        """Log into Earthdata and set up request library to track cookies."""

        # Create Earthdata authentication request
        manager = request.HTTPPasswordMgrWithDefaultRealm()
        manager.add_password(None, self.URS, token_data["user"], token_data["password"])
        auth = request.HTTPBasicAuthHandler(manager)

        # Set up the storage of cookies
        jar = CookieJar()
        processor = request.HTTPCookieProcessor(jar)

        # Define an opener to handle fetching auth request
        opener = request.build_opener(auth, processor)
        request.install_opener(opener)