# Standard imports
from os import scandir
from pathlib import Path
from shutil import copy, rmtree

class Upload:
    """Class that uploads data to AWS for processing by Confluence workflow.

    Attributes
    ----------
    instance: dict
        dictionary of data for EC2 instance and EFS mount used to upload data
    sos_fs: S3FileSystem
        references SWORD of Science S3 bucket
    swot_fs: S3FileSystem
        references PO.DAAC SWOT S3 bucket

    Methods
    -------
    upload()
        uploads SWOT and transfers SOS data to EFS via EC2 instance
    __upload_swot()
        uploads SWOT NetCDF files to EFS mount and deletes local files
    __upload_sos()
        transfers data from S3 bucket to EFS mount
    """

    def __init__(self, sos_fs, swot_fs):
        """
        Parameters
        ----------
        ## instance_name: str
            Name of EC2 instance used for upload and transfer
        ## instance_id: str
            Identifier of EC2 instance used for upload and transfer
        sos_fs: S3FileSystem
            references SWORD of Science S3 bucket
        swot_fs: S3FileSystem
           references PO.DAAC SWOT S3 bucket

        ## TODO:
        - Add instance parameters and populate instance dict attribute
        """

        self.instance = {}
        self.sos_fs = sos_fs
        self.swot_fs = swot_fs

    def upload_data(self):
        """Upload SWOT and SoS data to EFS mount via EC2 instance.
        
        ## TODO
        - Implement
        """

        raise NotImplementedError

    def upload_data_local(self, data_dir, temp_dir):
        """Copy data to local directory and remove temporary directory."""

        with scandir(Path(temp_dir.name)) as files:
            for file in files:
                copy(file, (data_dir / file.name))
        
        rmtree(temp_dir.name)

    def __upload_sos(self):
        """Transfers SOS from S3 bucket to EFS via DataSync on EC2 instance.
        
        ## TODO: 
        - Implement
        """

        raise NotImplementedError

    def __upload_swot(self):
        """Upload SWOT data to EFS mount via EC2 instance.
        
        ## TODO: 
        - Implement
        """

        raise NotImplementedError