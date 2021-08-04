# input

**NOTE: Input currently extracts and writes Sacramento data (stored on disk) from directories organized by time step.**

Input serves as the first Confluence module and takes SWOT data from PO.DAAC S3 bucket and data from the SoS S3 bucket and makes both available to the rest of the workflow via EFS mounts in AWS.

Input takes the PO.DAAC shapefile data and extracts required attributes and writes them as either node or reach NetCDF files organized by continent. The NetCDF files are then uploaded to the appropriate EFS mount.

Input also transfer the current version of the SoS to the appropriate EFS mount.

TO DO:
- PO.DAAC S3 transfer of data functionality is not available for SWOT and needs to be tested
- Extraction of data needs to be clarified (currently can use a local function)
- Upload/transfer of data to EFS mounts needs to be implemented
- Need to clarify time steps for SWOT NetCDF files that are written from SWOT shapefiles
- Handling of time for nodes -> will nodes have different time stamps than reaches?

# installation

# setup

# execution