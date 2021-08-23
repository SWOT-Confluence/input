# input

**NOTE: Input currently extracts and writes Sacramento data stored in a S3 bucket. It generates request fees.**

Input serves as the first Confluence module and takes SWOT data from PO.DAAC S3 bucket and data from the SoS S3 bucket and makes both available to the rest of the workflow via EFS mounts in AWS.

Input takes the PO.DAAC shapefile data and extracts required attributes and writes them as either node or reach NetCDF files organized by continent. The NetCDF files are then uploaded to the appropriate EFS mount.

Input also transfer the current version of the SoS to the appropriate EFS mount.

TO DO:
- PO.DAAC S3 transfer of data functionality is not available for SWOT and needs to be tested
- Handling of time for nodes -> will nodes have different time stamps than reaches?

# installation

# setup

# execution