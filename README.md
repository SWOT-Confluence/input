# input

`input` reads in SWOT shapefile data from a PO.DAAC S3 bucket and produces time series data saved as NetCDF files. The input module runs on the reach-level meaning it is parallelizable by SWORD reach identifier.

`input` generates temporary S3 credentials for the PO.DAAC S3 bucket, reads the SWOT shapefiles directly into memories and extracts data for the specified reach and nodes associated with the reach identifier. One data has been extracted, the data is written to a NetCDF file.

**Note:** `input` operations have been implemented for SWOT Lake shapefiles but they need to be tested.

## installation

Build a Docker image: `docker build -t input .`

## execution

**Command line arguments:**

- -i: index to locate reach and nodes in JSON file
- -r: path to reach node JSON file
- -p: path to cycle pass JSON file
- -s: path to S3 shapefiles list JSON file
- -c: context to generate data for: 'river' or 'lake'
- -d: directory to save output to
- -l: indicates local run (optional)
- -f: name of shapefile directory for local runs (optional)

**Execute a Docker container:**

AWS credentials will need to be passed as environment variables to the container so that `input` may access AWS infrastructure to generate JSON files.

```bash
# Credentials
export aws_key=XXXXXXXXXXXXXX
export aws_secret=XXXXXXXXXXXXXXXXXXXXXXXXXX

# Docker run command
docker run --rm --name input -e AWS_ACCESS_KEY_ID=$aws_key -e AWS_SECRET_ACCESS_KEY=$aws_secret -e AWS_DEFAULT_REGION=us-west-2 -e AWS_BATCH_JOB_ARRAY_INDEX=26 -v /mnt/input:/data input:latest -i "-235" -r /data/reach_node.json -p /data/cycle_passes.json -s /data/s3_list.json -d /data/swot -c river -d /data/swot
```

## tests

1. Run the unit tests: `python3 -m unittest discover tests`

## deployment

There is a script to deploy the Docker container image and Terraform AWS infrastructure found in the `deploy` directory.

Script to deploy Terraform and Docker image AWS infrastructure

REQUIRES:

- jq (<https://jqlang.github.io/jq/>)
- docker (<https://docs.docker.com/desktop/>) > version Docker 1.5
- AWS CLI (<https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html>)
- Terraform (<https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli>)

Command line arguments:

[1] registry: Registry URI
[2] repository: Name of repository to create
[3] prefix: Prefix to use for AWS resources associated with environment deploying to
[4] s3_state_bucket: Name of the S3 bucket to store Terraform state in (no need for s3:// prefix)
[5] profile: Name of profile used to authenticate AWS CLI commands

Example usage: ``./deploy.sh "account-id.dkr.ecr.region.amazonaws.com" "container-image-name" "prefix-for-environment" "s3-state-bucket-name" "confluence-named-profile"`

Note: Run the script from the deploy directory.
