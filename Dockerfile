# Stage 0 - Create from osge/gdal image
FROM osgeo/gdal as stage0

# Stage 1 - Debian dependencies
FROM stage0 as stage1
RUN apt update \
        && DEBIAN_FRONTEND=noninteractive apt install -y \
                curl \
                zip

# Stage 2 - Input Python dependencies
FROM stage1 as stage2
COPY requirements.txt /app/requirements.txt
RUN apt install -y python3.8-venv \
        && /usr/bin/python3 -m venv /app/env \
        && /app/env/bin/pip install -r /app/requirements.txt

# # Stage 3 - AWS CLI
FROM stage2 as stage3
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "/home/awscliv2.zip" \
        && unzip /home/awscliv2.zip -d /home \
        && /home/aws/install \
        && /usr/local/bin/aws configure set default.region us-west-2
COPY credentials /root/.aws/credentials

# Stage 5 - Copy and execute module
FROM stage3 as stage4
COPY ./input /app/input/
COPY run_input.py /app/run_input.py
LABEL version="1.0" \
        description="Containerized Input module." \
        "confluence.contact"="ntebaldi@umass.edu" \
        "algorithm.contact"="ntebaldi@umass.edu"
ENTRYPOINT ["/app/env/bin/python3", "/app/run_input.py"]