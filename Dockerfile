# Stage 0 - Create from Python3.9.7 image
FROM python:3.9.7-slim-buster as stage0

# Stage 1 - Debian dependencies
FROM stage0 as stage1
RUN apt update \
        && DEBIAN_FRONTEND=noninteractive apt install -y \
                curl \
                zip \
        && /usr/bin/curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"

# Stage 2 - Input Python dependencies
FROM stage1 as stage2
COPY requirements.txt /app/requirements.txt
RUN /usr/local/bin/python3 -m venv /app/env
RUN /app/env/bin/pip install -r /app/requirements.txt

# Stage 3 - AWS CLI
FROM stage2 as stage3
RUN /usr/bin/unzip awscliv2.zip 
RUN ./aws/install 
RUN /usr/local/bin/aws configure set default.region us-west-2
COPY credentials /root/.aws/credentials

# Stage 4 - Copy Input code
FROM stage3 as stage4
COPY ./input /app/input/

# Stage 5 - Execute module
FROM stage4 as stage5
COPY run_input.py /app/run_input.py
LABEL version="1.0" \
        description="Containerized Input module." \
        "confluence.contact"="ntebaldi@umass.edu" \
        "algorithm.contact"="ntebaldi@umass.edu"
ENTRYPOINT ["/app/env/bin/python3", "/app/run_input.py"]