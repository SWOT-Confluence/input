# Stage 0 - Create from Python3.12 image
# FROM python:3.12-slim-bookworm as stage0
FROM python:3.12-slim-bookworm

# Stage 1 - Debian dependencies
# FROM stage0 as stage1
RUN apt update \
        && DEBIAN_FRONTEND=noninteractive apt install -y curl zip python3-dev build-essential libhdf5-serial-dev netcdf-bin libnetcdf-dev

# # Stage 2 - Input Python dependencies
# # FROM stage1 as stage2
COPY requirements.txt /app/requirements.txt
RUN /usr/local/bin/python -m venv /app/env \
        && /app/env/bin/pip install -r /app/requirements.txt

# Stage 5 - Copy and execute module
# FROM stage3 as stage4
COPY ./input /app/input/
COPY run_input.py /app/run_input.py
LABEL version="1.0" \
        description="Containerized Input module." \
        "confluence.contact"="ntebaldi@umass.edu" \
        "algorithm.contact"="ntebaldi@umass.edu"
ENTRYPOINT ["/app/env/bin/python3", "/app/run_input.py"]