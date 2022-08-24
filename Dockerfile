# This dockerfile is for mybinder.org support for running examples.
# It's *not* required for using ibis-datasette, and should mostly
# be ignored.
FROM python:3.10-slim

ENV USER jovyan
ENV HOME /home/jovyan

RUN adduser --disabled-password --gecos "Default user" --uid 1000 ${USER}

RUN apt-get update && \
    apt-get install -y ffmpeg git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install dependencies
RUN pip install --no-cache --upgrade pip \
    && pip install --no-cache \
        notebook \
        jupyterlab \
        sqlglot \
        pydub \
        ipywidgets \
        git+https://github.com/ibis-project/ibis.git \
        git+https://github.com/jcrist/ibis-datasette.git \
    && find /usr/local/lib/python3.10/site-packages/ -follow -type f -name '*.a' -delete \
    && find /usr/local/lib/python3.10/site-packages/ -follow -type f -name '*.pyc' -delete \
    && find /usr/local/lib/python3.10/site-packages/ -follow -type f -name '*.js.map' -delete

COPY --chown=1000 examples ${HOME}

USER ${USER}
WORKDIR ${HOME}
