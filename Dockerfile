FROM continuumio/miniconda3

WORKDIR /src/cads-retrieve-tools

COPY environment.yml /src/cads-retrieve-tools/

RUN conda install -c conda-forge gcc python=3.10 \
    && conda env update -n base -f environment.yml

COPY . /src/cads-retrieve-tools

RUN pip install --no-deps -e .
