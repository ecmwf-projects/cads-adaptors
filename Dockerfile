FROM continuumio/miniconda3

WORKDIR /src/cads-adaptors

COPY environment.yml /src/cads-adaptors/

RUN conda install -c conda-forge gcc python=3.12 \
    && conda env update -n base -f environment.yml

COPY . /src/cads-adaptors

RUN pip install --no-deps -e .
