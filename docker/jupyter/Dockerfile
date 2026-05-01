FROM spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root

RUN python3 -m pip install --no-cache-dir \
    jupyterlab \
    notebook \
    ipykernel \
    pyspark==3.5.3 \
    pyarrow \
    pandas \
    matplotlib \
    seaborn

RUN mkdir -p /work

WORKDIR /work
