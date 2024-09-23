# syntax=docker/dockerfile:1

FROM jupyter/pyspark-notebook:latest
RUN pip install --no-cache-dir pyspark kaggle

COPY --chown=jovyan --chmod=600 kaggle.json /home/jovyan/.config/kaggle/kaggle.json

COPY --chown=jovyan --chmod=700 analysis.py /home/jovyan/analysis.py

CMD ["python", "/home/jovyan/analysis.py"]