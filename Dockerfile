MAINTAINER Daniel Kristiyanto <danielkr@uw.edu>
FROM python:2

WORKDIR /app
RUN pip install elasticsearch
RUN pip install kafka-python

ADD app/insight.py /app/insight.py

CMD python /app/insight.py