FROM python:3.11.4-slim


#ENV PIP_INDEX_URL https://vmnexus.profitero.com/repository/pypi-group/simple
#ENV SNS_SOURCE=/sns

RUN apt-get update

COPY /src /
COPY requirements.txt /

RUN pip install --no-cache-dir -r /requirements.txt
ENV PYTHONPATH $PYTHONPATH:/src


ENTRYPOINT ["python", "/coinbase/app.py"]
#
#
#
#WORKDIR $SNS_SOURCE
#
#
#RUN groupadd -o -g ${GID} dev
#RUN useradd -l -s /bin/bash -u $UID -g $GID p
#
#
#COPY / $SNS_SOURCE
#
#ARG PF_SNS_VERSION
#
#RUN pip install pf-sns==$PF_SNS_VERSION
#
#RUN chown -R p:dev $SNS_SOURCE
#RUN chown -R p: /home
#
#USER p
#
#ENV PYTHONPATH $PYTHONPATH:$SNS_SOURCE/src
