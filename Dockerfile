# pull official base image
FROM python:3.6.10-alpine
# set work directory
WORKDIR /usr/src/app
# set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
RUN apk update \
  && apk add \
    build-base \
    postgresql \
    postgresql-dev \
    libpq
# install dependencies
RUN pip install --upgrade pip
COPY ./requirements.txt /usr/src/app/requirements.txt
RUN export LDFLAGS="-L/usr/local/opt/openssl/lib"
RUN pip install -r requirements.txt
ENTRYPOINT /bin/sh
