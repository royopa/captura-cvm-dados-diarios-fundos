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
    libpq \
    unixodbc-dev \
    curl

#Download the desired package(s)
RUN curl -O https://download.microsoft.com/download/e/4/e/e4e67866-dffd-428c-aac7-8d28ddafb39b/msodbcsql17_17.5.2.2-1_amd64.apk

#(Optional) Verify signature, if 'gpg' is missing install it using 'apk add gnupg':
RUN curl -O https://download.microsoft.com/download/e/4/e/e4e67866-dffd-428c-aac7-8d28ddafb39b/msodbcsql17_17.5.2.2-1_amd64.sig

#Install the package(s)
RUN apk add --allow-untrusted msodbcsql17_17.5.2.2-1_amd64.apk

# install dependencies
RUN pip install --upgrade pip

COPY ./requirements.txt /usr/src/app/requirements.txt

RUN export LDFLAGS="-L/usr/local/opt/openssl/lib"

RUN pip install -r requirements.txt

ENTRYPOINT /bin/sh
