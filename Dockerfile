FROM redis:7.0.7-alpine as redis

FROM python:3.11.1-alpine3.17 as python-installation
RUN apk update && apk add bash
RUN apk add libc6-compat
RUN apk --no-cache add gcc musl-dev libffi-dev
RUN apk add py-pip python3-dev cmake gcc g++ openssl-dev build-base

WORKDIR /python
COPY ./requirements.txt .
#COPY ./requirements/ ./requirements/
ENV VIRTUAL_ENV=./.venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
RUN python3 -m pip install -r requirements.txt


FROM python:3.11.1-alpine3.17 as python
RUN apk update && apk add bash
#RUN apk add openjdk17-jre-headless
ARG MODE
COPY --from=redis usr/local/bin usr/local/bin
COPY --from=python-installation /python/.venv /python/.venv

ENV MODE=${MODE}
ENV USER=root
#ENV UID=12345
#ENV GID=23456
#RUN adduser --disabled-password --uid "$UID" "$USER"
ENV USER_PATH="/$USER"
ENV PATH="/python/.venv/bin:$PATH"
ENV PWD="/software/test"

WORKDIR $PWD
COPY . .
#RUN rm -rf requirements.txt
#RUN rm -rf ./requirements
#RUN rm -f ./Dockerfile
#RUN rm -f ./Dockerfile.dockerignore
CMD  ["/bin/bash", "entry-point.sh"]

