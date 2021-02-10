# coding=utf-8
# Copyright 2020 Google LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Docker file for running CC4D cloudbuid CI tests.

FROM python:3.7-slim-buster

COPY requirements.txt /requirements.txt

RUN set -ex \
    && apt-get update -yqq \
    && apt-get upgrade -yqq \
    && apt-get install -yqq --no-install-recommends \
        build-essential \
    && pip install -r /requirements.txt \
    && pip install --upgrade protobuf \
    && pip install mock \
        pytest \
        requests_mock \
        freezegun \
        pytest-cov

ADD . /root/cc4d

RUN mkdir -p /root/cc4d/src/gps_building_blocks/cloud/utils
RUN cp -r /root/cc4d/gps_building_blocks/py/gps_building_blocks/cloud/utils/* /root/cc4d/src/gps_building_blocks/cloud/utils

ENV PYTHONPATH=".:./src"

WORKDIR /root/cc4d/src
