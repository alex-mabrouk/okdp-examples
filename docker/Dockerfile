#
# Copyright 2025 The OKDP Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

# dl.min.io prunes old archived releases (the pinned curl download 404s/410s
# over time); the same binaries stay available, tagged, as a container image.
FROM quay.io/minio/mc:RELEASE.2025-08-13T08-35-41Z AS mc

FROM python:3.12-slim-bookworm

ENV TRINO_VERSION=480 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/usr/local/bin:${PATH}"

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      bash \
      less \
      git  \
      curl \
      jq   \
      ca-certificates \
      openjdk-17-jre-headless && \
    update-ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=mc /usr/bin/mc /usr/local/bin/mc

RUN curl -fsSL -o /usr/local/bin/trino \
      "https://github.com/trinodb/trino/releases/download/${TRINO_VERSION}/trino-cli-${TRINO_VERSION}" && \
    chmod +x /usr/local/bin/trino

RUN pip install --no-cache-dir \
      requests \
      pyyaml \
      confluent-kafka

COPY docker/polaris-admin.py /usr/local/bin/polaris-admin
RUN chmod +x /usr/local/bin/polaris-admin

COPY docker/douane_producer.py /usr/local/bin/douane-producer
RUN chmod +x /usr/local/bin/douane-producer

RUN python --version && \
    git --version && \
    mc --version && \
    trino --version && \
    java -version

WORKDIR /data
RUN mkdir -p /data

ENTRYPOINT ["bash"]

