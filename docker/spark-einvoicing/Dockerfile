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

# The einvoicing chain is the one workload the platform image cannot run: writing
# Factur-X means a PDF engine, and validating it means running the Schematrons of
# the standard. Both come from here; everything else uses the platform image.
ARG SPARK_IMAGE=quay.io/okdp/spark-py:spark-3.5.6-python-3.11-scala-2.12-java-17
FROM ${SPARK_IMAGE}

USER root

# factur-x ships the XSD and the Schematrons of release 1.09, for the five profiles
# and for EXTENDED-CTC-FR, so no artefact has to be fetched from a download form.
# It pulls lxml, pypdf and python-stdnum, whose SIREN/SIRET and VAT check digits
# the generator and the controls both use.
# boto3 is what writes the invoices out one file each: the platform image has no
# S3 client for Python, and the JVM gateway that Spark exposes on the driver does
# not exist on an executor.
RUN python3 -m pip install --no-cache-dir \
      factur-x==6.8 \
      reportlab==4.2.5 \
      boto3

# Those Schematrons are XSLT 2.0/3.0, which lxml cannot run: factur-x delegates
# them to a Saxon server over HTTP and, by default, treats an unreachable one as a
# pass. A validator that cannot fail is worse than none, so the jobs drive Saxon
# themselves, in the JVM this image already carries.
ENV SAXON_VERSION=12.5 \
    XMLRESOLVER_VERSION=5.2.2 \
    SAXON_HOME=/opt/saxon

RUN mkdir -p ${SAXON_HOME} && \
    curl -fsSL -o ${SAXON_HOME}/saxon-he.jar \
      "https://repo1.maven.org/maven2/net/sf/saxon/Saxon-HE/${SAXON_VERSION}/Saxon-HE-${SAXON_VERSION}.jar" && \
    curl -fsSL -o ${SAXON_HOME}/xmlresolver.jar \
      "https://repo1.maven.org/maven2/org/xmlresolver/xmlresolver/${XMLRESOLVER_VERSION}/xmlresolver-${XMLRESOLVER_VERSION}.jar"

RUN python3 -c "import facturx, reportlab, stdnum, lxml, pypdf, boto3; print('python deps ok')" && \
    java -cp "${SAXON_HOME}/saxon-he.jar:${SAXON_HOME}/xmlresolver.jar" \
      net.sf.saxon.Transform -? 2>&1 | head -1

ARG spark_uid=185
USER ${spark_uid}
