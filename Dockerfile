FROM apache/airflow:2.7.1-python3.11 AS base

ENV AIRFLOW_HOME=/opt/airflow

USER root
RUN apt-get update && \
    apt-get install -y gcc python3-dev openjdk-11-jdk && \
    apt-get clean

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-arm64

# GCS config
# Ref: https://airflow.apache.org/docs/docker-stack/recipes.html

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

USER 0

# Note the sdk version, older sdk versions are not compatible with python versions > 3.8
ARG CLOUD_SDK_VERSION=374.0.0
ENV GCLOUD_HOME=/opt/google-cloud-sdk

ENV PATH="${GCLOUD_HOME}/bin/:${PATH}"

RUN DOWNLOAD_URL="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz" \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/google-cloud-sdk.tar.gz" \
    && mkdir -p "${GCLOUD_HOME}" \
    && tar xzf "${TMP_DIR}/google-cloud-sdk.tar.gz" -C "${GCLOUD_HOME}" --strip-components=1 \
    && "${GCLOUD_HOME}/install.sh" \
       --bash-completion=false \
       --path-update=false \
       --usage-reporting=false \
       --additional-components alpha beta kubectl \
       --quiet \
    && rm -rf "${TMP_DIR}" \
    && rm -rf "${GCLOUD_HOME}/.install/.backup/" \
    && gcloud --version

WORKDIR $AIRFLOW_HOME

USER $AIRFLOW_UID

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Stage 2 - GCS connector for Spark
FROM bitnami/spark:latest AS spark

USER root
RUN apt-get update && \
    apt-get install -y curl && \
    apt-get clean

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

USER 0

ENV HADOOP_VERSION=3.2.2
ENV GCS_CONNECTOR_VERSION=2.2.20
ENV SPARK_HOME=/opt/bitnami/spark 

RUN curl https://github.com/GoogleCloudDataproc/hadoop-connectors/releases/download/v${GCS_CONNECTOR_VERSION}/gcs-connector-hadoop3-${GCS_CONNECTOR_VERSION}-shaded.jar \
     -o ${SPARK_HOME}/jars/gcs-connector-hadoop3-${GCS_CONNECTOR_VERSION}.jar

# Stage 3 - Final image
FROM base AS final

ENV SPARK_HOME=/opt/bitnami/spark 

COPY --from=spark ${SPARK_HOME} ${SPARK_HOME}

#TODO Copy over Spark code possibly

