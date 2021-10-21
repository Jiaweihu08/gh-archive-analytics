FROM azul/zulu-openjdk-debian:8u292-8.54.0.21
USER root
# Set the working dir to /home/jovyan to be able to share files with qbeast-platform (Jupyter Notebooks)
WORKDIR /home/jovyan

ENV APACHE_SPARK_VERSION="3.1.1" \
  HADOOP_VERSION="3.2" \
  spark_checksum="E90B31E58F6D95A42900BA4D288261D71F6C19FA39C1CB71862B792D1B5564941A320227F6AB0E09D946F16B8C1969ED2DEA2A369EC8F9D2D7099189234DE1BE" \
  SPARK_HOME=/opt/spark \
  SPARK_OPTS="--driver-java-options=-Xms1024M --driver-java-options=-Xmx4096M --driver-java-options=-Dlog4j.logLevel=info" \
  PATH=${PATH}:/opt/spark/bin \
  PYTHONPATH=/opt/spark/python/:/opt/spark/python/lib/py4j-0.10.9-src.zip:/opt/spark/python/lib/py4j-0.10.9-src.zip:${PYTHONPATH}

# Install Python 3.9 from source & install Spark after that
RUN apt-get update && \
  apt install wget build-essential libreadline-gplv2-dev libncursesw5-dev libssl-dev libsqlite3-dev tk-dev libgdbm-dev libc6-dev libbz2-dev libffi-dev zlib1g-dev --yes && \
   wget https://www.python.org/ftp/python/3.9.5/Python-3.9.5.tgz && \
   tar xzf Python-3.9.5.tgz && \
   cd Python-3.9.5 && \
   ./configure --enable-optimizations && \
   make altinstall && \
   ln -s /usr/local/bin/python3.9 /usr/local/bin/python3 && \
   ln -s /usr/local/bin/python3.9 /usr/local/bin/python && \
  # Intall Spark
  cd /tmp && \
  wget -q "https://archive.apache.org/dist/spark/spark-${APACHE_SPARK_VERSION}/spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
  echo "${spark_checksum} *spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" | sha512sum -c - && \
  tar xzf "spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" -C /opt/ --owner root --group root --no-same-owner && \
  rm "spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
  cd /opt && \
  ln -s "spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}" spark

ADD https://repo1.maven.org/maven2/com/microsoft/azure/azure-storage/2.0.0/azure-storage-2.0.0.jar \
  https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/3.2.0/hadoop-azure-3.2.0.jar \
  https://repo1.maven.org/maven2/com/azure/azure-storage-blob/12.8.0/azure-storage-blob-12.8.0.jar \
  https://repo1.maven.org/maven2/com/azure/azure-storage-common/12.8.0/azure-storage-common-12.8.0.jar \
  https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.0/hadoop-aws-3.2.0.jar \
  https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.7.4/aws-java-sdk-1.7.4.jar \
  https://repo1.maven.org/maven2/io/delta/delta-core_2.12/0.8.0/delta-core_2.12-0.8.0.jar \
  https://repo1.maven.org/maven2/com/google/guava/guava/18.0/guava-18.0.jar \
  /opt/spark/jars/

RUN apt-get update && apt-get install -y cmake libjpeg-dev zlib1g-dev


EXPOSE 8501
WORKDIR /app
RUN adduser --disabled-password regular && chown -R regular /app
USER regular
RUN python -m ensurepip --upgrade
COPY requirements.txt ./requirements.txt
RUN pip3.9 install -r requirements.txt
COPY . .
CMD streamlit run app.py
