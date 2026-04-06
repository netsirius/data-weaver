FROM eclipse-temurin:17-jre-jammy AS base

# Install Spark
ENV SPARK_VERSION=4.0.2
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

RUN apt-get update && apt-get install -y curl procps && \
    curl -fsSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" | \
    tar -xz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 $SPARK_HOME && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Copy the assembled JAR
COPY cli/target/scala-2.13/data-weaver.jar /app/data-weaver.jar

# Copy default configs
COPY scripts/docker-entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Default environment
ENV WEAVER_HOME=/app
ENV JAVA_OPTS="-Xmx1g"

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["--help"]
