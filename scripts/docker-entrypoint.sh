#!/bin/bash
set -e

# Data Weaver Docker entrypoint
# Usage: docker run data-weaver:latest <command> [args]
# Example: docker run data-weaver:latest apply pipelines/my_pipeline.yaml --env prod

exec java $JAVA_OPTS -jar /app/data-weaver.jar "$@"
