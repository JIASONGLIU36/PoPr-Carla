# Multi-stage build: First the full builder image:

# liboqs build type variant; maximum portability of image; no openssl dependency:
ARG LIBOQS_BUILD_DEFINES="-DOQS_DIST_BUILD=ON -DBUILD_SHARED_LIBS=ON -DOQS_USE_OPENSSL=OFF"

FROM alpine:3.16 as intermediate
# Take in all global args
ARG LIBOQS_BUILD_DEFINES

LABEL version="2"

ENV DEBIAN_FRONTEND noninteractive

RUN apk update && apk upgrade

# Get all software packages required for builing all components:
RUN apk add build-base linux-headers cmake ninja git

# get all sources
WORKDIR /opt
RUN git clone --depth 1 --branch main https://github.com/open-quantum-safe/liboqs && \
    git clone --depth 1 --branch main https://github.com/open-quantum-safe/liboqs-python.git 

# build liboqs 
WORKDIR /opt/liboqs
RUN mkdir build && cd build && cmake -G"Ninja" .. ${LIBOQS_BUILD_DEFINES} && ninja install

## second stage: Only create minimal image without build tooling and intermediate build results generated above:
FROM alpine:3.16

RUN apk update && apk upgrade

# Get all software packages required for running all components:
RUN apk add --no-cache python3 py3-pip python3-dev gcc musl-dev libffi-dev \
    && python3 -m ensurepip \
    && pip3 install --no-cache --upgrade pip setuptools wheel

# Only retain the binary contents in the final image
COPY --from=intermediate /usr/local /usr/local
COPY --from=intermediate /opt/liboqs-python /opt/liboqs-python

ENV PYTHONPATH=/opt/liboqs-python

WORKDIR /app

ARG UID=10001
RUN adduser \
    --disabled-password \
    --gecos "" \
    --home "/nonexistent" \
    --shell "/sbin/nologin" \
    --no-create-home \
    --uid "${UID}" \
    appuser

# create /app and set permission
RUN mkdir -p /app/data && chown appuser:appuser /app/data && chmod 755 /app/data


# Download dependencies as a separate step to take advantage of Docker's caching.
# Leverage a cache mount to /root/.cache/pip to speed up subsequent builds.
# Leverage a bind mount to requirements.txt to avoid having to copy them into
# into this layer.
RUN --mount=type=cache,target=/root/.cache/pip \
    --mount=type=bind,source=requirements.txt,target=requirements.txt \
    python3 -m pip install -r requirements.txt

# Switch to the non-privileged user to run the application.
USER appuser

# Copy the source code into the container.
#COPY . .
COPY --chown=appuser:appuser . /app

# Expose the port that the application listens on.
EXPOSE 8100

EXPOSE 9900

# Run the application.
ENTRYPOINT ["python3", "carla_nodes.py"]
CMD ["13000"]

