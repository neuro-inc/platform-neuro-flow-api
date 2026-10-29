ARG PY_VERSION=3.14.10

FROM python:${PY_VERSION}-slim-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && python -c "from importlib.metadata import version; assert tuple(map(int, version('pip').split('.'))) >= (26, 2, 2)"

ENV PATH=/root/.local/bin:$PATH

# Copy to tmp folder to don't pollute home dir
WORKDIR /tmp
COPY requirements.txt /tmp/

RUN pip install --user --no-cache-dir -r requirements.txt
COPY dist /tmp/dist/
RUN pip install --user --no-cache-dir --find-links /tmp/dist platform-neuro-flow-api && \
    rm -rf /tmp/dist

FROM python:${PY_VERSION}-slim-bookworm AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && python -c "from importlib.metadata import version; assert tuple(map(int, version('pip').split('.'))) >= (26, 2, 2)"

LABEL org.opencontainers.image.source = "https://github.com/neuro-inc/platform-neuro-flow-api"

ARG SERVICE_NAME="platform-neuro-flow-api"
ARG SERVICE_UID=1001
ARG SERVICE_GID=1001

RUN addgroup --gid $SERVICE_GID $SERVICE_NAME && \
    adduser --uid $SERVICE_UID --gid $SERVICE_GID \
    --home /home/$SERVICE_NAME --shell /bin/false \
    --disabled-password --gecos "" $SERVICE_NAME && \
    mkdir -p /var/log/$SERVICE_NAME && \
    chown $SERVICE_NAME:$SERVICE_NAME /var/log/$SERVICE_NAME

COPY --from=builder --chown=$SERVICE_NAME:$SERVICE_GID /root/.local /home/$SERVICE_NAME/.local

WORKDIR /app
COPY alembic.ini alembic.ini
COPY alembic alembic

USER $SERVICE_NAME
ENV PATH=/home/$SERVICE_NAME/.local/bin:$PATH
ENV NP_NEURO_FLOW_API_PORT=8080
EXPOSE $NP_NEURO_FLOW_API_PORT

RUN python -c "from importlib.metadata import version; assert tuple(map(int, version('certifi').split('.'))) >= (2026, 10, 19)"

ENTRYPOINT [ "platform-neuro-flow-api" ]
