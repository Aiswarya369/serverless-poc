FROM python:3.8

ARG ARTIFACTORY_USERNAME
ARG ARTIFACTORY_PASSWORD
ARG ARTIFACTORY_HOST=ci.dev.rocno.com.au
ARG ARTIFACTORY_URL=https://${ARTIFACTORY_HOST}/nexus/repository/pypi-releases/simple
ARG MODULE_DIRECTORY

ENV \
    TERM="xterm" \
    LANG="en_US.UTF-8" \
    LANGUAGE="en_US.UTF-8" \
    LC_ALL="en_US.UTF-8" \
    DEBIAN_FRONTEND="noninteractive"

ARG POETRY_VERSION="1.1.14"
ENV POETRY_VIRTUALENVS_IN_PROJECT true

ARG PSYCOPG2_VERSION="2.9.3"
ARG BOTO3_VERSION="1.26.61"

RUN echo "Installing apt changes..."
WORKDIR /app/
RUN apt-get update && \
    apt-get install -y curl libpq-dev unixodbc-dev && \
    apt-get clean && \
    apt-get autoclean && \
    apt-get autoremove --yes && \
    rm -rf /var/lib/{apt,dpkg,cache,log}/

RUN echo "Installed apt changes."

RUN echo "Installing pyodbc requirements..."
# Add SQLServer drivers.
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update
RUN ACCEPT_EULA=Y apt-get install -y msodbcsql17
RUN echo "Installed pyodbc requirements."

RUN echo "Installing Poetry..."
RUN curl -sSL https://install.python-poetry.org | POETRY_HOME=/usr/local/poetry python3 -
ENV PATH /usr/local/poetry/bin:${PATH}
RUN echo "Installed Poetry."
RUN echo $(poetry --version)

# Add to python path /app, /app/${MODULE_DIRECTORY} and /app/${MODULE_DIRECTORY}/lamdas for backward compatibilty.
RUN mkdir -p /app/${MODULE_DIRECTORY}
ENV PYTHONPATH /app:/app/${MODULE_DIRECTORY}:/app/${MODULE_DIRECTORY}/lambdas:${PYTHONPATH}
COPY poetry.lock pyproject.toml /app/

# Configure the CrescoNet Nexus repository as a secondary package source.
RUN poetry config repositories.cresconet $ARTIFACTORY_URL
RUN poetry config http-basic.cresconet $ARTIFACTORY_USERNAME $ARTIFACTORY_PASSWORD
RUN poetry install --no-dev --no-root

RUN echo "Installing pip requirements..."
ENV PATH /app/.venv/bin:${PATH}
RUN python3 -m pip install boto3==${BOTO3_VERSION}
RUN python3 -m pip install psycopg2==${PSYCOPG2_VERSION}
RUN echo "Installed pip requirements."

ADD ${MODULE_DIRECTORY} /app/${MODULE_DIRECTORY}
