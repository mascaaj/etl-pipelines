FROM python:3.9-slim-buster

# Dont cache python packages
ENV PIP_NO_CACHE_DIR=yes

ENV PYTHONDONTWRITEBYTECODE 1

# Upgrade pip and install pipenv
RUN python -m pip install --upgrade pip
RUN pip install pipenv

# Copy the code from current folder
COPY . code
WORKDIR /code

ENV PYTHONPATH "${PYTHONPATH}:/code/"

# Use pipfile to create the piplock file
RUN pipenv install --dev

# run coverage tests
RUN pipenv install --ignore-pipfile --system
RUN pipenv install --dev --system
# RUN pip list

# run tests & generate coverage report
RUN coverage run --omit="*/tests*" -m unittest discover -v
RUN coverage report -m