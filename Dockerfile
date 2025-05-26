FROM python:3.10-slim-bullseye

RUN apt update ; apt upgrade -y
RUN apt install -y curl iputils-ping telnet
RUN apt autoremove -y ; apt clean ; apt autoclean

WORKDIR /директория

COPY requirements.txt /директория/requirements.txt

RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir --upgrade -r /директория/requirements.txt

COPY . /директория


ARG ENV_VALUE
ENV ENV_VALUE ${ENV_VALUE}

#HEALTHCHECK --start-period=30s --interval=20s --timeout=10s --retries=3 CMD curl -f http://localhost:8010/ping || exit 1
EXPOSE 1111
CMD ["python", "main.py"]
