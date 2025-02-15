FROM ubuntu:oracular
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get -yq install python3-pip python3-venv
COPY src/* /simulator/
COPY model/* /simulator/
COPY main_simulator.py /simulator/
COPY requirements.txt /simulator/
WORKDIR /simulator
RUN python3 -m venv /simulator
RUN /simulator/bin/pip3 install -r requirements.txt
ENTRYPOINT ["/simulator/bin/python3", "/simulator/main_simulator.py"]
CMD ["--history=/data/history.csv"]