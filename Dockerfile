FROM python:3.7

COPY . /meteringdatamigration

RUN pip install sqlalchemy
RUN pip install pandas
RUN pip install configparser
RUN pip install psycopg2
RUN pip install mysql-connector
RUN pip install mysqlclient

CMD [ "python", "./meteringdatamigration/main.py" ]
