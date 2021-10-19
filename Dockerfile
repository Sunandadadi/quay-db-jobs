FROM quay.io/bitnami/python

RUN pip install bintrees pymysql prometheus-client

ADD main.py /

ENTRYPOINT [ "python", "/main.py" ]
