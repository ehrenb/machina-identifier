FROM behren/machina-base-alpine:latest

RUN apk --update add --no-cache libmagic

COPY requirements.txt /tmp/
RUN pip3 install --trusted-host pypi.org \
                --trusted-host pypi.python.org \
                --trusted-host files.pythonhosted.org \
                -r /tmp/requirements.txt
RUN rm /tmp/requirements.txt

COPY Identifier.json /schemas/

COPY src /machina/src
