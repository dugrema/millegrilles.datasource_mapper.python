FROM registry.millegrilles.com/millegrilles/messages_python:2025.4.104 as stage1

# Pour offline build
#ENV PIP_FIND_LINKS=$BUILD_FOLDER/pip \
#    PIP_RETRIES=0 \
#    PIP_NO_INDEX=true
COPY requirements.txt $BUILD_FOLDER/requirements.txt

RUN mkdir -p /var/opt/millegrilles/datasource_mapper && \
    chown 984:980 /var/opt/millegrilles/datasource_mapper && \
    pip3 install --no-cache-dir -r $BUILD_FOLDER/requirements.txt

FROM stage1

ARG VBUILD=2025.4.0

ENV CERT_PEM=/run/secrets/cert.pem \
    KEY_PEM=/run/secrets/key.pem \
    CA_PEM=/run/secrets/pki.millegrille.cert \
    DIR_DATA=/var/opt/millegrilles/datasource_mapper

# Creer repertoire app, copier fichiers
COPY . $BUILD_FOLDER

RUN cd $BUILD_FOLDER/ && \
    python3 ./setup.py install

# UID fichiers = 984
# GID millegrilles = 980
USER 984:980

VOLUME /var/opt/millegrilles/datasource_mapper

CMD ["-m", "millegrilles_datasourcemapper", "--verbose"]
