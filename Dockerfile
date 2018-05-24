FROM debian:9.4-slim

ENV BP_RELEASE=0.6.12

RUN apt-get update && apt-get install --yes --no-install-recommends wget ca-certificates && mkdir /workdir && \
    cd /tmp && \
    wget -O bp_linux.tar.gz https://github.com/Azure/blobporter/releases/download/v${BP_RELEASE}/bp_linux.tar.gz && \
    tar xpf bp_linux.tar.gz && \
    mv linux_amd64/* /usr/local/bin/ && \
    apt-get clean && rm -rf /tmp/*.tar.gz /var/cache/apt/*


VOLUME /workdir
WORKDIR /workdir
