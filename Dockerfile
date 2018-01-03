FROM maven:3.5-jdk-8 as builder

COPY . /opt/bunny
WORKDIR /opt/bunny
RUN mvn install -pl rabix-cli -P all,tes -am
RUN tar xzf rabix-cli/target/rabix-cli-*-release.tar.gz

FROM openjdk:8-jre-slim

COPY --from=builder /opt/bunny/rabix-cli-* /opt/rabix-cli
RUN ln -s /opt/rabix-cli/rabix /usr/bin/rabix

# install python3.6
RUN echo 'deb http://ftp.de.debian.org/debian testing main' >> /etc/apt/sources.list
RUN echo 'APT::Default-Release "stable";' | tee -a /etc/apt/apt.conf.d/00local
RUN apt-get update
RUN apt-get -t testing install python3.6 python3-pip -y
RUN pip3 install --no-cache-dir requests boto3
