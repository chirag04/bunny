FROM maven:3.5-jdk-8 as builder

COPY . /opt/bunny
WORKDIR /opt/bunny
RUN mvn install -pl rabix-cli -P all,tes -am
RUN tar xzf rabix-cli/target/rabix-cli-*-release.tar.gz

FROM openjdk:8-jre-slim

COPY --from=builder /opt/bunny/rabix-cli-* /opt/rabix-cli
RUN ln -s /opt/rabix-cli/rabix /usr/bin/rabix
