FROM gradle:8.13-jdk21-corretto

LABEL authors="manuele.pasini2"

COPY . /dt_graph
WORKDIR /dt_graph

RUN gradle build  -x test --no-daemon

CMD ["bash"]