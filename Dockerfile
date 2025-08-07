FROM gradle:8.3-jdk17

LABEL authors="manuele.pasini2"

COPY . /dt_graph
WORKDIR /dt_graph

RUN gradle build  -x test --no-daemon

CMD ["bash"]