<<<<<<< HEAD
FROM gradle:9.6-jdk17
=======
FROM gradle:9.6-jdk17
>>>>>>> feat-tssingletable

LABEL authors="manuele.pasini2"

COPY . /dt_graph
WORKDIR /dt_graph
RUN apt-get update && apt install nano
RUN gradle build  -x test --no-daemon

CMD ["bash"]