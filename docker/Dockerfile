FROM openjdk

COPY ["ita-dataloader-0.0.1-SNAPSHOT.jar", "/temp/"]

EXPOSE 80

CMD java -jar /temp/ita-dataloader-0.0.1-SNAPSHOT.jar --spring.profiles.active=production --server.port=80
