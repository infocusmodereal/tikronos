FROM eclipse-temurin:21-jdk@sha256:ebfbb18d11ff163ee83b21258acf12248af48d22a0fdfdc68fa1a1516c05976a AS build
WORKDIR /source
COPY gradlew build.gradle.kts settings.gradle.kts LICENSE ./
COPY gradle/ gradle/
COPY src/ src/
RUN ./gradlew --no-daemon installDist

FROM eclipse-temurin:21-jre@sha256:0b99b3376fbb50dbf9c0af8535fdd5b8eb0d3fdfc8b1e4967f383b6919f6901a
RUN groupadd --gid 10001 tikronos && useradd --uid 10001 --gid tikronos --no-create-home tikronos
WORKDIR /app
COPY --from=build /source/build/install/tikronos/ /app/
USER 10001:10001
ENV JAVA_OPTS="-XX:MaxRAMPercentage=70.0 -XX:+ExitOnOutOfMemoryError"
EXPOSE 8000
ENTRYPOINT ["/app/bin/tikronos"]
CMD ["--listen", "0.0.0.0:8000"]
