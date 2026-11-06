FROM eclipse-temurin:25-jdk@sha256:83f4a7fc27df94dcc7b8de04d14cbbba8da14406f921e966969c3b09000aa507 AS build
WORKDIR /source
COPY gradlew build.gradle.kts settings.gradle.kts LICENSE ./
COPY gradle/ gradle/
COPY src/ src/
RUN ./gradlew --no-daemon installDist

FROM eclipse-temurin:25-jre@sha256:ecdc0adb37bde1a281c4d245a3cd535004b1fc8322d560f5b09463b248c81df0
RUN groupadd --gid 10001 tikronos && useradd --uid 10001 --gid tikronos --no-create-home tikronos
WORKDIR /app
COPY --from=build /source/build/install/tikronos/ /app/
USER 10001:10001
ENV JAVA_OPTS="-XX:MaxRAMPercentage=70.0 -XX:+ExitOnOutOfMemoryError"
EXPOSE 8000
ENTRYPOINT ["/app/bin/tikronos"]
CMD ["--listen", "0.0.0.0:8000"]
