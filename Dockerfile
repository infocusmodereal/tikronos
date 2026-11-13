FROM eclipse-temurin:24-jdk@sha256:7493205ffe6caa8074fa8a06a276bb1c5ac41d3dd0fd43a0db66d7f776e80b3e AS build
WORKDIR /source
COPY gradlew build.gradle.kts settings.gradle.kts LICENSE ./
COPY gradle/ gradle/
COPY src/ src/
RUN ./gradlew --no-daemon installDist

FROM eclipse-temurin:24-jre@sha256:8cb2387a28af84cf0db0948d9c67d4480192f4e567027a3963f145d218e8b4f2
RUN groupadd --gid 10001 tikronos && useradd --uid 10001 --gid tikronos --no-create-home tikronos
WORKDIR /app
COPY --from=build /source/build/install/tikronos/ /app/
USER 10001:10001
ENV JAVA_OPTS="-XX:MaxRAMPercentage=70.0 -XX:+ExitOnOutOfMemoryError"
EXPOSE 8000
ENTRYPOINT ["/app/bin/tikronos"]
CMD ["--listen", "0.0.0.0:8000"]
