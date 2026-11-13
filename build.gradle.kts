plugins {
    kotlin("jvm") version "2.4.21"
    id("io.gitlab.arturbosch.detekt") version "1.23.8"
    application
}

repositories {
    mavenCentral()
}

kotlin {
    jvmToolchain(21)
}

application {
    mainClass.set("tikronos.MainKt")
}

distributions {
    main {
        contents { from("LICENSE") }
    }
}

dependencies {
    implementation("io.ktor:ktor-server-netty:3.6.1")
    runtimeOnly("org.slf4j:slf4j-jdk14:2.0.20")
    implementation("org.tikv:tikv-client-java:3.3.5")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.22.4")
    testImplementation(kotlin("test-junit5"))
    testRuntimeOnly("org.junit.platform:junit-platform-launcher:1.12.2")
}

tasks.test {
    useJUnitPlatform { excludeTags("tikv") }
}


tasks.register<Test>("integrationTest") {
    description = "Run storage protocol checks against TIKRONOS_TEST_PD."
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    useJUnitPlatform { includeTags("tikv") }
    testLogging { exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL }
}
