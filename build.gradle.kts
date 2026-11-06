plugins {
    kotlin("jvm") version "2.2.21"
    id("io.gitlab.arturbosch.detekt") version "1.23.8"
    application
}

repositories {
    mavenCentral()
}

kotlin {
    jvmToolchain(17)
}

application {
    mainClass.set("tikronos.MainKt")
}

dependencies {
    runtimeOnly("org.slf4j:slf4j-jdk14:1.7.36")
    implementation("org.tikv:tikv-client-java:3.3.5")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.21.7")
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
}
