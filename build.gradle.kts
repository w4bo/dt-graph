import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    kotlin("jvm") version "2.4.0"
}

repositories {
    mavenCentral()
}

val jVersion = 17

kotlin {
    compilerOptions {
        jvmTarget = JvmTarget.JVM_17
    }
    jvmToolchain(jVersion)
}

tasks.withType<Test>().configureEach {
     javaLauncher = javaToolchains.launcherFor {
         languageVersion = JavaLanguageVersion.of(jVersion)
     }
    jvmArgs(
        "-Dcom.sun.management.jmxremote",
        "-Dcom.sun.management.jmxremote.port=9010",
        "-Dcom.sun.management.jmxremote.rmi.port=9010",
        "-Dcom.sun.management.jmxremote.authenticate=false",
        "-Dcom.sun.management.jmxremote.ssl=false",
        // "-Djava.rmi.server.hostname=172.27.236.32"
        // "-XX:-Inline"
    )
    testLogging {
        events("passed", "skipped", "failed")
    }
    useJUnitPlatform()
    maxHeapSize = "64g"
}

dependencies {
    implementation("org.apache.commons:commons-lang3:3.20.0")
    implementation("org.rocksdb:rocksdbjni:10.10.1.1")
    implementation("org.json:json:20250107")
    implementation("org.locationtech.jts:jts-core:1.20.0")
    implementation("org.locationtech.jts.io:jts-io-common:1.20.0")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.21.4")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.21.4")
    implementation("com.fasterxml.jackson.core:jackson-core:2.21.4")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.11.0")
    implementation("org.slf4j:slf4j-simple:2.0.18")
    implementation("org.yaml:snakeyaml:2.6")
    implementation("org.postgresql:postgresql:42.7.11")
    implementation("org.neo4j.driver:neo4j-java-driver-slim:4.4.26")

    testImplementation("org.jetbrains.kotlin:kotlin-test")
    testImplementation(platform("org.junit:junit-bom:6.1.0"))
    testImplementation("org.junit.jupiter:junit-jupiter-api")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}

tasks.jar {
    exclude("datasets/**")
}

defaultTasks("clean", "build", "check")
