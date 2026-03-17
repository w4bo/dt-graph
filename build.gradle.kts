import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    kotlin("jvm") version "2.3.20"
}

repositories {
    mavenCentral()
}

val porcodio = 21

kotlin {
    compilerOptions {
        jvmTarget = JvmTarget.JVM_21
    }
    jvmToolchain(porcodio)
}

//java {
//    toolchain {
//        languageVersion = JavaLanguageVersion.of(javaVersion)
//    }
//}

tasks.withType<Test>().configureEach {
     javaLauncher = javaToolchains.launcherFor {
         languageVersion = JavaLanguageVersion.of(porcodio)
     }
    testLogging {
//        events "passed", "skipped", "failed"
    }
    useJUnitPlatform()
}
//tasks.named("testClasses") {
//    dependsOn(tasks.named("compileTestKotlin"))
//}

//test {
//
//}

//jar {
//    manifest {
//        attributes(
//                'Main-Class': 'AsterixDataSource'
//        )
//    }
//}
//
//idea {
//    module {
//        downloadJavadoc = true
//        downloadSources = true
//    }
//}

dependencies {
    implementation("org.apache.commons:commons-lang3:3.20.0")
    implementation("org.rocksdb:rocksdbjni:10.5.1")
    implementation("org.json:json:20250107")
    implementation("org.locationtech.jts:jts-core:1.20.0")
    implementation("org.locationtech.jts.io:jts-io-common:1.20.0")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.21.1")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.21.1")
    implementation("com.fasterxml.jackson.core:jackson-core:2.21.1")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")
    implementation("org.slf4j:slf4j-simple:2.0.17")
    implementation("org.yaml:snakeyaml:2.6")
    implementation("org.postgresql:postgresql:42.7.3")
    implementation("org.neo4j.driver:neo4j-java-driver-slim:4.4.21")

    testImplementation("org.jetbrains.kotlin:kotlin-test")
    testImplementation(platform("org.junit:junit-bom:6.0.3"))
    testImplementation("org.junit.jupiter:junit-jupiter-api")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}

tasks.jar {
    exclude("datasets/**")
}

defaultTasks("clean", "build", "check") // , "shadowJar"
