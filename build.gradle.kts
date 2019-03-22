import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.3.21"
    application
    war
}

repositories {
    maven("http://www.datanucleus.org/downloads/maven2/")
    mavenCentral()
}

dependencies {
    compile(kotlin("stdlib-jdk8"))
    runtime("oracle:ojdbc6:11.2.0.3")
    testCompile("junit:junit:4.12")
    testCompile("org.assertj:assertj-core:3.6.1")
}

sourceSets {
    getByName("main").apply {
        java.srcDirs("src")
        resources.srcDirs("conf", "src")
    }
    getByName("test").apply {
        java.srcDirs("test")
        resources.srcDirs("test")
    }
}

tasks.withType<KotlinCompile> {
    kotlinOptions {
        jvmTarget = "1.8"
        freeCompilerArgs = listOf("-Xjsr305=strict -progressive")
    }
}

application {
    mainClassName = "LauncherKt"
}
