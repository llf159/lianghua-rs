import org.gradle.buildconfiguration.tasks.UpdateDaemonJvm

buildscript {
    repositories {
        google()
        mavenCentral()
    }
    dependencies {
        classpath("com.android.tools.build:gradle:9.0.1")
        classpath("org.jetbrains.kotlin:kotlin-gradle-plugin:2.2.10")
    }
}

allprojects {
    repositories {
        google()
        mavenCentral()
    }
}

tasks.register("clean").configure {
    delete("build")
}

tasks.named<UpdateDaemonJvm>("updateDaemonJvm") {
    // Only record the criteria. Developers and CI provide a local JDK 21.
    toolchainPlatforms.empty()
}
