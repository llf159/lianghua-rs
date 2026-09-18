import org.gradle.buildconfiguration.tasks.UpdateDaemonJvm
import com.android.build.api.dsl.LibraryExtension

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

subprojects {
    plugins.withId("com.android.library") {
        afterEvaluate {
            extensions.configure<LibraryExtension> {
                defaultConfig.consumerProguardFiles.removeAll { !it.exists() }
            }
        }
    }
}

tasks.register("clean").configure {
    delete("build")
}

tasks.named<UpdateDaemonJvm>("updateDaemonJvm") {
    toolchainPlatforms.empty()
}
