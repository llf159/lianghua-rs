import groovy.json.JsonSlurper
import java.io.File
import java.util.Properties
import org.gradle.api.artifacts.dsl.RepositoryHandler
import org.gradle.api.artifacts.repositories.MavenArtifactRepository

plugins {
    id("com.android.application")
    id("org.jetbrains.kotlin.android")
    id("rust")
}

val tauriProperties = Properties().apply {
    val propFile = file("tauri.properties")
    if (propFile.exists()) {
        propFile.inputStream().use { load(it) }
    }
}

data class RustlsPlatformVerifierConfig(
    val repoPath: String,
    val version: String,
)

fun resolveRustlsPlatformVerifier(): RustlsPlatformVerifierConfig {
    val manifestPath = File(project.rootDir, "../../Cargo.toml").canonicalPath
    val dependencyJson = providers.exec {
        workingDir = project.rootDir
        commandLine(
            "cargo",
            "metadata",
            "--format-version",
            "1",
            "--filter-platform",
            "aarch64-linux-android",
            "--manifest-path",
            manifestPath,
        )
    }.standardOutput.asText.get()

    @Suppress("UNCHECKED_CAST")
    val packages = (JsonSlurper().parseText(dependencyJson) as Map<String, Any?>)["packages"] as List<Map<String, Any?>>
    val verifierManifestPath = packages
        .first { it["name"] == "rustls-platform-verifier-android" }["manifest_path"] as String
    val repoRoot = File(File(verifierManifestPath).parentFile, "maven")
    val metadataFile = File(repoRoot, "rustls/rustls-platform-verifier/maven-metadata-local.xml")
    val metadataText = metadataFile.readText()
    val version = Regex("<release>([^<]+)</release>")
        .find(metadataText)
        ?.groupValues
        ?.get(1)
        ?: error("无法从 ${metadataFile.path} 解析 rustls-platform-verifier 版本")

    return RustlsPlatformVerifierConfig(
        repoPath = repoRoot.path,
        version = version,
    )
}

fun RepositoryHandler.rustlsPlatformVerifier(repoPath: String): MavenArtifactRepository {
    return maven {
        url = uri(repoPath)
        metadataSources {
            mavenPom()
            artifact()
        }
    }
}

val rustlsPlatformVerifier = resolveRustlsPlatformVerifier()

repositories {
    rustlsPlatformVerifier(rustlsPlatformVerifier.repoPath)
}

android {
    compileSdk = 36
    namespace = "com.mingyuan.lianghua"
    defaultConfig {
        manifestPlaceholders["usesCleartextTraffic"] = "false"
        applicationId = "com.mingyuan.lianghua"
        minSdk = 24
        targetSdk = 36
        versionCode = tauriProperties.getProperty("tauri.android.versionCode", "1").toInt()
        versionName = tauriProperties.getProperty("tauri.android.versionName", "1.0")
    }
    buildTypes {
        getByName("debug") {
            manifestPlaceholders["usesCleartextTraffic"] = "true"
            isDebuggable = true
            isJniDebuggable = true
            isMinifyEnabled = false
            packaging {                jniLibs.keepDebugSymbols.add("*/arm64-v8a/*.so")
                jniLibs.keepDebugSymbols.add("*/armeabi-v7a/*.so")
                jniLibs.keepDebugSymbols.add("*/x86/*.so")
                jniLibs.keepDebugSymbols.add("*/x86_64/*.so")
            }
        }
        getByName("release") {
            isMinifyEnabled = true
            proguardFiles(
                *fileTree(".") { include("**/*.pro") }
                    .plus(getDefaultProguardFile("proguard-android-optimize.txt"))
                    .toList().toTypedArray()
            )
        }
    }
    kotlinOptions {
        jvmTarget = "1.8"
    }
    buildFeatures {
        buildConfig = true
    }
}

rust {
    rootDirRel = "../../../"
}

dependencies {
    implementation("androidx.webkit:webkit:1.14.0")
    implementation("androidx.appcompat:appcompat:1.7.1")
    implementation("androidx.activity:activity-ktx:1.10.1")
    implementation("com.google.android.material:material:1.12.0")
    implementation("rustls:rustls-platform-verifier:${rustlsPlatformVerifier.version}")
    testImplementation("junit:junit:4.13.2")
    androidTestImplementation("androidx.test.ext:junit:1.1.4")
    androidTestImplementation("androidx.test.espresso:espresso-core:3.5.0")
}

apply(from = "tauri.build.gradle.kts")
