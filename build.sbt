val commonSettings = Seq(
  scalaVersion := "2.12.6",
  crossScalaVersions := Seq(scalaVersion.value, "2.11.12"),
  organization := "com.gilt",

  releaseCrossBuild := true,

  scalacOptions += "-target:jvm-1.7",

  javacOptions ++= Seq("-source", "1.7", "-target", "1.7"),

  releasePublishArtifactsAction := PgpKeys.publishSigned.value,

  publishMavenStyle := true,

  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value)
      Some("snapshots" at nexus + "content/repositories/snapshots")
    else
      Some("releases"  at nexus + "service/local/staging/deploy/maven2")
  },

  publishArtifact in Test := false,

  pomIncludeRepository := { _ => false },

  licenses := Seq("Apache-style" -> url("https://raw.githubusercontent.com/gilt/gfc-aws-kinesis/master/LICENSE")),

  homepage := Some(url("https://github.com/gilt/gfc-aws-kinesis")),

  pomExtra := (
    <scm>
      <url>https://github.com/gilt/gfc-aws-kinesis.git</url>
      <connection>scm:git:git@github.com:gilt/gfc-aws-kinesis.git</connection>
    </scm>
    <developers>
      <developer>
        <id>andreyk0</id>
        <name>Andrey Kartashov</name>
        <url>https://github.com/andreyk0</url>
      </developer>
      <developer>
        <id>krschultz</id>
        <name>Kevin Schultz</name>
        <url>https://github.com/krschultz</url>
      </developer>
      <developer>
        <id>mikegirkin</id>
        <name>Mike Girkin</name>
        <url>https://github.com/mikegirkin</url>
      </developer>
    </developers>
  )

)

lazy val client = (project in file("client"))
  .settings(commonSettings:_*)
  .settings(
  name := "gfc-aws-kinesis",
  libraryDependencies ++= Seq(
    "com.gilt"      %% "gfc-util"              % "0.2.2",
    "com.gilt"      %% "gfc-logging"           % "0.0.8",
    "com.gilt"      %% "gfc-concurrent"        % "0.3.8",
    "com.amazonaws" %  "aws-java-sdk-kinesis"  % "1.11.333",
    "com.amazonaws" %  "amazon-kinesis-client" % "1.9.0",
    "com.amazonaws" % "dynamodb-streams-kinesis-adapter" % "1.2.2",
    "org.specs2"    %% "specs2-scalacheck"     % "4.0.3" % Test
  )
)

lazy val akka = (project in file("akka"))
  .settings(commonSettings:_*)
  .settings(
    name := "gfc-aws-kinesis-akka",
    libraryDependencies += "com.typesafe.akka" %% "akka-stream" % "2.5.6")
  .dependsOn(client)

lazy val root = (project in file("."))
  .settings(commonSettings: _*)
  .settings(publishArtifact := false)
  .aggregate(client, akka)
