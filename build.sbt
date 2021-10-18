name := "transformer-spark-scala"

version := "0.0.1"

scalaVersion := "2.12.10"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.8" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.4.8" % "provided" // for integration testing

libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4"

//libraryDependencies += "com.github.mrpowers" % "spark-daria" % "0.27.1-s_2.12"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.9" % Test
//libraryDependencies += "org.scalamock" %% "scalamock" % "4.4.0" % Test
libraryDependencies += "org.mockito" %% "mockito-scala" % "1.16.42" % Test
libraryDependencies += "com.github.mrpowers" %% "spark-fast-tests" % "1.0.0" % "test"

// test suite settings
Test / fork := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
// Show runtime of tests
Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oD")

// JAR file settings

// don't include Scala in the JAR file
//assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

// Add the JAR file naming conventions described here: https://github.com/MrPowers/spark-style-guide#jar-files
// You can add the JAR file naming conventions by running the shell script
