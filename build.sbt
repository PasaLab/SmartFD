name := "SparkFD"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies += "org.apache.spark" %% "spark-core" %"2.1.0" % "provided"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.3" % "provided"

resolvers  += "MavenRepository" at "http://central.maven.org/maven2"
