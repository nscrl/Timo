
name := "Timo"
organization :="nscrl"
version := "0.1"

scalaVersion := "2.12.8"
libraryDependencies += "org.apache.spark" % "spark-core_2.12" % "2.4.0"
libraryDependencies += "org.apache.spark" % "spark-catalyst_2.12" % "2.4.0"
libraryDependencies += "org.apache.spark" % "spark-sql_2.12" % "2.4.0"

//libraryDependencies += "com.vividsolutions" % "jts-core" % "1.14.0"
//libraryDependencies +="org.scalatest" %% "scalatest" % "3.1.0-SNAP6" % Test