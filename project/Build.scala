import sbt._
import Keys._

object Build extends Build {

  lazy val root = project.in(file(".")).aggregate(core,repl)

  lazy val core = project.settings(
    organization       := "org.apache",
    version            := "0.0.1",
    scalaVersion       := "2.10.4",
    libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "1.1.0" % "provided",
                                "org.apache.hadoop" % "hadoop-client" % "1.0.4" % "provided",
                                "org.apache.commons" % "commons-math3" % "3.0",
                                "org.scalatest"    %% "scalatest"       % "1.9.1"  % "test",
                                "org.scalanlp" %% "breeze" % "0.7",
                                "org.scalanlp" %% "breeze-natives" % "0.7",
                                "org.apache.spark" % "spark-mllib_2.10" % "1.1.0",
                                "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly()

                              ),
    resolvers  ++= Seq("Apache Repository" at "https://repository.apache.org/content/repositories/releases",
                       "Akka Repository" at "http://repo.akka.io/releases/",
                       "netlib Repository" at "http://repo1.maven.org/maven2/",
                       "Spray Repository" at "http://repo.spray.cc/")
  )

  lazy val repl = project.dependsOn(core)

}

