import AssemblyKeys._

seq(assemblySettings: _*)

  mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
    {
      case x => {
        val oldstrat = old(x)
          if (oldstrat == MergeStrategy.deduplicate) MergeStrategy.first
          else oldstrat
      }
    }
  }

libraryDependencies  ++= Seq(
    "org.scalanlp" %% "breeze-math" % "0.2",
    "com.twitter" %% "scalding-core" % "0.8.4", 
    "org.scalacheck" %% "scalacheck" % "1.10.1" % "test"
    )

resolvers ++= Seq(
    "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
    "Conjars" at "http://conjars.org/repo"
    )

test in assembly := {}

mainClass in assembly := None

scalaVersion := "2.9.2"

name := "ScaldingLinalg"

version := "0.1-SNAPSHOT"

