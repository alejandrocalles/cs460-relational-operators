name := "project-1"

version := "0.1"

scalaVersion := "3.3.1"
val skeletonVersion = "2025.1.5"
val calciteVersion = "1.26.0"

ThisBuild / useCoursier := false

resolvers += Resolver.jcenterRepo
resolvers += "GitLab Packages Registry" at "https://gitlab.com/api/v4/projects/66826293/packages/maven"

initialize := {
  val _ = initialize.value // run the previous initialization
  val required = VersionNumber("11")
  val current  = VersionNumber(sys.props("java.specification.version"))
  assert(current.matchesSemVer(SemanticSelector(">=11")), s"Unsupported JDK: java.specification.version $current < $required")
}

// https://mvnrepository.com/artifact/org.apache.calcite/calcite-core
// Include Calcite Core
libraryDependencies += "org.apache.calcite" % "calcite-core" % calciteVersion

libraryDependencies += "ch.epfl.dias.cs460" %% "base" % skeletonVersion
libraryDependencies += "ch.epfl.dias.cs460" %% "base" % skeletonVersion % Test classifier "tests"

// https://mvnrepository.com/artifact/org.junit.jupiter/junit-jupiter-api
libraryDependencies += "org.junit.jupiter" % "junit-jupiter-api" % "5.3.1" % Test
libraryDependencies += "org.junit.jupiter" % "junit-jupiter-params" % "5.3.1" % Test

// junit tests (invoke with `sbt test`)
libraryDependencies += "com.novocode" % "junit-interface" % "0.11" % "test"

libraryDependencies += "net.aichler" % "jupiter-interface" % JupiterKeys.jupiterVersion.value % Test

testOptions += Tests.Argument(jupiterTestFramework, "-q")
