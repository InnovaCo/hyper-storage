addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.6.0")
addSbtPlugin("eu.inn" % "hyperbus-sbt-plugin" % "0.1.62")

resolvers ++= Seq(
  "Innova plugins" at "http://repproxy.srv.inn.ru/artifactory/plugins-release-local"
)
