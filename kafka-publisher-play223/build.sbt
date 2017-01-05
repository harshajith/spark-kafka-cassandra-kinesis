name := "kafka-publisher"

version := "1.0-SNAPSHOT"

libraryDependencies ++= Seq(
  javaJdbc,
  javaEbean,
  cache,
  "com.redmart.kafka"   %  "event-publisher" % "1.2",
  "org.apache.kafka" % "kafka_2.10" % "0.8.1.1" intransitive(),
  "com.yammer.metrics" % "metrics-core" % "2.2.0",
  "org.slf4j" % "slf4j-api" % "1.7.2",
  "log4j" % "log4j" % "1.2.17",
  "org.scala-lang" % "scala-library" % "2.10.1",
  "com.google.code.gson" % "gson" % "2.2.4",
  "com.redmart.kafka"   %  "event-publisher" % "1.0",
  "com.rabbitmq" % "amqp-client" % "3.3.0",
  "com.redmart.rabbitmq.integ" % "publisher" % "1.0",
  "com.datastax.cassandra" % "cassandra-driver-core" % "2.1.4",
  "com.google.guava" % "guava" % "14.0.1",
  "com.codahale.metrics" % "metrics-core" % "3.0.0",
  "io.netty" % "netty" % "3.9.0.Final"
)     

play.Project.playJavaSettings

resolvers += "maven-repo" at "https://github.com/Redmart/maven-repo/raw/master/"

resolvers += Resolver.url("play-easymail (release)", url("http://joscha.github.com/play-easymail/repo/releases/"))(Resolver.ivyStylePatterns)

resolvers += Resolver.url("play-easymail (snapshot)", url("http://joscha.github.com/play-easymail/repo/snapshots/"))(Resolver.ivyStylePatterns)

resolvers += Resolver.url("typesafe", url("http://repo.typesafe.com/typesafe/releases/"))

resolvers += "google-sedis-fix" at "http://pk11-scratch.googlecode.com/svn/trunk"
