name := "BasicCacheCoherency"

version := "1.0"

libraryDependencies += "net.sf.ehcache" % "ehcache-core" % "2.6.5"
libraryDependencies += "net.sf.ehcache" % "ehcache-jgroupsreplication" % "1.7"
libraryDependencies += "org.jgroups" % "jgroups" % "3.6.7.Final"
libraryDependencies += "org.infinispan" % "infinispan-core" % "7.1.0.Final"