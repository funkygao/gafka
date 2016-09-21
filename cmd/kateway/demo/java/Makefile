all:
	@echo hello

generate:
	mvn archetype:generate -DgroupId=com.foo -DartifactId=PubsubDemo -DarchetypeArtifactId=maven-archetype-quickstart -DinteractiveMode=false

run:
	cd PubsubDemo; mvn package; mvn exec:java -Dexec.mainClass=com.foo.App

idea:
	cd PubsubDemo; mvn idea:idea


