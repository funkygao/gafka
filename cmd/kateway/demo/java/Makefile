all:
	@echo hello

generate:
	mvn archetype:generate -DgroupId=com.foo -DartifactId=PubsubDemo -DarchetypeArtifactId=maven-archetype-quickstart -DinteractiveMode=false

run:
	cd PubsubDemo; mvn package; java -cp target/PubsubDemo-1.0-SNAPSHOT.jar com.foo.App

idea:
	cd PubsubDemo; mvn idea:idea


