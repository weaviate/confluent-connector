# Run unit tests
test:
	sbt test

# Run integration tests
it:
	sbt it:test

# Build the jar
jar:
	sbt clean assembly
	
# Build the thin jar
thin-jar:
    sbt clean compile package