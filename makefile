setup:
	sudo apt update && \
		sudo apt-get install -y gcc python3-dev && \
		pip3 install -r ./notebooks/requirements.txt

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