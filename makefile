setup:
	sudo apt update && \
		sudo apt-get install -y gcc python3-dev && \
		pip3 install -r ./notebooks/requirements.txt
	
	curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sudo sh && \
		databricks configure

# Run unit tests
test:
	sbt coverage test

# Run integration tests
it:
	sbt coverage IntegrationTest/test

# Build the jar
jar:
	sbt clean assembly
	
# Build the thin jar
thin-jar:
	sbt clean compile package

format:
	sbt scalafmtAll scalafmtSbt

clean:
	sbt clean

cov: clean test it
	sbt coverageReport