# Definitions
JAVAC = javac
JAVA = java
JAR = client.jar
SRC_DIR = .

# Compiler flags
JFLAGS = -cp $(JAR)

# Java programs

CONTROLLER = Controller
DSTORE = Dstore

# Default target
all: compile

# Compile Java files
compile: $(CONTROLLER).java $(DSTORE).java

	$(JAVAC) $(SRC_DIR)/$(CONTROLLER).java
	$(JAVAC) $(SRC_DIR)/$(DSTORE).java

# Run the Controller
run-controller:
	$(JAVA) $(CONTROLLER) 12345 3 1000 30

# Run a Dstore
run-dstore:
	$(JAVA) $(DSTORE) 12346 12345 1000 "data"

run-dstore2:
	$(JAVA) $(DSTORE) 12347 12345 1000 "data2"


# Clean up
clean:
	rm -f $(SRC_DIR)/*.class

# Help
help:
	@echo "Makefile for compiling and running a distributed file system"
	@echo ""
	@echo "Available commands:"
	@echo "  make            - Compile all Java classes."
	@echo "  make compile    - Compile all source files."
	@echo "  make run-controller - Start the Controller."
	@echo "  make run-dstore - Start a Dstore."
	@echo "  make run-client - Start a Client."
	@echo "  make clean      - Remove all compiled files."
	@echo "  make help       - Display this help."
