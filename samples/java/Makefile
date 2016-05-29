# Defines the location of project directories.
CLASS_DIR := bin
LIB_DIR := lib
SRC_DIR := src

# Compilation variables
JAVAC := javac
JFLAGS := -g -d $(CLASS_DIR) -sourcepath $(SRC_DIR) -Xlint:all

# Define the classpath
PROJECT_CLASSPATH := $(CLASS_DIR):$(LIB_DIR)/*

# If there's already a CLASSPATH, prepend our classpath
ifneq ($(CLASSPATH),)
        PROJECT_CLASSPATH := $(CLASSPATH):$(PROJECT_CLASSPATH)
endif

# Re-export the CLASSPATH.
export CLASSPATH := $(PROJECT_CLASSPATH)

# Defines the location of class files.
CLASSES := $(patsubst src/%.java,bin/%.class,$(wildcard src/*.java))

.SUFFIXES: .java .class
.PHONY: all clean

all: $(CLASSES)

# Default rule to build a class
$(CLASSES): $(CLASS_DIR)/%.class: $(SRC_DIR)/%.java | $(CLASS_DIR)
	$(JAVAC) $(JFLAGS) $<

$(CLASS_DIR):
	mkdir -p $@

clean:
	rm -rf $(CLASS_DIR)/*
