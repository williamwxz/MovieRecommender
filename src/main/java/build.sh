#!/bin/sh

# build jar
hadoop com.sun.tools.javac.Main *.java
jar cf recommender.jar *.class

mv recommender.jar ../../../
