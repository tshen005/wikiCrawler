all:
	javac -cp "./:./lib/commons-cli-1.4.jar:./lib/jsoup-1.11.2.jar:./lib/sqlite-jdbc-3.21.0.jar:./lib/lucene-analyzers-common-7.2.1.jar:./lib/lucene-core-7.2.1.jar" src/edu/ucr/cs242/*.java src/edu/ucr/cs242/crawler/*.java src/edu/ucr/cs242/indexing/*.java

clean:
	find src/ -type f -name "*.class" -delete 