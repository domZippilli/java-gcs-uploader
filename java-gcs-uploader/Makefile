.PHONY: compile blob 

compile:
	export JAVA_1_9_HOME=/usr/local/buildtools/java/jdk9 && \
	mvn clean compile assembly:single 
	chmod 775 target/java-gcs-uploader-1.0-SNAPSHOT-jar-with-dependencies.jar 

blob:
	dd if=/dev/urandom of=../sampleblob bs=64M count=32
