cd flink-core || exit
mvn clean install -Dcheckstyle.skip -DskipTests -Dmaven.javadoc.skip
cd ../flink-filesystems || exit
mvn clean install -Dcheckstyle.skip -DskipTests -Dmaven.javadoc.skip
cd ../flink-runtime || exit
mvn clean install -Dcheckstyle.skip -DskipTests -Dmaven.javadoc.skip
cd ../flink-streaming-java || exit
mvn clean install -Dcheckstyle.skip -DskipTests -Dmaven.javadoc.skip