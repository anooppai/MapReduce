input_file=1912.csv
default:
	mvn package
clean:
	mvn clean
local: clean package 
	mvn exec:java -Dexec.args=${input_file}
