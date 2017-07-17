# Customize these paths for your environment.
# -----------------------------------------------------------
#hadoop.root=/usr/bin/hadoop/hadoop-2.7.3
hadoop.root=/home/anpspk/Documents/hadoop-2.7.3
jar.name=PRank-assembly-1.0.jar
maven.jar.name=PageRank-0.0.1-SNAPSHOT.jar
job.name=PageRank
local.input=input/Scala
local.output=output
local.log=log_part1
# Pseudo-Cluster Execution
hdfs.user.name=joe
hdfs.input=input
hdfs.output=output
# AWS EMR Execution
aws.region=us-east-1
aws.bucket.name=anpspk-hw4-parallelism-5
aws.subnet.id=subnet-2ec26067
aws.input=input
aws.output=output2-10machines
aws.log.dir=log2-10machines
aws.num.nodes=5
yarn.local=local
aws.instance.type=m4.large
yarn.master=yarn-cluster
# -----------------------------------------------------------

# Compiles code and builds jar (with dependencies).
jar:
	sbt package
	cp target/scala-2.11/PRank-assembly-1.0.jar ${jar.name}

jar-old-school:
	rm -rf scratch
	mkdir scratch
	javac -d scratch -cp src:${hadoop.root}/share/hadoop/common/hadoop-common-2.7.3.jar:${hadoop.root}/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.7.3.jar \
			$(shell find ./src/weather/asgn2/* | grep .java)
	jar cf ${jar.name} -C scratch weather
	rm -r scratch

# Removes local output directory.
clean-local-output:
	rm -rf ${local.output}

clean-local-log:
	rm -rf ${local.log}



# Runs standalone
# Make sure Hadoop  is set up (in /etc/hadoop files) for standalone operation (not pseudo-cluster).
# https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Standalone_Operation
alone: jar clean-local-output
	spark-submit --class ${job.name} --master local[4] ${jar.name} ${yarn.local} ${local.input} ${local.output}


# Start HDFS
start-hdfs:
	${hadoop.root}/sbin/start-dfs.sh

# Stop HDFS
stop-hdfs: 
	${hadoop.root}/sbin/stop-dfs.sh
	
# Start YARN
start-yarn: stop-yarn
	${hadoop.root}/sbin/start-yarn.sh

# Stop YARN
stop-yarn:
	${hadoop.root}/sbin/stop-yarn.sh

# Reformats & initializes HDFS.
format-hdfs: stop-hdfs
	rm -rf /tmp/hadoop*
	${hadoop.root}/bin/hdfs namenode -format

# Initializes user & input directories of HDFS.	
init-hdfs: start-hdfs
	${hadoop.root}/bin/hdfs dfs -rm -r -f /user
	${hadoop.root}/bin/hdfs dfs -mkdir /user
	${hadoop.root}/bin/hdfs dfs -mkdir /user/${hdfs.user.name}
	${hadoop.root}/bin/hdfs dfs -mkdir /user/${hdfs.user.name}/${hdfs.input}

# Load data to HDFS
upload-input-hdfs: start-hdfs
	${hadoop.root}/bin/hdfs dfs -put ${local.input}/* /user/${hdfs.user.name}/${hdfs.input}

# Removes hdfs output directory.
clean-hdfs-output:
	${hadoop.root}/bin/hdfs dfs -rm -r -f ${hdfs.output}

# Download output from HDFS to local.
download-output:
	mkdir ${local.output}
	${hadoop.root}/bin/hdfs dfs -get ${hdfs.output}/* ${local.output}

# Runs pseudo-clustered (ALL). ONLY RUN THIS ONCE, THEN USE: make pseudoq
# Make sure Hadoop  is set up (in /etc/hadoop files) for pseudo-clustered operation (not standalone).
# https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation
pseudo: jar stop-yarn format-hdfs init-hdfs upload-input-hdfs start-yarn clean-local-output 
	${hadoop.root}/bin/hadoop jar ${jar.name} ${job.name} ${hdfs.input} ${hdfs.output}
	make download-output

# Runs pseudo-clustered (quickie).
pseudoq: jar clean-local-output clean-hdfs-output 
	${hadoop.root}/bin/hadoop jar ${jar.name} ${job.name} ${hdfs.input} ${hdfs.output}
	make download-output

# Create S3 bucket.
make-bucket:
	aws s3 mb s3://${aws.bucket.name}

# Upload data to S3 input dir.
upload-input-aws: make-bucket
	aws s3 sync ${local.input} s3://${aws.bucket.name}/${aws.input}
	
# Delete S3 output dir.
delete-output-aws:
	aws s3 rm s3://${aws.bucket.name}/${aws.output} --recursive

# Upload application to S3 bucket.
upload-app-aws:
	aws s3 cp ${jar.name} s3://${aws.bucket.name}

# Main EMR launch.
cloud: jar upload-app-aws delete-output-aws
	aws emr create-cluster \
		--name "Proe Processor2:" \
		--release-label emr-5.0.0 \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Hadoop \
	    --steps '[{"Args":["${job.name}","s3://${aws.bucket.name}/${aws.input}","s3://${aws.bucket.name}/${aws.output}"],"Type":"CUSTOM_JAR","Jar":"s3://${aws.bucket.name}/${jar.name}","ActionOnFailure":"TERMINATE_CLUSTER","Name":"Custom JAR"}]' \
		--log-uri s3://${aws.bucket.name}/${aws.log.dir} \
		--service-role EMR_DefaultRole \
		--ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,SubnetId=${aws.subnet.id} \
		--region ${aws.region} \
		--enable-debugging \
		--auto-terminate

cloud-spark: delete-output-aws
	aws emr create-cluster \
	--name "Page Spark Cluster 5 Machines Parallelism" \
	--release-label emr-5.0.3 \
	--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
        --applications Name=Hadoop Name=Spark \
        --steps '[{"Args":["spark-submit","--deploy-mode","cluster","--class","${job.name}","s3://${aws.bucket.name}/${jar.name}","${yarn.master}","s3://${aws.bucket.name}/${aws.input}","s3://${aws.bucket.name}/${aws.output}"],"Type":"CUSTOM_JAR","ActionOnFailure":"TERMINATE_CLUSTER","Jar":"command-runner.jar","Properties":"","Name":"SparkApp"}]' \
	--log-uri s3://${aws.bucket.name}/${aws.log.dir} \
	--service-role EMR_DefaultRole \
	--ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,SubnetId=${aws.subnet.id} \
	--region ${aws.region} \
	--enable-debugging \
	--auto-terminate

# Download output from S3.
download-output-aws: clean-local-output
	mkdir ${local.output}
	aws s3 sync s3://${aws.bucket.name}/${aws.output} ${local.output}

download-log-aws: clean-local-log
	mkdir ${local.log}
	aws s3 sync s3://${aws.bucket.name}/${aws.log.dir} ${local.log}

# Change to standalone mode.
switch-standalone:
	cp config/standalone/*.xml ${hadoop.root}/etc/hadoop

# Change to pseudo-cluster mode.
switch-pseudo:
	cp config/pseudo/*.xml ${hadoop.root}/etc/hadoop

# Package for release.
distro:
	rm -rf build
	mkdir build
	mkdir build/deliv
	mkdir build/deliv/WeatherJ
	cp pom.xml build/deliv/WeatherJ
	cp -r src build/deliv/WeatherJ
	cp Makefile build/deliv/WeatherJ
	cp README.txt build/deliv/WeatherJ
	tar -czf WeatherJ.tar.gz -C build/deliv WeatherJ
	cd build/deliv && zip -rq ../../WeatherJ.zip WeatherJ
	
