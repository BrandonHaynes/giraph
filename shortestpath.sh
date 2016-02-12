sudo -u hduser $HADOOP_HOME/bin/hadoop jar /usr/local/giraph/giraph-examples/target/giraph-examples-1.1.0-for-hadoop-2.6.0-jar-with-dependencies.jar org.apache.giraph.GiraphRunner -Dmapred.job.tracker org.apache.giraph.examples.SimpleShortestPathsComputation -vif org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexInputFormat -vip /tiny_graph.txt -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op /ushortestpaths9 -w 1 -ca giraph.SplitMasterWorker=false