# SentimentAnalysis_LambdaArchitecture
This project contains the implementation of a Lambda Architecure that allows to perform sentiment analysis of tweets loaded from Twitter.<br>
Specifically, the architecture allows you to calculate and compare the number of positive, neutral and negative tweets containing a given word <b>(query)</b>.<br>
To classify a tweet written in a given language, each term is expected to be compared with a set of words contained in a file corrisponding to the language of tweet (AFINN files) that associates a sentiment value in a range from -5 (negative) to +5 (positive).
The sum of the sentiment values of each word in the tweet is defined as the sentiment value of the tweet.<br>
The Lambda Architecture presented involves the use of the following software systems:
<ul>
<li><b>Apache Hadoop:</b> for the batch layer</li>
<li><b>Apache Storm:</b> for the speed layer</li>
<li><b>Apache HBase:</b> for the serving layer</li>
</ul>
<br>
There is then a software module (situated in the print_result package) which contains the code that allows you to query HBase and display a graph showing its results.

<h3>Batch Layer</h3>
The batch layer was created with Apache Hadoop. <br>
This involves analyzing the data contained in the master dataset, the path of which must be defined in the twitter_config.properties file.
To operate, the mapper reads the tweets from the dataset, filters them considering only those that contain the requested query and classifies them by sending the classification to the reducer.<br>
The reducer counts how many tweets are received for a given classification and copies the values in the relative table contained in hbase.<br>
It is important to note that before each batch cycle the timestamp is calculated so that only tweets that have not been processed by the speed layer in the time elapsed between the start of the batch cycle and the current time are considered.

<h3>Speed Layer</h3>
The speed layer was implemented with Apache Storm.<br>
This involves the analysis of tweets downloaded from Twitter in streaming.The topology has a spout and two bolts<br>
The spout downloads the tweets that contains the query through the Twitter4j library and the credentials provided by Twitter after creating an app in the developer section. After that it sends the tweet to the first bolt of the topology.<br>
The first bolt (ParserBolt) filters the tweets considering only those written in a language of which you have the afinn file and classifies them in positive, neutral and negative after calculating their sentiment value. 
The send to the last bolt of the topology the tuple composed by tweet and its classification.<br>
The last bolt of the topology (CalculatorBolt) reads the data corresponding to the classification of the tweet from the table for the query and increase this value by one.<br>
Finally, bolt append newly computed tweet by adding the language of the tweet and the timestamp of the current moment to the basic information.<br>
The language of the tweet will be used by batch layer in the next cycle of computation to load the correct afinn file for calculation of the sentiment value of the tweet. 
Instead the timestamp is necessary to the mapper of hadoop to decide whether or not to compute the tweet during the current computation.<br>
<h2>Prerequisistes</h2>
To run the program it is necessary to download and link to the project of the following libraries:
<ul>
  <li><b>Apache Hadoop:</b> available at <a href="https://hadoop.apache.org/docs/r3.0.0/"> https://hadoop.apache.org/docs/r3.0.0/ </a></li>
  <li><b>Apache Storm:</b> available at <a  href="https://storm.apache.org/downloads.html">https://storm.apache.org/downloads.html</a></li>
  <li><b>Apache HBase:</b> available at <a href="https://hbase.apache.org/downloads.html" >https://hbase.apache.org/downloads.html </a></li>
  <li><b>Twitter4j:</b> available from Maven at link: org.twitter4j:twitter4j-stream:4.0.3</li>
  <li><b>jfree:</b> available from Maven at the two link: org.jfree:jcommon:1.0.24 and org.jfree:jfreechart:1.5.0</li>
  <li><b>google.guava:</b> available from Maven at link: com.google.guava:guava:23.0</li>
  <li><b>googlecode.disruptor:</b> available from Maven at link: com.googlecode.disruptor:disruptor:2.10.1</li>
</ul>
Once downloaded and configured HBase it is also necessary to start the database by performing the following operation from the terminal: <em>start-hbase.sh</em>

<h2>How to use the code</h2>
To use the code it is necessary to set the following settings in the <b>twitter_config.properties</b> file:

<ul>
<li><b>Twitter app keys:</b> for loading tweets by the speed layer (its property is not necessary by the batch layer). </li>
<li><b>Query:</b> the word that you want to filter </li>
<li><b>Path of master dataset:</b> for reading of data by the batch layer and for writing of data by the speed layer.
This is required to be a .csv file in which the attributes of the tweet are separated by the ',' symbol. </li>
<li><b>Index of the data in a line of dataset:</b> its necessary to indicate in which position of the row of the master dataset the information about text, timestamp is found. 
This information is used by batch layer for computing corretly the data and by speed layer for write the new information in the correct position.</li>
</ul>
