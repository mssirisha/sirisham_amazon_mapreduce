# sirisham_amazon_mapreduce
Java code for map reduce on dsba-hadoop.uncc.edu. There are 14,741,571 amazon products and 153,871,242 amazon product reviews.

# Executing AmazonProductDescAnalysis - To find topp 100 words in Product description after parsing out HTML and removing stopwords
 

Log into dsba-hadoop.uncc.edu using ssh
git clone https://github.com/mssirisha/sirisham_amazon_mapreduce.git to clone this repo

Go into the repo directory. In this case: cd sirisham_amazon_mapreduce

Make a "build" directory (if it does not already exist): mkdir build

Compile the java code (all one line). You may see some warnings--that' ok. 
javac -cp /opt/cloudera/parcels/CDH/lib/hadoop/client/*:/opt/cloudera/parcels/CDH/lib/hbase/* AmazonProductDescAnalysis.java -d build -Xlint

Now we wrap up our code into a Java "jar" file: jar -cvf process_product_desc.jar -C build/ .

This is the final step
Note that you will need to delete the output folder if it already exists: 
hadoop fs -rm -r /user/smanam/product_fields 
otherwise you will get an "Exception in thread "main" org.apache.hadoop.mapred.FileAlreadyExistsException: Output directory hdfs://dsba-nameservice/user/... type of error.

Now we execute the map-reduce job: 
HADOOP_CLASSPATH=$(hbase mapredcp):/etc/hbase/conf hadoop jar process_product_desc.jar AmazonProductDescAnalysis '/user/smanam/product_fields' -skip /user/smanam/amazonProductDesc/input/stopwords.txt


Once that job completes, you can concatenate the output across all output files with: hadoop fs -cat /user/smanam/product_fields/* or if you have output that is too big for displaying on the terminal screen you can do 
hadoop fs -cat /user/smanam/product_fields/* > output.txt 
to redirect all output to output.txt

for top100 words in the product description
hadoop fs -cat /user/smanam/product_fields/* | sort -n -k2 -r | head -n100 > top100.txt
