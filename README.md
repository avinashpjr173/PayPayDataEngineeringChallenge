Q.DataEngineerChallenge
This is an interview challenge for PayPay. Please feel free to fork. Pull Requests will be ignored.

The challenge is to make make analytical observations about the data using the distributed tools below.

Processing & Analytical goals:
1.Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a session. https://en.wikipedia.org/wiki/Session_(web_analytics)

2.Determine the average session time

3.Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.

4.Find the most engaged users, ie the IPs with the longest session times


About Solution:

The Solution has been entirely built on SPARK SQL/DATAFRAME and window function

Note:
IDE used is IntelliJ -- Project was built on Scala Sbt .

The solution will be running on local machine and doesn't use any distribution(cloudera) or Hadoop connections.

The input and output location will be in local machine.

How to run:

Just import the project in IDE and build the project.

then run the scala object .






