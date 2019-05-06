### Joining Data & Performance Tuning

#### Goals:
- Understand how to do basic joins with spark in Shopping class

    - Practice with left, right, inner joins

- Analyze DAGs in Employee Class
    - Broadcast joins
    - Shuffle Joins vs. Broadcast joins


#### How to run spark program through Intellij?
- Build project using
    sbt package
- Set main class as 
    org.apache.spark.deploy.SparkSubmit
- Set program arguments as
   --master local -- class <main_class> target/scala-2.12/<jar_name>
