# How to

## Prerequisites

* You have active AWS account
* AWS SQS queue created
* AWS local config set up as per [Setting the default credentials](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/setup.html)
* Maven installed
* Java 17+ installed

## Test

* Run in your favourite IDE

OR

* `mvn clean package`
* `java -jar target/*.jar <queue_name>`
