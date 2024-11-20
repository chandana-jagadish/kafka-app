# kafka-app

To run, update the kafka bootstrap server port number in the below files:
1. app/BeamDriver.java
2. app/consumers/EvenConsumer.java
3. app/consumers/OddConsumer.java

The below kafka topics need to be created:
1. INPUT_TOPIC
2. EVEN_TOPIC
3. ODD_TOPIC

Sample input messages:
```
person1:{"name": "Person1", "address": "123 First St", "dateOfBirth": "1990-01-01"}
person2:{"name": "Person2", "address": "345 Second St", "dateOfBirth": "1997-07-12"}
person3:{"name": "Person3", "address": "567 Third St", "dateOfBirth": "1963-12-15"}
person4:{"name": "Person4", "address": "123 Third St", "dateOfBirth": "1962-12-15"}
```
