# Project 3 - STEDI Human Balance Analytics

## Project Introduction
In this project, you'll act as a data engineer for the STEDI team to build a data lakehouse solution for sensor data that trains a machine learning model.

## Project Details
The STEDI Team has been hard at work developing a hardware STEDI Step Trainer that:

trains the user to do a STEDI balance exercise;
and has sensors on the device that collect data to train a machine-learning algorithm to detect steps;
has a companion mobile app that collects customer data and interacts with the device sensors.
STEDI has heard from millions of early adopters who are willing to purchase the STEDI Step Trainers and use them.

Several customers have already received their Step Trainers, installed the mobile application, and begun using them together to test their balance. The Step Trainer is just a motion sensor that records the distance of the object detected. The app uses a mobile phone accelerometer to detect motion in the X, Y, and Z directions.

The STEDI team wants to use the motion sensor data to train a machine learning model to detect steps accurately in real-time. Privacy will be a primary consideration in deciding what data can be used.

Some of the early adopters have agreed to share their data for research purposes. Only these customers’ Step Trainer and accelerometer data should be used in the training data for the machine learning model.

## Project Structure
### Landing Zone
Create Glue Tables for customer, accelerator and step trainer landing datasets stored in the S3 bucket:
- [customer_landing.sql](https://github.com/ckishiye/Udacity-Data-Eng-with-AWS/blob/56bd404a8108f7fe286300042d1504fa9eca78bc/Project%20Spark%20Data%20Lakehouse/sql/customer_landing.sql)

  _customer_landing table_:
  ![customer_landing](https://github.com/user-attachments/assets/fa5b1a26-99d0-4981-b7e1-5472f84abf15)
  
- [accelerometer_landing.sql](https://github.com/ckishiye/Udacity-Data-Eng-with-AWS/blob/56bd404a8108f7fe286300042d1504fa9eca78bc/Project%20Spark%20Data%20Lakehouse/sql/accelerometer_landing.sql)

  _accelerometer_landing table_:
![accelerometer_landing](https://github.com/user-attachments/assets/6be68482-eeae-4cc4-8c72-54555562f0af)

- [step_trainer_landing.sql](https://github.com/ckishiye/Udacity-Data-Eng-with-AWS/blob/56bd404a8108f7fe286300042d1504fa9eca78bc/Project%20Spark%20Data%20Lakehouse/sql/step_trainer_landing.sql)

  _step_trainer_landing table_:
![step_trainer_landing](https://github.com/user-attachments/assets/fa1fc92a-5545-4721-a12c-226c207e102f)

### Trusted Zone
Glue Job Scripts and Athena query screenshots:
- [customer_landing_to_trusted.py](https://github.com/ckishiye/Udacity-Data-Eng-with-AWS/blob/56bd404a8108f7fe286300042d1504fa9eca78bc/Project%20Spark%20Data%20Lakehouse/jobs/customer_landing_to_trusted.py) - Filter protected PII with Spark in Glue Jobs.

  _customer_trusted table_:
  ![customer_trusted](https://github.com/user-attachments/assets/ef13259a-adfc-4e27-8fea-77b683d5a3a4)


- [accelerometer_landing_to_trusted.py](https://github.com/ckishiye/Udacity-Data-Eng-with-AWS/blob/56bd404a8108f7fe286300042d1504fa9eca78bc/Project%20Spark%20Data%20Lakehouse/jobs/accelerometer_landing_to_trusted.py) - Join privacy tables with Glue Jobs.

   _accelerometer_trusted table_:
 ![accelerometer_trusted](https://github.com/user-attachments/assets/235b0fa3-ea13-4c56-8c17-ac2b5289c536)


- [step_trainer_trusted.py](https://github.com/ckishiye/Udacity-Data-Eng-with-AWS/blob/ff320196210fd1993c5eaa930dbadeaa257c2825/Project%20Spark%20Data%20Lakehouse/jobs/step_trainer_trusted.py) - populate a Trusted Zone Glue Table called step_trainer_trusted that contains the Step Trainer Records data for customers who have accelerometer data and have agreed to share their data for research (customers_curated).

  _step_trainer_trusted table_:
![step_trainer_trusted](https://github.com/user-attachments/assets/c2acf32e-3c0c-4258-8f84-78f3b4776b62)


### Curated Zone 
Glue Job Scripts and Athena query screenshots:
- [customer_trusted_to_curated.py](https://github.com/ckishiye/Udacity-Data-Eng-with-AWS/blob/56bd404a8108f7fe286300042d1504fa9eca78bc/Project%20Spark%20Data%20Lakehouse/jobs/customer_trusted_to_curated.py) - Glue Job to join trusted data into curated table.

  _customer_curated table_:
![customer_curated](https://github.com/user-attachments/assets/e732f7eb-25ca-4144-9783-f1eee3587830)


- [machine_learning_curated.py](https://github.com/ckishiye/Udacity-Data-Eng-with-AWS/blob/56bd404a8108f7fe286300042d1504fa9eca78bc/Project%20Spark%20Data%20Lakehouse/jobs/machine_learning_curated.py) - aggregated table that has each of the Step Trainer Readings, and the associated accelerometer reading data for the same timestamp, but only for customers who have agreed to share their data.

  _machine_learning_curated table_:
![machine_learning_curated](https://github.com/user-attachments/assets/99d5d033-967c-4537-8d51-52b74a31b8db)
