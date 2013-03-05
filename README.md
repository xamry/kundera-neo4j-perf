Performance Number Running:
----------------------------

1. Create Schema:

create database performanceNos;

use performanceNos;

CREATE TABLE PerformanceNo ( id INT NOT NULL AUTO_INCREMENT , date DATE NOT NULL , clientType VARCHAR(45) NOT NULL , totalTimeTaken DOUBLE NOT NULL ,releaseNo DOUBLE NOT NULL , noOfThreads INT NOT NULL , noOfOperations BIGINT NOT NULL , testType VARCHAR(45) NOT NULL , runSequence INT NOT NULL , PRIMARY KEY (id) );

select * from PerformanceNo;



2. Run Test

mvn clean install -DskipTests
mvn assembly:assembly -DskipTests
mvn test -DfileName=src/main/resources/db.properties
