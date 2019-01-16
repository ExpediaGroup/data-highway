# Data Highway

## Building

```
# Normal build (jib build with push to ECR)
mvn clean package -T1C

# With docker daemon
mvn clean package -Djib.goal=dockerBuild -T1C

# Without docker images
mvn clean package -Djib.skip=true -T1C

# Without tests
mvn clean package -DskipTests

# Without docker images or tests
mvn clean package -DskipTests -Djib.skip=true -T1C
```