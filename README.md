# Data Highway

## Building

```bash
# Normal build (jib build with push to ECR)
mvn clean package -T1C -Djib.httpTimeout=300000

# With docker daemon
mvn clean package -T1C -Djib.goal=dockerBuild -Djib.httpTimeout=300000

# Without docker images
mvn clean package -Djib.skip=true -T1C

# Without tests
mvn clean package -DskipTests -Djib.httpTimeout=300000

# Without docker images or tests
mvn clean package -DskipTests -Djib.skip=true -T1C
```
