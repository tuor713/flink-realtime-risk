# Real Time Issuer Risk in Flink 

The project demonstrates a risk processing flow, with simulated inputs, that includes 
- live joins (on both sides)
- aggregation, as well as throttling
- limit calculation against a stream of thresholds

The project also introduces an API layer similar to Flink SQL API but with the ability to easily drop 
back to Datastreams. As part of that, it introduces a generic record type powered by a generic
Avro schema internally.

### Flow

The flow is structured along the lines of a traditional snowflake schema with dimensions
joining in from the further branches and converging on the main risk stream (aka the fact table).
One particular optimization is to optimize the final join of issuer risk, positions and issuers
as a single operator that uses broadcast on the issuer dimension to avoid multiple shuffle
steps.

The flow is as follows:

```
S1: Issuer join with parent isuer 
S2: Position join with firm account
S3: Issuer risk join with S2 and broadcasted S1
S4: Aggregate S3 to issuer level
S4: Join S3 with issuer limit thresholds and calculate limit utilization
```
