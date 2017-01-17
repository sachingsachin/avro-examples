# avro-examples

Most of the avro-related functionality is put in `BaseAvroTest.java` class.
Other classes can just extend this class and focus on testing the features.


# Running avro-examples

```bash
mvn clean test
```

# Failing tests as of now
```
  testInterfaces(examples.InterfaceTypesTest): java.lang.NoSuchMethodException: java.util.Map.<init>()
  testParameterizedSchema(examples.ParameterizedTypesTest): Unknown type: P
  testCircularRefs(examples.TestCircularRefs): java.lang.StackOverflowError
```

# Supported constructs
1. UUID
2. Non-String Map-Keys
3. Byte-arrays
