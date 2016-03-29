# Contributing guidelines

## Design goals

* no runtime dependencies
* extensibility: it is possible to add new message codecs, new subcodecs for result kinds, errors
  and even a whole new protocol version 

## Code formatting

We follow the [Google Java Style Guide](https://google.github.io/styleguide/javaguide.html). See
https://github.com/google/google-java-format for IDE plugins. The rules are not configurable.

The build will fail if the code is not formatted. To format all files from the command line, run:
 
```
mvn fmt:format -Dformat.validateOnly=false
```

## License headers

The build will fail if some license headers are missing. To update all files from the command line,
run:

```
mvn license:format
```

## TODO

* implement (and test) request deserialization and response serialization. Look for 
  `UnsupportedOperationException` in the code.
* implement protocol v5 beta.