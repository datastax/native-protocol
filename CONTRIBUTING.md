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

Some aspects are not covered by the formatter:
* imports: please configure your IDE to follow the guide (no wildcard imports, normal imports 
  in ASCII sort order come first, followed by a blank line, followed by static imports in ASCII
  sort order).
* XML files: indent with two spaces and try to respect the column limit of 100 characters.

## Coding style

Avoid static imports, with those exceptions:
* `ProtocolConstants.Version` constants (`V3`, `V4`, etc).
* AssertJ's `assertThat` / `fail` in unit tests.

Tests:
* test methods names use lower snake case, generally start with "should" and clearly indicate the
  purpose of the test, for example: `should_fail_if_key_already_exists`. If you have trouble coming
  up with a simple name, it might be a sign that your method does too much and should be split.
* we use AssertJ (`assertThat`). Don't use TestNG's assertions (`assertEquals`, `assertNull`, etc).
* don't try to generify at all cost: a bit of duplication is acceptable, if that helps keep the
  tests simple to understand (a newcomer should be able to understand how to fix a failing test
  without having to read too much code).
* given the simplicity of this project, it's unlikely that we'll ever need integration tests: don't
  start any external process from the test, the whole suite should run in a couple of seconds.

## License headers

The build will fail if some license headers are missing. To update all files from the command line,
run:

```
mvn license:format
```

## Pre-commit hook (highly recommended)
 
Ensure `pre-commit.sh` is executable, then run:

```
ln -s ../../pre-commit.sh .git/hooks/pre-commit
```

This will only allow commits if the tests pass. It is also a good reminder to keep the test suite
short. 
