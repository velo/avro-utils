# Avro Utils

[![Build Status](https://travis-ci.org/${owner}/${name}.svg?branch=master)](https://travis-ci.org/${owner}/${name}?branch=master) 
[![Coverage Status](https://coveralls.io/repos/github/${owner}/${name}/badge.svg?branch=master)](https://coveralls.io/github/${owner}/${name}?branch=master) 
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.marvinformatics/${name}/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.marvinformatics/${name}/) 
[![Issues](https://img.shields.io/github/issues/${owner}/${name}.svg)](https://github.com/${owner}/${name}/issues) 
[![Forks](https://img.shields.io/github/forks/${owner}/${name}.svg)](https://github.com/${owner}/${name}/network) 
[![Stars](https://img.shields.io/github/stars/${owner}/${name}.svg)](https://github.com/${owner}/${name}/stargazers)

Provides a set of utilities for use with Avro:

## IsAvroObjectEqual.java

[HamCrest](http://hamcrest.org) matcher to assert Avro Specific objects, and use Avro with mocking frameworks such as Jmock and Mockito. Usage:

```java
import static org.hamcrest.MatcherAssert.assertThat;
import static com.byhiras.avro.IsAvroObjectEqual.isAvroObjectEqualTo
...
assertThat( actualResult, isAvroObjectEqualTo( expectedResult ));
```

Or with expectations:

```java
mockery.checking(new Expectations() {{
   oneOf().myFunction( with(isAvroObjectEqualTo( expectedAvroObject )));
}});
```

## IsAvroIterableContaining.java

[HamCrest](http://hamcrest.org) matcher which takes an iterable of Avro objects and wraps them in IsAvoObjectEqualTo matchers. Also attempts to print out much more readable descriptions that pinpoint the item that is different. Usage:

```java
import static com.byhiras.avro.IsAvroIterableContaining.contains;
...
assertThat( actualList, contains( expectedList ));
```


## License

Copyright 2016 Byhiras (Europe) Limited

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at: 

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
