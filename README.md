# Avro Utils

Provides a set of utilities for use with Avro:

## IsAvroObjectEqual.java

[HamCrest](http://hamcrest.org) matcher to assert Avro Specific objects, and use Avro with mocking frameworks such as Jmock and Mockito. Usage:

```import static org.hamcrest.MatcherAssert.assertThat;
import static com.byhiras.avro.IsAvroObjectEqual.isAvroObjectEqualTo
...
assertThat( actualResult, isAvroObjectEqualTo( expectedResult ));```

Or with expectations:

```mockery.checking(new Expectations() {{
   oneOf().myFunction( with(isAvroObjectEqualTo( expectedAvroObject )));
}});```

## License

Copyright 2013 Byhiras (Europe) Limited

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at: 

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
