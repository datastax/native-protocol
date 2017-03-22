# Native protocol layer for Apache Cassandra™
 
A set of Java types representing the frames and messages of the Apache Cassandra™ native protocol,
with the associated serialization and deserialization logic (this is a third-party implementation,
not related to the Apache Cassandra project).

Native protocol versions 3 and above are supported.

## Usage

The code is agnostic about the underlying binary representation: start by implementing a 
`PrimitiveCodec` for your target type `B` (which could be `ByteBuffer`, Netty's `ByteBuf`, 
`byte[]`, etc.)

You may also implement a `Compressor<B>` (it can be `null` if you're not going to compress frames).
 
Finally, build a `FrameCodec<B>` that will allow you to encode and decode frames. 
`Frame.defaultClient` and `Frame.defaultServer` give you the default sets of codecs for the 
protocol versions that are currently supported; alternatively, you can use the constructor
to register an arbitrary set of codecs.

`Frame`, `Message`, and `Message` subclasses are immutable, but for efficiency they don't make
defensive copies of their fields. If these fields are mutable (for example collections), they
shouldn't be modified after creating a message instance.

The code makes very few assumptions about how the messages will be used. Data is often represented
in the most simple way. For example, `ProtocolConstants` uses simple constants to represent the
protocol codes; client code can (and probably should) wrap them in more type-safe structures (such
as enums) before exposing them to higher-level layers.

Well-formed inputs are expected; if you pass an inconsistent `Frame` (ex: protocol v3 with a custom
payload), an `IllegalArgumentException` will be thrown.

## Versioning

We follow [semantic versioning](http://semver.org/). In addition, the minor (second) number matches
the highest *stable* protocol version supported. The next beta version might also be partially
supported. Examples:
* 1.4.0: supports up to protocol v4, and possibly some features of v5 beta.
* 1.4.1: bugfixes and/or new v5 beta features.
* 1.5.0: v5 stable.

All the types in this project are considered low-level and intended for framework developers, not
end users. We'll try to preserve binary compatibility (in particular for `Frame` and `Message` 
implementations), but we reserve the right to introduce breaking changes at any time.

## License

Copyright 2017, DataStax

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

----

DataStax is a registered trademark of DataStax, Inc. and its subsidiaries in the United States 
and/or other countries.

Apache Cassandra, Apache, Tomcat, Lucene, Solr, Hadoop, Spark, TinkerPop, and Cassandra are 
trademarks of the [Apache Software Foundation](http://www.apache.org/) or its subsidiaries in
Canada, the United States and/or other countries. 