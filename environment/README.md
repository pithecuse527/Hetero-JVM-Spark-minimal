# Runtime environment

The study used Spark 4.1.2, Scala 2.13.17, and JDK 21 on linux/amd64.

- `Dockerfile.hotspot` extends the official `apache/spark:4.1.2` image with
  the required S3A dependencies.
- `Dockerfile.semeru-milestone` independently starts from the same official
  Spark image and replaces its Java runtime with Semeru/OpenJ9.

The OpenJ9 image does not depend on a private or locally prebuilt HotSpot image.
After building it, pass its deployment-specific tag through `OPENJ9_IMAGE`.
Before a release or rerun, record immutable base and output image digests;
mutable tags alone are insufficient experiment provenance.

Cluster manifests are intentionally omitted from this minimal bundle because
the original manifests contain deployment-specific node, storage, ingress,
and service details. A deployment must provide homogeneous G1 and OpenJ9
pools, identical Spark resources and data, fresh application JVMs, and a
persistent writable SCC path for the OpenJ9 treatment.
