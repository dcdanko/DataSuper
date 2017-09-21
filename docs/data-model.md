# DataSuper - The Data Model

DataSuper puts files into groups that make it easier to keep track of complicated results and large projects.

To keep everything organized DataSuper defines an abstract data model. There are four different categories in this data model:
 - Sample Groups
 - Samples
 - Results
 - Files

## File Objects

Files are the simplest to understand. Each file object represents one file on disk. Files objects don't know or care what the other layers are doing. Their only responsibility is to keep track of their file.
 - File objects keep track of a file path. This path is stored relative to the repo and must be unique.
 - File objects keep track of a file type.
 - File objects keep track of a checksum to validate the stored file

## Result Objects

At their core, result objects are groups of files but result objects define more complex relationships
 - Result objects have a 'type' that inidcates what schema that should use
 - Result objects have a 'schema' that indicates what files they should store (more on this later)
 - Result objects can be derived from other result objects. This is a one way relationship, results can keep track of the results they were derived from but results do not know what other results were derived from them
 - Result objects have a name. The pair of parameters (name, result-type) must define a unique result
 - Result objects keep track of their provenance. This is optional but makes it possible to record what commands were run to produce a result

## Sample Objects

Samples are used to collect related groups of results. Sample objects store metadata. Samples are meant to correspond to actual samples in a wet-lab experiment, if different datatypes are collected or different analyses are run on the sample that can be represented using results.
 - Samples have a 'type' that can be used to aggregate similar samples
 - Samples have a name which must be unique
 - Samples include results, this is a one way relationship
 - Samples have a metadata field that can store arbitrary JSON-able data
 
## Sample Group Objects

Sample groups collect groups of samples and results. Groups are used to define projects and subprojects. Groups can store results that aggregate every sample in the group (such as an intersample distance matrix)
 - Sample groups have a name which must be unique
 - Sample groups can contain other subgroups, this is a one way relationship
 - Sample groups can contain samples. There is a distinction between 'all-samples' which refers to every sample in the group and in the groups subgroups and 'direct-samples' that are contained directly on the group. This is a one way relationship.
 - Sample groups can contain results that apply to all the samples in the group. This is a one way relationship.
