# Pridwen: towards robust analytical workflows for Big Data

## Project goal

Big data analysis requires the construction of complex hybrid workflows in which data are transformed several times at different levels of abstraction (data, schema, model). These transformations are complex because there are many links to consider between all the levels of abstraction, especially between the schema and the model, since the latter can impose restricted sets of supported attribute types, mandatory attributes, etc. 

The main goal of the Pridwen library is to provide types allowing for specifying the inputs and outputs of workflow operators at different levels of abstraction, so that bad transformations (*e.g.* adding a multivalued attribute in a relation) or bad operator compositions (*e.g.* composition of an operator returning a JSON with an operator requiring a Graph) in workflows can be detected at compile time rather than being raised at runtime after time-consuming and expensive processing.

The library also provides types to infer the schema of an operator output based on the schemas of its inputs and the kind of schema-level transformation the operator perform. 

The long-term plan is also to provide various operators for transforming and analysing big data based on the different types proposed in order to simplify the creation of robust analytical workflows for Big Data.

From a technical point of view, the library is mainly relying on Scala implicits and the Shapeless library.

### Example

Coming soon. In the meantime, you can find several examples of use in the src/eval and src/test folders.


### Additional resources
- Guyot, A., Leclercq, É., Gillet, A., & Cullot, N. (2023, August). Preventing Technical Errors in Data Lake Analyses with Type Theory. In International Conference on Big Data Analytics and Knowledge Discovery (pp. 18-24). Cham: Springer Nature Switzerland. Link: https://hal.science/hal-04452461/document
- https://github.com/milessabin/shapeless

## Project description

Description of the content of the directories included in the src folder.

### Main

Sources of the library. Contains two packages pridwen.types and pridwen.operators.

The first package includes the new types proposed to model data as relations, JSON or property graphs (pridwen.types.models) and to infer new schemas based on existing ones and various kind of schema-level transformations (pridwen.types.opschema).

The second package includes various operators implemented with Pridwen's types to transform and analyse data. These are also provided as examples to show how to use the library to implement new operators. 

Almost everything is documented to make the project easier to understand directly from the source code. 

### Test

Some unit tests to validate the proposed types and operators. Can also be studied to understand how the library works.

### Eval

Sub-project used to validate and evaluate the library in the context of my phd thesis. Includes different versions of an analytical workflow measuring the polarisation of an online social network based on some graphs of interactions. Can also be studied to better understand how the library works and how to use it.

Here too, the code is documented to make this part of the project easier to understand directly from the source code. 

### Dataset

Extension of Spark datasets based on the different new types provided by the Pridwen library. See docs/PridwenDatasets<VF/VE>.pdf for more information.

## How to use the project

Simply clone the git repository and use sbt to compile/package the project. Or download the provided release and add it to your project.

See the src/eval/src/* folder for further information on how to use the project.