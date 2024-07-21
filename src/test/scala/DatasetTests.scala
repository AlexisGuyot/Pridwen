import pridwen.dataset._
import pridwen.dataset.functions._
import pridwen.types.models._

import org.apache.spark.sql.SparkSession

import shapeless.{::, HNil}

import scala.reflect.runtime.universe._
import org.scalatest.matchers.should.Matchers._
import org.scalatest.flatspec._

case class Base(att1: String, att2: Int, att3: Boolean)
case class Multivalued(att1: String, att2: Int, att3: Boolean, att4: List[Double])
case class Nested(att4: String, att5: Int, att6: Boolean, att7: Base)

class DatasetTests extends AnyFlatSpec {
    // =========== Starting with Spark Datasets

    val spark = SparkSession.builder.master("local").appName("PridwenDataset").getOrCreate

    import spark.implicits._

    val dataset = Seq(Base("A", 1, true), Base("B", 2, false), Base("C", 3, true)).toDS
    val datasetM = Seq(Multivalued("A", 1, true, List(1.1, 1.2, 1.3)), Multivalued("B", 2, false, List(2.1, 2.2)), Multivalued("C", 3, true, List())).toDS
    val datasetN = Seq(Nested("A", 1, true, Base("B", 2, false)), Nested("C", 3, true, Base("D", 4, false)), Nested("E", 5, true, Base("E", 6, false))).toDS

    import pridwen.dataset.implicits._

    "Data" should "provide the same main functionalities as Spark's dataset while providing a better type safety" in {
        // =========== Turning into Pridwen Datasets
        
        // Three different syntaxes for turning Spark datasets into Pridwen datasets. Available models = Relation, JSON, Graph (property, as list of edges)

        val data = dataset.asModel[Relation]
        //val data = Data(dataset).as[Relation]
        //val data = Data[Relation](dataset)

        val dataM = datasetM.asModel[JSON]
        val dataN = datasetN.asModel[JSON]

        // Function "describe" prints information about the dataset. The first argument (optional) gives a name to the dataset, the second one (optional) specifies if the underlying dataframe should be printed (show).

        data.describe("MyRelation", true)
        dataM.describe("MyMultiJSON", true)
        dataN.describe("MyNestedJSON", true)

        // =========== Turning back to Spark Datasets

        val sparkDS = data.toDS
        //val sparkDS = data.toDS.withSchema[Base]

        // =========== Transforming Pridwen Datasets

        // -------- Model change (if possible)

        data.as[JSON].describe(true)
        "dataM.as[Relation].describe(true)" shouldNot typeCheck
        /*data.as[Graph].describe(true)*/   // Not possible, the schema of data does not conform to this model

        // -------- Attribute(s) projection

        // Simple projection
        data.select(col('att1)).describe(true)

        // Projecting a nested attribute
        dataN.select(col('att7) -> col('att2)).describe(true)

        // Projecting multiple attributes
        dataN.select(col('att4) && col('att7) -> col('att2)).describe(true)

        // Projecting with an alias
        data.select(col('att1).as('test)).describe(true)
        dataN.select(col('att4) && (col('att7) -> col('att1)).as('test)).describe(true)

        // ******** Safety brought by typing

        // Impossible to project an attribute that does not exist in the data schema
        "data.select(col('fail)).describe(true)" shouldNot typeCheck

        // -------- Row(s) selection 

        // By comparing the values of some attributes
        dataN.filter(col('att4) === col('att7) -> col('att1)).describe(true)
        dataN.select(col('att4) && col('att7) -> col('att1)).filter(col('att4) === col('att1)).describe(true)

        // By comparing the values of an attribute with a constant value
        dataN.filter(col('att7) -> col('att1) === v("D")).describe(true)
        dataN.select(col('att4) && col('att7) -> col('att1)).filter(col('att4) === v("A")).describe(true)
        data.filter(col('att2) =!= v(2)).describe(true)
        data.filter(col('att2) > v(2)).describe(true)
        data.filter(col('att2) >= v(2)).describe(true)
        data.filter(col('att2) < v(2)).describe(true)
        data.filter(col('att2) <= v(2)).describe(true)
        data.filter(col('att3).isNull).describe(true)
        data.filter(col('att3).isNotNull).describe(true)

        // Multiple conditions
        dataN.filter(col('att7) -> col('att1) === v("D") || col('att4) === col('att7) -> col('att1)).describe(true)

        // Comparison based on a UDF
        data.filter(col('att1) && col('att2), (x: String, y: Int) => x == "A" || y == 2).describe(true)     // Applied on some attributes
        data.filter((x: String, y: Int, z: Boolean) => x == "A" || y == 2 || z).describe(true)              // Applied on all attributes

        // ******** Safety brought by typing

        // Impossible to compare two columns that have different types
        "dataN.filter(col('att4) === col('att7) -> col('att2)).describe(true)" shouldNot typeCheck

        // Impossible to compare a column with a constant value that does not have the right type
        "dataN.filter(col('att7) -> col('att1) === v(1)).describe(true)" shouldNot typeCheck

        // Impossible to apply a UDF with a wrong signature
        "data.filter(col('att1) && col('att2), (x: String, y: Int) => 0).describe(true)" shouldNot typeCheck
        "data.filter(col('att1) && col('att2), (x: String, y: String) => true).describe(true)" shouldNot typeCheck
        "data.filter(col('att1) && col('att2), (x: String, y: Int, z: Boolean) => y == 2).describe(true)" shouldNot typeCheck

        "dataN.filter(col('fail) === col('att7) -> col('att1)).describe(true)" shouldNot typeCheck

        // -------- Adding a new attribute

        // New attribute with constant values
        data.add(col('test), 0).keepModel.describe(true)
        dataN.add(col('att7) -> col('test), 0).keepModel.describe(true)

        data.add(col('test), List[Int](0)).changeModel[JSON].describe(true)     // test can be added with a model change
        
        // New attribute whose values are the result of applying some predefined operations on the values of existing attribute(s). Supported operations: +, -, *, %, /
        data.add(col('test), col('att2) + v(10)).keepModel.describe(true)
        data.add(col('test), col('att2) + col('att2)).keepModel.describe(true)
        dataN.add(col('test), col('att5) + (col('att7) -> col('att2))).keepModel.describe(true)

        // New attribute whose values are created by mapping values of another attribute with a UDF
        data.add(col('test), col('att2), (x: Int) => x + 1).keepModel.describe(true)
        dataN.add(col('test), col('att7) -> col('att2), (x: Int) => x + 1).keepModel.describe(true)
        dataN.add(col('att7) -> col('test), col('att7) -> col('att2), (x: Int) => x + 1).keepModel.describe(true)

        // New attribute whose values are created by mapping values of multiple other attributes with a UDF
        data.add(col('test), col('att1) && col('att2), (x: String, y: Int) => s"$x$y").keepModel.describe(true)
        data.add(col('test), "*", (x: String, y: Int, z: Boolean) => 0).keepModel.describe(true)

        // ******** Safety brought by typing

        // Impossible to add an attribute not supported by the model of the dataset
        "data.add(col('test), List[Int]()).keepModel.describe(true)" shouldNot typeCheck

        // Impossible to apply predefined operations on not numeric attributes
        "data.add(col('test), col('att1) + v(10)).keepModel.describe(true)" shouldNot typeCheck
        "data.add(col('test), col('att1) + col('att2)).keepModel.describe(true)" shouldNot typeCheck

        // Impossible to use an attribute that does not exist, impossible to apply operations on attributes/values with different types, impossible to apply a UDF with a wrong signature (see select/filter)
        "data.add(col('test), col('fail) + v(10)).keepModel.describe(true)" shouldNot typeCheck
        """data.add(col('test), col('att1) && col('att2), (x: String, y: String) => s"$x$y").keepModel.describe(true)""" shouldNot typeCheck

        // -------- Deleting an attribute

        data.drop(col('att1)).describe(true)
        /*dataN.drop(col('att7) -> col('att1)).describe(true)*/ // Not working well for now with nested attributes, WIP.

        // ******** Safety brought by typing

        // Impossible to drop an attribute that does not exist in the data schema
        "data.drop(col('fail)).describe(true)" shouldNot typeCheck

        // -------- Attribute update 

        // Renaming an attribute. Also not working well with nested attributes for now (WIP).
        data.withColumnRenamed('att1, 'test).describe(true)

        // Updating the values of an attribute with and without changing its name.
        data.update('att1, 'test, (x: String) => 0).keepModel.describe(true)
        data.update('att1, (x: String) => 0).keepModel.describe(true)

        // ******** Safety brought by typing

        // Impossible to update/rename an attribute that does not exist, impossible to apply a UDF with a wrong signature (see select/filter/add/drop).
        "data.update('fail, 'test, (x: String) => 0).keepModel.describe(true)" shouldNot compile
        "data.update('att1, 'test, (x: Int) => 0).keepModel.describe(true)" shouldNot compile

        // -------- Sorting a dataset

        data.orderBy(col('att1).desc).describe(true)
        data.orderBy(col('att1).desc && col('att2).asc).describe(true)

        // ******** Safety brought by typing

        // Impossible to use an attribute that does not exist to order the dataset
        "data.orderBy(col('fail).desc).describe(true)" shouldNot compile

        // -------- Joins between datasets

        val data2 = data.drop(col('att3)).add(col('test), col('att2) % v(2)).keepModel.drop(col('att2)).withColumnRenamed('att1, 'truc)
        data2.describe(true)    // Relation

        // Inner join with one or multiple conditions (left operand = in data / left dataset, right operand = in data2 / right dataset)
        data.join(data2, col('att1) === col('truc)).keepLeftModel.describe(true)
        data.join(data2, col('att1) === col('truc) && col('att2) > col('test)).keepLeftModel.describe(true)
        data.join(data2, col('att1) === col('truc) && col('att2) > col('test)).keepRightModel.describe(true)

        data.join(dataM, col('att1) === col('att1)).keepRightModel.describe(true)
        data.join(dataM, col('att1) === col('att1)).changeModel[JSON].describe(true)

        // Other join modes supported: inner, cross, outer, full, fullouter, full_outer, left, leftouter, left_outer, right, rightouter, right_outer (all aside semi and anti joins -> for now)
        data.join(data2, col('att1) === col('truc), "inner").keepLeftModel.describe(true)
        data.join(data2, col('att1) === col('truc), "left").keepLeftModel.describe(true)

        // ******** Safety brought by typing

        // Impossible to request a not supported join mode.
        """data.join(data2, col('att1) === col('truc), "fail").keepLeftModel.describe(true)""" shouldNot typeCheck

        // Impossible to use attributes that does not exist in the data schema, impossible to create a new dataset with a schema that does not conform to the requested model (see filter/add/update)
        "data.join(data2, col('fail) === col('truc)).keepLeftModel.describe(true)" shouldNot typeCheck
        "data.join(dataM, col('att1) === col('att1)).keepLeftModel.describe(true)" shouldNot typeCheck

        // -------- Aggregation of a dataset

        // Predefined aggregate operations: count, max, min, avg, median, sum, product
        data.groupBy(col('att3))
            .agg(col('att3).count && col('att2).avg && col('att2).max)
            .keepModel.describe(true)

        // ******** Safety brought by typing

        // Impossible to use the avg, sum and product predefined operations on non numeric attributes
        "data.groupBy(col('att3)).agg(col('att1).avg).keepModel.describe(true)" shouldNot typeCheck

        // Impossible to use attributes that does not exist in the data schema, impossible to create a new dataset with a schema that does not conform to the requested model (see add/update/join)
        "data.groupBy(col('fail)).agg(col('fail).count).keepModel.describe(true)" shouldNot typeCheck

        spark.close
    }
}