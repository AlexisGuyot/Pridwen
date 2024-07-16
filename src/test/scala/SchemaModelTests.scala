import java.time.{LocalDate => Date}

import org.scalatest.flatspec._
import org.scalatest.matchers.should.Matchers._

import shapeless.{Witness, ::, HNil, LabelledGeneric}
import shapeless.labelled.{field, FieldType}

import pridwen.types.models._
import pridwen.types.support._

class SchemaModelTests extends AnyFlatSpec {
    // Attribute names
    val aString = Witness('aString) ; val aInt = Witness('aInt) ; val aDouble = Witness('aDouble)
    val aLong = Witness('aLong) ; val aBool = Witness('aBool) ; val aDate = Witness('aDate)
    val aFloat = Witness('aFloat) ; val aList = Witness('aList) ; val aNested = Witness('aNested)

    /* ------------ Tests ------------*/

    /* ------ Tests with Relation ------*/

    "A Relation" should "support String, Integer, Double, Long, Bool and Date attribute types" in {
        Relation(List(field[aString.T]("test") :: field[aInt.T](52) :: field[aDouble.T](52.5) :: field[aLong.T](52) :: field[aBool.T](true) :: field[aDate.T](Date.now) :: HNil))
    }

    it should "not support other scalar attribute types" in {
        "Relation(List(field[aFloat.T](1.5f) :: HNil))" shouldNot typeCheck
    }

    it should "not support multi-valued attribute types" in {
        "Relation(List(field[aList.T](List(0)) :: HNil))" shouldNot typeCheck
    }

    it should "not support nested attribute types" in {
        """Relation(List(field[aNested.T](field[aString.T]("test") :: field[aInt.T](52) :: field[aDouble.T](52.5) :: field[aLong.T](52) :: field[aBool.T](true) :: field[aDate.T](Date.now) :: HNil) :: HNil))""" shouldNot typeCheck
    }

    it should "not have an empty schema" in {
        "Relation(List(HNil))" shouldNot typeCheck
    }

    it should "be able to be created from a case class" in {
        case class MySchema(aString: String, aInt: Int, aDouble: Double, aLong: Long, aBool: Boolean, aDate: Date)
        Relation(List(MySchema("test", 52, 52.5, 52, true, Date.now)))
    }

    /* ------ Tests with JSON ------*/

    "A JSON" should "support String, Integer, Double, Long and Bool scalar attribute types" in {
        JSON(List(field[aString.T]("test") :: field[aInt.T](52) :: field[aDouble.T](52.5) :: field[aLong.T](52) :: field[aBool.T](true) :: HNil))
    }

    it should "not support other scalar attribute types" in {
        "JSON(List(field[aDate.T](Date.now) :: HNil))" shouldNot typeCheck
    }

    it should "support multi-valued attribute types" in {
        JSON(List(field[aList.T](List(0)) :: HNil))
    }

    it should "support nested attribute types" in {
        JSON(List(field[aNested.T](field[aString.T]("test") :: field[aInt.T](52) :: field[aDouble.T](52.5) :: field[aLong.T](52) :: field[aBool.T](true) :: HNil) :: HNil))
    }

    it should "be able to have an empty schema" in {
        implicitly[JSON.ValidSchema[HNil]]
    }

    it should "be able to be created from a case class" in {
        case class MySchema(aString: String, aInt: Int, aDouble: Double, aLong: Long, aBool: List[Boolean])
        case class MyNestedSchema(aNested: MySchema)
        JSON(List(MySchema("test", 52, 52.5, 52, List(true, false))))
        JSON(List(MyNestedSchema(MySchema("test", 52, 52.5, 52, List(true, false)))))
    }

    /* ------ Tests with Graph ------*/

    "A Graph" should "support String, Integer, Double, Long and Bool scalar attribute types" in {
        Graph(
            List(field[Graph.Source](field[aString.T]("test") :: field[aInt.T](52) :: HNil) :: field[Graph.Destination](field[aDouble.T](52.5) :: field[aLong.T](52) :: HNil) :: field[Graph.Edge](field[aBool.T](true) :: HNil) :: HNil),
            aString,
            aDouble,
            aBool
        )
    }

    it should "not support other scalar attribute types" in {
        """Graph(
            List(field[Graph.Source](field[aString.T]("test") :: field[aInt.T](52) :: HNil) :: field[Graph.Destination](field[aDouble.T](52.5) :: field[aLong.T](52) :: HNil) :: field[Graph.Edge](field[aBool.T](true) :: field[aDate.T](Date.now) :: HNil) :: HNil),
            aString,
            aDouble,
            aBool
        )""" shouldNot typeCheck
    }

    it should "support multi-valued attribute types" in {
        Graph(
            List(field[Graph.Source](field[aString.T](List("test1", "test2")) :: field[aInt.T](52) :: HNil) :: field[Graph.Destination](field[aDouble.T](52.5) :: field[aLong.T](52) :: HNil) :: field[Graph.Edge](field[aBool.T](true) :: HNil) :: HNil),
            aString,
            aDouble,
            aBool
        )
    }

    it should "not support any nested attributes other than those imposed by the model" in {
        """Graph(
            List(field[Graph.Source](field[aNested.T](field[aString.T]("test") :: HNil) :: field[aInt.T](52) :: HNil) :: field[Graph.Destination](field[aDouble.T](52.5) :: field[aLong.T](52) :: HNil) :: field[Graph.Edge](field[aBool.T](true) :: HNil) :: HNil),
            aInt,
            aDouble,
            aBool
        )""" shouldNot typeCheck
    }

    it should "not be able to have any empty subschema" in {         
        implicitly[Graph.ValidSchema[FieldType[aString.T, String] :: HNil, aString.T, FieldType[aString.T, String] :: HNil, aString.T, FieldType[aString.T, String] :: HNil, aString.T]]
        "implicitly[Graph.ValidSchema[HNil, aString.T, FieldType[aString.T, String] :: HNil, aString.T, FieldType[aString.T, String] :: HNil, aString.T]]" shouldNot typeCheck
        "implicitly[Graph.ValidSchema[FieldType[aString.T, String] :: HNil, aString.T, HNil, aString.T, FieldType[aString.T, String] :: HNil, aString.T]]" shouldNot typeCheck
        "implicitly[Graph.ValidSchema[FieldType[aString.T, String] :: HNil, aString.T, FieldType[aString.T, String] :: HNil, aString.T, HNil, aString.T]]" shouldNot typeCheck
    }

    it should "be able to be created from a case class" in {
        case class Node(id: Int) ; case class EdgeAtt(weight: Int)
        case class Edge(source: Node, dest: Node, edge: EdgeAtt)

        Graph(List(Edge(Node(0), Node(1), EdgeAtt(5))), Witness('id), Witness('id), Witness('weight))
    }

    it should "not compile if one or both nodes ID do not exist" in {
        case class Node(id: Int) ; case class EdgeAtt(weight: Int)
        case class Edge(source: Node, dest: Node, edge: EdgeAtt)

        "Graph(List(Edge(Node(0), Node(1), EdgeAtt(5))), Witness('dummy), Witness('id), Witness('weight))" shouldNot typeCheck
        "Graph(List(Edge(Node(0), Node(1), EdgeAtt(5))), Witness('id), Witness('dummy), Witness('weight))" shouldNot typeCheck
        "Graph(List(Edge(Node(0), Node(1), EdgeAtt(5))), Witness('dummy), Witness('dummy), Witness('weight))" shouldNot typeCheck
    }

    /* ------ Tests with Model.Aux ------*/

    "Model.As" should "compute a new relation if the input schema conforms to this model" in {
        implicitly[Model.As[FieldType[aString.T, String] :: HNil, Relation]]
    }

    it should "not compute a new relation if the input schema conforms to this model" in {
        "implicitly[Model.As[FieldType[aFloat.T, Float] :: HNil, Relation]]" shouldNot typeCheck
    }

    it should "compute a new JSON if the input schema conforms to this model" in {
        implicitly[Model.As[FieldType[Witness.`'aNested`.T, 
                    FieldType[Witness.`'aDouble`.T, Double] :: FieldType[Witness.`'aNested`.T, 
                        FieldType[Witness.`'aString`.T, String] :: FieldType[Witness.`'aInt`.T, Int] :: HNil
                    ] :: HNil
                ] :: FieldType[Witness.`'aLong`.T, Long] :: HNil, JSON]]
    }

    it should "compute a new Graph if the input schema conforms to this model" in {
        implicitly[Model.As[
            FieldType[Graph.Source, FieldType[aString.T, String] :: HNil] :: 
            FieldType[Graph.Destination, FieldType[aString.T, String] :: HNil] :: 
            FieldType[Graph.Edge, FieldType[aString.T, String] :: HNil] :: HNil, 
            Graph.WithID[aString.T, aString.T, aString.T]
        ]]
    }

    it should "not compute a new Graph if the input schema conforms to this model" in {
        """implicitly[Model.As[
            FieldType[Graph.Source, FieldType[aString.T, String] :: HNil] :: 
            FieldType[Graph.Destination, FieldType[aString.T, String] :: HNil] :: 
            FieldType[Graph.Edge, FieldType[aString.T, String] :: HNil] :: HNil, 
            Graph.WithID[aInt.T, aString.T, aString.T]
        ]]""" shouldNot typeCheck

        """implicitly[Model.As[
            FieldType[Graph.Source, FieldType[aString.T, String] :: HNil] :: 
            FieldType[Graph.Destination, FieldType[aString.T, String] :: HNil] :: 
            FieldType[Graph.Edge, FieldType[aString.T, String] :: HNil] :: HNil, 
            Graph
        ]]""" shouldNot typeCheck
    }

    /* ------ Tests with DeepLabelledGeneric ------*/

    "DeepLabelledGeneric" should "be equivalent to Shapeless LabelledGeneric if there is no nested HList" in {
        case class MySchema(aString: String, aInt: Int, aDouble: Double, aLong: Long, aBool: List[Boolean])
        val lg = LabelledGeneric[MySchema]
        val dlg = DeepLabelledGeneric[MySchema]
        implicitly[=:=[lg.Repr, dlg.Repr]]
    }

    it should "handle nested HList" in {
        case class MySchema(aString: String, aInt: Int, aDouble: Double, aLong: Long, aBool: List[Boolean])
        case class MyNestedSchema(aNested: MySchema)
        val dlg = DeepLabelledGeneric[MyNestedSchema]
        implicitly[=:=[
            dlg.Repr, FieldType[Witness.`'aNested`.T, 
                        FieldType[Witness.`'aString`.T, String] :: 
                        FieldType[Witness.`'aInt`.T, Int] :: 
                        FieldType[Witness.`'aDouble`.T, Double] :: 
                        FieldType[Witness.`'aLong`.T, Long] :: 
                        FieldType[Witness.`'aBool`.T, List[Boolean]] :: 
                        HNil
                    ] :: HNil
        ]]
    }

    it should "handle deeply nested HList" in {
        case class MySchema(aString: String, aInt: Int)
        case class MyNestedSchema(aDouble: Double, aNested: MySchema)
        case class MyDeeplyNestedSchema(aNested: MyNestedSchema, aLong: Long)
        val dlg = DeepLabelledGeneric[MyDeeplyNestedSchema]
        implicitly[=:=[dlg.Repr, 
            FieldType[Witness.`'aNested`.T, 
                FieldType[Witness.`'aDouble`.T, Double] :: FieldType[Witness.`'aNested`.T, 
                    FieldType[Witness.`'aString`.T, String] :: FieldType[Witness.`'aInt`.T, Int] :: HNil
                ] :: HNil
            ] :: FieldType[Witness.`'aLong`.T, Long] :: HNil
        ]]
    }

    it should "handle multiple nested HList" in {
        case class MySchema1(aString: String)
        case class MySchema2(aInt: Int)
        case class MyNestedSchema(aNested1: MySchema1, aNested2: MySchema2)
        val dlg = DeepLabelledGeneric[MyNestedSchema]
        implicitly[=:=[dlg.Repr, 
            FieldType[Witness.`'aNested1`.T, 
                FieldType[Witness.`'aString`.T, String] :: HNil
            ] :: FieldType[Witness.`'aNested2`.T, 
                FieldType[Witness.`'aInt`.T, Int] :: HNil
            ] :: HNil
        ]]
    }
}