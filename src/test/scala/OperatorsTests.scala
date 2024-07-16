import org.scalatest.flatspec._
import org.scalatest.matchers.should.Matchers._

import shapeless.{Witness, HNil, ::}
import shapeless.labelled.{FieldType, field}

import pridwen.types.models._ 
import pridwen.operators._

class OperatorsTests extends AnyFlatSpec {
    val name = Witness('name) ; val names = Witness('names) ; val age = Witness('age) ; val label = Witness('label) 
    type SchemaRel = FieldType[name.T, String] :: FieldType[age.T, Int] :: HNil
    val aRelation = Relation(List(
        field[name.T]("alexis") :: field[age.T](25) :: HNil,
        field[name.T]("pierre") :: field[age.T](42) :: HNil,
        field[name.T]("marie") :: field[age.T](22) :: HNil,
        field[name.T]("lola") :: field[age.T](16) :: HNil
    ))
    type SchemaJSON = FieldType[names.T, List[String]] :: FieldType[age.T, Int] :: HNil
    val aJSON = JSON(List(
        field[names.T](List("alexis", "robert", "michel")) :: field[age.T](25) :: HNil,
        field[names.T](List("pierre", "paul")) :: field[age.T](42) :: HNil,
        field[names.T](List("marie", "christine", "geraldine")) :: field[age.T](22) :: HNil,
        field[names.T](List("lola")) :: field[age.T](16) :: HNil
    ))
    type SchemaGraph = FieldType[Graph.Source, FieldType[name.T, String] :: HNil] :: FieldType[Graph.Destination, FieldType[name.T, String] :: HNil] :: FieldType[Graph.Edge, FieldType[label.T, String] :: HNil] :: HNil
    val aGraph = Graph(List(
        field[Graph.Source](field[name.T]("alexis") :: HNil) :: field[Graph.Destination](field[name.T]("pierre") :: HNil) :: field[Graph.Edge](field[label.T]("knows") :: HNil) :: HNil,
        field[Graph.Source](field[name.T]("alexis") :: HNil) :: field[Graph.Destination](field[name.T]("marie") :: HNil) :: field[Graph.Edge](field[label.T]("knows") :: HNil) :: HNil,
        field[Graph.Source](field[name.T]("alexis") :: HNil) :: field[Graph.Destination](field[name.T]("lola") :: HNil) :: field[Graph.Edge](field[label.T]("knows") :: HNil) :: HNil,
        field[Graph.Source](field[name.T]("lola") :: HNil) :: field[Graph.Destination](field[name.T]("marie") :: HNil) :: field[Graph.Edge](field[label.T]("knows") :: HNil) :: HNil,
        field[Graph.Source](field[name.T]("lola") :: HNil) :: field[Graph.Destination](field[name.T]("alexis") :: HNil) :: field[Graph.Edge](field[label.T]("knows") :: HNil) :: HNil,
        field[Graph.Source](field[name.T]("marie") :: HNil) :: field[Graph.Destination](field[name.T]("alexis") :: HNil) :: field[Graph.Edge](field[label.T]("knows") :: HNil) :: HNil,
    ), name, name, label)

    /* ------ Tests with Construct ------*/

    "Construct" should "compile if the relation is transformed into a JSON" in {
        val newJSON1: JSON.Aux[SchemaRel] = construct.from(aRelation).a[JSON].apply
        val newJSON2: JSON.Aux[FieldType[name.T, String] :: HNil] = construct.from(aRelation).a[JSON].withAttributes[name.T :: HNil].apply
    }

    it should "compile if the graph is transformed into a JSON" in {
        val newJSON1: JSON.Aux[SchemaGraph] = construct.from(aGraph).a[JSON].apply
        val newJSON2: JSON.Aux[FieldType[Graph.Source, FieldType[name.T, String] :: HNil] :: HNil] = construct.from(aGraph).a[JSON].withAttributes[Graph.Source :: HNil].apply
    }

    it should "not compile if the relation is transformed into a graph" in {

    }

    it should "not compile if the JSON is transformed into a relation unless multi-valued and nested fields are removed" in {
        "val newRel1 = construct.from(aJSON).a[Relation].apply" shouldNot typeCheck
        val newRel2 = construct.from(aJSON).a[Relation].withAttributes[age.T :: HNil].apply
    }

    it should "not compile if the JSON is transformed into a graph" in {

    }

    it should "not compile if the graph is transformed into a relation unless multi-valued and nested fields are removed" in {
        "val newRel1 = construct.from(aGraph).a[Relation].apply" shouldNot typeCheck
        val newRel2 = construct.from(aGraph).a[Relation].withAttributes[(Graph.Source :: name.T :: HNil) :: (Graph.Destination :: name.T :: HNil) :: HNil].apply
    }
}