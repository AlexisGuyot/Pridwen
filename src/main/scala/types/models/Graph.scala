package pridwen.types.models

import scala.annotation.implicitNotFound

import shapeless.{HList, Witness, ::, HNil}
import shapeless.ops.record.Selector
import shapeless.labelled.{FieldType}

import pridwen.types.support.DeepLabelledGeneric

/** Type associated with the Graph model.
*
* The Schemas of a Graph (SourceSchema, DestSchema, EdgeSchema) can be referenced through the Graph.Aux auxiliary type.
*/
trait Graph extends Model {
    type SourceLabel ; type DestLabel ; type EdgeLabel
    type SourceSchema <: HList ; type DestSchema <: HList ; type EdgeSchema <: HList
}

object Graph {
    // Names of mandatory attributes in the Graph's Schema
    type Source = Witness.`'source`.T ; type Destination = Witness.`'dest`.T ; type Edge = Witness.`'edge`.T

    type Aux[SourceS <: HList, DestS <: HList, EdgeS <: HList, LS, LD, LE] = Graph { 
        type Schema = FieldType[Source, SourceS] :: FieldType[Destination, DestS] :: FieldType[Edge, EdgeS] :: HNil
        type SourceSchema = SourceS ; type DestSchema = DestS ; type EdgeSchema = EdgeS
        type SourceLabel = LS ; type DestLabel = LD ; type EdgeLabel = LE
    }
    type All[SourceS <: HList, DestS <: HList, EdgeS <: HList, LS, LD, LE, S <: HList] = Graph { 
        type Schema = S 
        type SourceSchema = SourceS ; type DestSchema = DestS ; type EdgeSchema = EdgeS
        type SourceLabel = LS ; type DestLabel = LD ; type EdgeLabel = LE
    }
    type WithID[LS, LD, LE] = Graph { type SourceLabel = LS ; type DestLabel = LD ; type EdgeLabel = LE }

    /** Creates terms of type Graph (list of tuples).
    *
    * @param _data Tuples to encapsulate as a term of type Graph.
    * @param ls Name of the attribute containing the label of each source node.
    * @param ld Name of the attribute containing the label of each destination node.
    * @param le Name of the attribute containing the label of each edge.
    * @param isValid Can be implicitly created if the schema of the tuples conforms to the Graph model.
    */
    def apply[SourceS <: HList, DestS <: HList, EdgeS <: HList, LS, LD, LE](
        _data: List[FieldType[Source, SourceS] :: FieldType[Destination, DestS] :: FieldType[Edge, EdgeS] :: HNil], 
        ls: Witness.Aux[LS], 
        ld: Witness.Aux[LD],
        le: Witness.Aux[LE]
    )(
        implicit 
        isValid: ValidSchema[SourceS, LS, DestS, LD, EdgeS, LE]
    ) = common_apply[SourceS, LS, DestS, LD, EdgeS, LE](_data)

    /** Creates terms of type Graph (list of tuples expressed through case classes).
    *
    * @param _data Tuples to encapsulate as a term of type Graph.
    * @param ls Name of the attribute containing the label of each source node.
    * @param ld Name of the attribute containing the label of each destination node.
    * @param le Name of the attribute containing the label of each edge.
    * @param toHList Creates implicitly a function to convert a case class to a hlist.
    * @param eq Breaks down the HList into three distinct parts.
    * @param isValid Can be implicitly created if the schema of the tuples conforms to the Graph model.
    */
    def apply[S <: Product, HS <: HList, HS1 <: HList, HS2 <: HList, SourceS <: HList, DestS <: HList, EdgeS <: HList, LS, LD, LE](
        _data: List[S], 
        ls: Witness.Aux[LS], 
        ld: Witness.Aux[LD],
        le: Witness.Aux[LE]
    )(
        implicit 
        toHList: DeepLabelledGeneric.Aux[S, HS],
        eq: =:=[HS, FieldType[Source, SourceS] :: FieldType[Destination, DestS] :: FieldType[Edge, EdgeS] :: HNil],
        isValid: ValidSchema[SourceS, LS, DestS, LD, EdgeS, LE]
    ) = common_apply[SourceS, LS, DestS, LD, EdgeS, LE](_data.map(tuple => eq(toHList.to(tuple))))

    /** Creates terms of type Graph (list of tuples).
    * With ls and ld as type parameters.
    *
    * @param _data Tuples to encapsulate as a term of type Graph.
    * @param isValid Can be implicitly created if the schema of the tuples conforms to the Graph model.
    */
    def apply[SourceS <: HList, SourceLabel, DestS <: HList, DestLabel, EdgeS <: HList, EdgeLabel](
        _data: List[FieldType[Source, SourceS] :: FieldType[Destination, DestS] :: FieldType[Edge, EdgeS] :: HNil]
    )(
        implicit 
        isValid: ValidSchema[SourceS, SourceLabel, DestS, DestLabel, EdgeS, EdgeLabel]
    ) = common_apply[SourceS, SourceLabel, DestS, DestLabel, EdgeS, EdgeLabel](_data)

    /** Factorizes the common part of the bodies of the two apply functions.
    * The varying part is passed as a parameter (data to encapsulate).
    */
    private def common_apply[SourceS <: HList, LS, DestS <: HList, LD, EdgeS <: HList, LE](
        d: List[FieldType[Source, SourceS] :: FieldType[Destination, DestS] :: FieldType[Edge, EdgeS] :: HNil]
    ): Graph.Aux[SourceS, DestS, EdgeS, LS, LD, LE] = new Graph {
        type Schema = FieldType[Source, SourceS] :: FieldType[Destination, DestS] :: FieldType[Edge, EdgeS] :: HNil
        type SourceSchema = SourceS ; type DestSchema = DestS ; type EdgeSchema = EdgeS
        type SourceLabel = LS ; type DestLabel = LD ; type EdgeLabel = LE
        val data: List[Schema] = d
    }

    /** Type that checks that three schemas can be merged to form a new schema that conforms to the Graph model. 
    */
    @implicitNotFound("[Pridwen / ValidSchema] Schemas ${SourceSchema}, ${DestSchema} and ${EdgeSchema} cannot form a schema that conforms to the Graph model.")
    trait ValidSchema[SourceSchema, SourceLabel, DestSchema, DestLabel, EdgeSchema, EdgeLabel]
    object ValidSchema {
        /** Creates terms of type ValidSchema (empty objects).
        */
        private def inhabit_type[SourceSchema, SourceLabel, DestSchema, DestLabel, EdgeSchema, EdgeLabel]: ValidSchema[SourceSchema, SourceLabel, DestSchema, DestLabel, EdgeSchema, EdgeLabel]
            = new ValidSchema[SourceSchema, SourceLabel, DestSchema, DestLabel, EdgeSchema, EdgeLabel] {}

        /** Introduction rule that creates a term of type ValidSchema if the three schemas SourceS, DestS and EdgeS can be merged to form a new schema that conforms to the Graph model. 
        * @tparam SourceS The schema for source nodes.
        * @tparam LS Name of the attribute containing the label of each source node (as a singleton type).
        * @tparam DestS The schema for destination nodes.
        * @tparam LD Name of the attribute containing the label of each destination node (as a singleton type).
        * @tparam DestS The schema for edges.
        * @tparam LE Name of the attribute containing the label of each edge (as a singleton type).
        * @param lsExists Can be implicitly created if the attribute LS exists in SourceS.
        * @param ldExists Can be implicitly created if the attribute LD exists in DestS.
        * @param leExists Can be implicitly created if the attribute LE exists in EdgeS.
        * @param validSourceS Can be implicitly created if the schema for source nodes only contains attributes with supported types.
        * @param validDestS Can be implicitly created if the schema for destination nodes only contains attributes with supported types.
        * @param validEdgeS Can be implicitly created if the schema for edges only contains attributes with supported types.
        */
        implicit def valid_subschemas[SourceS <: HList, LS, DestS <: HList, LD, EdgeS <: HList, LE](
            implicit
            @implicitNotFound("[Pridwen / Graph.ValidSchema] Attribute ${LS} is not included in schema ${SourceS}.") lsExists: Selector[SourceS, LS],
            @implicitNotFound("[Pridwen / Graph.ValidSchema] Attribute ${LD} is not included in schema ${DestS}.") ldExists: Selector[DestS, LD],
            @implicitNotFound("[Pridwen / Graph.ValidSchema] Attribute ${LE} is not included in schema ${EdgeS}.") leExists: Selector[EdgeS, LE],
            @implicitNotFound("[Pridwen / Graph.ValidSchema] Source schema ${SourceS} does not conform to the Graph model.") validSourceS: ValidSubSchema[SourceS],
            @implicitNotFound("[Pridwen / Graph.ValidSchema] Dest schema ${DestS} does not conform to the Graph model.") validDestS: ValidSubSchema[DestS],
            @implicitNotFound("[Pridwen / Graph.ValidSchema] Edge schema ${EdgeS} does not conform to the Graph model.") validEdgeS: ValidSubSchema[EdgeS]
        ) = inhabit_type[SourceS, LS, DestS, LD, EdgeS, LE]

        @implicitNotFound("[Pridwen / ValidSubSchema] Subschema ${S} does not conform to the Graph model.") 
        trait ValidSubSchema[S <: HList]
        object ValidSubSchema {
            /** Creates terms of type ValidSchema (empty objects).
            */
            private def inhabit_type[S <: HList]: ValidSubSchema[S] = new ValidSubSchema[S] {}

            /** Similar as [[pridwen.types.models.Relation]]. */
            implicit def schema_with_multiple_fields[FName, FType, OtherF <: HList](
                implicit
                validFType: GType[FType],
                validOtherF: ValidSubSchema[OtherF]
            ) = inhabit_type[FieldType[FName, FType]::OtherF]
            implicit def schema_with_one_field[FName, FType](
                implicit 
                validFType: GType[FType]
            ) = inhabit_type[FieldType[FName, FType]::HNil]
        }

        /** Type that checks that a given attribute type (T) is supported by the Graph model.
        */
        @implicitNotFound("[Pridwen / GType] ${T} is not a supported type by the attributes of the Graph model.") 
        trait GType[T]
        object GType {
            /** Creates terms of type GType (empty objects).
            */
            private def inhabit_GType[T]: GType[T] = new GType[T] {}

            // Introduction rules (one by supported attribute type).
            implicit def graph_string = inhabit_GType[String]
            implicit def graph_int = inhabit_GType[Int]
            implicit def graph_double = inhabit_GType[Double]
            implicit def graph_long = inhabit_GType[Long]
            implicit def graph_bool = inhabit_GType[Boolean]
            implicit def graph_multi[T : GType] = inhabit_GType[List[T]]
        }
    }
}