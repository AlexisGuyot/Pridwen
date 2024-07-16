package pridwen.types.models

import scala.annotation.implicitNotFound

import shapeless.{HList, HNil, ::}
import shapeless.labelled.FieldType

/** Super-type for all models.
* 
* May be associated with a Schema type (HList).
* Terms of concrete models are lists of Schema terms (tuples).
*/
trait Model {
    type Schema <: HList
    val data: List[Schema]
}

object Model {
    /** Auxiliary type allowing to modelize data with a certain schema (S) into a specific model (M).
    * 
    * Intern type Out is the result of the modelling and can be referenced through the Aux type of As.
    * Terms of this type are functions modelling a list of terms of type S (tuples) into a specific model.
    */
    @implicitNotFound("[Pridwen / Model.As] Schema ${S} does not conform to the ${M} model.") 
    trait As[S <: HList, M <: Model] { type Out <: Model ; def apply(data: List[S]): Out }
    object As {
        type Aux[S <: HList, M <: Model, Out0 <: Model] = As[S, M] { type Out = Out0 }

        /** Creates terms of type Model.As (functions).
        *
        * @param f Function encapsulated as a term of type Model.As.
        */
        private def inhabit_type[S <: HList, M <: Model, Out0 <: Model](
            f: List[S] => Out0
        ): Aux[S, M, Out0] = new As[S, M] {
            type Out = Out0
            def apply(data: List[S]): Out = f(data)
        }

        /** Introduction rule that creates a term of type Model.As if the schema type parameter S of inhabit_type conforms to the relational model.
        * The term is a function that creates a term of type Relation from data with schema S.
        * @tparam S Schema (as a type) of the data to be modelled as a relation. 
        * @param isValid Can be implicitly created if the schema S conforms to the relational model.
        */
        implicit def as_relation[S <: HList](
            implicit
            @implicitNotFound("[Pridwen / Model.As] Schema ${S} does not conform to the relational model.") isValid: Relation.ValidSchema[S]
        ) = inhabit_type[S, Relation, Relation.Aux[S]](
            (d: List[S]) => Relation(d)
        )

        /** Introduction rule that creates a term of type Model.As if the schema type parameter S of inhabit_type conforms to the JSON model.
        * The term is a function that creates a term of type JSON from data with schema S.
        * @tparam S Schema (as a type) of the data to be modelled as a JSON. 
        * @param isValid Can be implicitly created if the schema S conforms to the JSON model.
        */
        implicit def as_json[S <: HList](
            implicit
            @implicitNotFound("[Pridwen / Model.As] Schema ${S} does not conform to the JSON model.") isValid: JSON.ValidSchema[S]
        ) = inhabit_type[S, JSON, JSON.Aux[S]](
            (d: List[S]) => JSON(d)
        )

        /** Introduction rule that creates a term of type Model.As if the schema type parameter S of inhabit_type conforms to the Graph model.
        * The term is a function that creates a term of type Graph from data with schema S.
        * @tparam S Schema (as a type) of the data to be modelled as a Graph. 
        * @param isValid Can be implicitly created if the schema S conforms to the Graph model.
        */
        implicit def as_graph[SourceS <: HList, DestS <: HList, EdgeS <: HList, LS, LD, LE, S <: HList](
            implicit
            isValid: Graph.ValidSchema[SourceS, LS, DestS, LD, EdgeS, LE]
        ) = inhabit_type[FieldType[Graph.Source, SourceS] :: FieldType[Graph.Destination, DestS] :: FieldType[Graph.Edge, EdgeS] :: HNil, Graph.WithID[LS, LD, LE], Graph.Aux[SourceS, DestS, EdgeS, LS, LD, LE]](
            (d: List[FieldType[Graph.Source, SourceS] :: FieldType[Graph.Destination, DestS] :: FieldType[Graph.Edge, EdgeS] :: HNil]) => Graph[SourceS, LS, DestS, LD, EdgeS, LE](d)
        )
    }
}