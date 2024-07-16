package pridwen.types.models

import java.time.{LocalDate => Date}

import scala.annotation.implicitNotFound

import shapeless.{HList, ::, HNil}
import shapeless.labelled.{FieldType}

import pridwen.types.support.DeepLabelledGeneric

/** Type associated with the JSON model.
*
* The Schema of a JSON can be referenced through the JSON.Aux auxiliary type.
*/
trait JSON extends Model

object JSON {
    type Aux[Schema0 <: HList] = JSON { type Schema = Schema0 }

    /** Creates terms of type JSON (list of tuples).
    *
    * @param _data Tuples to encapsulate as a term of type JSON.
    * @param isValid Can be implicitly created if the schema of the tuples conforms to the JSON model.
    */
    def apply[S <: HList](_data: List[S])(implicit isValid: ValidSchema[S]): JSON.Aux[S] = new JSON {
        type Schema = S
        val data: List[Schema] = _data
    }

    /** Creates terms of type JSON (list of tuples expressed through case classes).
    *
    * @param _data Tuples to encapsulate as a term of type JSON.
    * @param toHList Creates implicitly a function to convert a case class to a hlist.
    * @param isValid Can be implicitly created if the schema of the tuples conforms to the JSON model.
    */
    def apply[S <: Product, HS <: HList](_data: List[S])(implicit toHList: DeepLabelledGeneric.Aux[S,HS], isValid: ValidSchema[HS]): JSON.Aux[HS] = new JSON {
        type Schema = HS
        val data: List[Schema] = _data.map(tuple => toHList.to(tuple))
    }

    /** Type that checks that a given schema (S) conforms to the JSON model. 
    */
    @implicitNotFound("[Pridwen / ValidSchema] Schema ${S} does not conform to the JSON model.")
    trait ValidSchema[S <: HList] 
    object ValidSchema {
        /** Creates terms of type ValidSchema (empty objects).
        */
        private def inhabit_type[S <: HList]: ValidSchema[S] = new ValidSchema[S] {}

        /** Introduction rule that creates a term of type ValidSchema if the type parameter of inhabit_type conforms to the JSON model.
        * Can be applied when the input schema is not an empty schema.
        * @tparam FName First attribute name as a singleton type.
        * @tparam FType First attribute type.
        * @tparam OtherF Schema formed by the remaining attributes of the input schema.
        * @param validFType Can be implicitly created if the type of the first attribute is supported by the JSON model.
        * @param validOtherF Can be implicitly created if the schema formed by the remaining attributes conforms to the JSON model.
        */
        implicit def schema_not_empty[FName, FType, OtherF <: HList](
            implicit
            validFType: JType[FType],
            validOtherF: ValidSchema[OtherF]
        ) = inhabit_type[FieldType[FName, FType]::OtherF]

        /** Introduction rule that creates a term of type ValidSchema if the type parameter of inhabit_type conforms to the JSON model.
        * Can be applied when the input schema is an empty schema.
        */
        implicit def schema_empty = inhabit_type[HNil]

        /** Type that checks that a given attribute type (T) is supported by the JSON model.
        */
        @implicitNotFound("[Pridwen / JType] ${T} is not a supported type by the attributes of the JSON model.") 
        trait JType[T]
        object JType {
            /** Creates terms of type JType (empty objects).
            */
            private def inhabit_JType[T]: JType[T] = new JType[T] {}

            // Introduction rules (one by supported attribute type).
            implicit def json_string = inhabit_JType[String]
            implicit def json_int = inhabit_JType[Int]
            implicit def json_double = inhabit_JType[Double]
            implicit def json_long = inhabit_JType[Long]
            implicit def json_bool = inhabit_JType[Boolean]
            implicit def json_multi[T : JType] = inhabit_JType[List[T]]
            implicit def json_nested[Schema <: HList : ValidSchema] = inhabit_JType[Schema]
        }
    }
}