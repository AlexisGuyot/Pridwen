package pridwen.types.models

import java.time.{LocalDate => Date}

import scala.annotation.implicitNotFound

import shapeless.{HList, ::, HNil, LabelledGeneric}
import shapeless.labelled.{FieldType}

/** Type associated with the relational model.
*
* The Schema of a Relation can be referenced through the Relation.Aux auxiliary type.
*/
trait Relation extends Model

object Relation {
    type Aux[Schema0 <: HList] = Relation { type Schema = Schema0 }

    /** Creates terms of type Relation (list of tuples).
    *
    * @param _data Tuples to encapsulate as a term of type Relation.
    * @param isValid Can be implicitly created if the schema of the tuples conforms to the relational model.
    */
    def apply[S <: HList](_data: List[S])(implicit isValid: ValidSchema[S]): Relation.Aux[S] = new Relation {
        type Schema = S
        val data: List[Schema] = _data
    }

    /** Creates terms of type Relation (list of tuples expressed through case classes).
    *
    * @param _data Tuples to encapsulate as a term of type Relation.
    * @param toHList Creates implicitly a function to convert a case class to a hlist.
    * @param isValid Can be implicitly created if the schema of the tuples conforms to the relational model.
    */
    def apply[S <: Product, HS <: HList](_data: List[S])(implicit toHList: LabelledGeneric.Aux[S,HS], isValid: ValidSchema[HS]): Relation.Aux[HS] = new Relation {
        type Schema = HS
        val data: List[Schema] = _data.map(tuple => toHList.to(tuple))
    }

    /** Type that checks that a given schema (S) conforms to the relational model. 
    */
    @implicitNotFound("[Pridwen / ValidSchema] Schema ${S} does not conform to the relational model.")
    trait ValidSchema[S <: HList] 
    object ValidSchema {
        /** Creates terms of type ValidSchema (empty objects).
        */
        private def inhabit_type[S <: HList]: ValidSchema[S] = new ValidSchema[S] {}

        /** Introduction rule that creates a term of type ValidSchema if the type parameter of inhabit_type conforms to the relational model.
        * Can be applied when the schema formed by the remaining attributes of the input schema is not an empty schema.
        * @tparam FName First attribute name as a singleton type.
        * @tparam FType First attribute type.
        * @tparam OtherF Schema formed by the remaining attributes of the input schema.
        * @param validFType Can be implicitly created if the type of the first attribute is supported by the relational model.
        * @param validOtherF Can be implicitly created if the schema formed by the remaining attributes conforms to the relational model.
        */
        implicit def schema_with_multiple_fields[FName, FType, OtherF <: HList](
            implicit
            validFType: RType[FType],
            validOtherF: ValidSchema[OtherF]
        ) = inhabit_type[FieldType[FName, FType]::OtherF]

        /** Introduction rule that creates a term of type ValidSchema if the type parameter of inhabit_type conforms to the relational model.
        * Can be applied when the schema formed by the remaining attributes of the input schema is an empty schema.
        * @tparam FName First attribute name as a singleton type.
        * @tparam FType First attribute type.
        * @param validFType Can be implicitly created if the type of the first attribute is supported by the relational model.
        */
        implicit def schema_with_one_field[FName, FType](
            implicit 
            validFType: RType[FType]
        ) = inhabit_type[FieldType[FName, FType]::HNil]

        /** Type that checks that a given attribute type (T) is supported by the relational model.
        */
        @implicitNotFound("[Pridwen / RType] ${T} is not a supported type by the attributes of the relational model.") 
        trait RType[T]
        object RType {
            /** Creates terms of type RType (empty objects).
            */
            private def inhabit_RType[T]: RType[T] = new RType[T] {}

            // Introduction rules (one by supported attribute type).
            implicit def rel_string = inhabit_RType[String]
            implicit def rel_int = inhabit_RType[Int]
            implicit def rel_double = inhabit_RType[Double]
            implicit def rel_long = inhabit_RType[Long]
            implicit def rel_bool = inhabit_RType[Boolean]
            implicit def rel_date = inhabit_RType[Date]
        }
    }
}