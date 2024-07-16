package pridwen.types.opschema

import scala.annotation.implicitNotFound

import shapeless.{HList, ::, HNil}
import shapeless.labelled.{FieldType, field}

import pridwen.types.support.functions.fieldToValue

/** Same as [[pridwen.types.opschema.SelectField]] but with multiple paths included in a HList.
* Computes a new schema formed by all the selected attributes.
*
* @tparam Schema The schema to traverse.
* @tparam Paths The paths to follow to traverse the nested structure of the schema.
* 
* Example :
* Schema =  a ->> (
                b ->> (
                    c ->> String :: d ->> Int :: HNil
                ) :: 
                e ->> String :: 
                HNil
            ) :: 
            f ->> Int :: 
            HNil 
* with f ->> Int <=> FieldType[Witness.`'f`.T, Int].
*
* Paths = (a :: b :: c) :: (a :: e :: HNil) :: f :: HNil
* Out = c ->> String :: e ->> String :: f ->> Int :: HNil
*/
@implicitNotFound("[Pridwen / SelectMany] Impossible to create a new schema Out by selecting multiple fields in schema ${Schema} through paths ${Paths}.") 
trait SelectMany[Schema <: HList, Paths <: HList] { 
    /** Schema formed by all the selected attributes. */
    type Out <: HList 

    /** Creates a new tuple with the selected attributes. */
    def apply(data: Schema): Out 
}

object SelectMany {
    type Aux[Schema <: HList, Paths <: HList, Out0 <: HList] = SelectMany[Schema, Paths] { type Out = Out0 }

    /** Creates a term of type SelectMany (encapsulated function that creates a new tuple with the selected fields).
    */
    protected def inhabit_type[Schema <: HList, Paths <: HList, Out0 <: HList](
        f: Schema => Out0
    ): Aux[Schema, Paths, Out0] = new SelectMany[Schema, Paths] {
        type Out = Out0
        def apply(data: Schema): Out = f(data)
    }

    /** Introduction rule that creates a term of type SelectMany in the case where there is only
    * one single field named FN (as a singleton type) with type FT to select in S through path P.
    *
    * @param getField Selects implicitly the field and computes its type.
    *
    * In this case, the term is a function using the implicit parameter getField to select the field in the data,
    * and using the result to create a new tuple with only one element.
    */
    implicit def only_one_path[S <: HList, P, FN, FT](
        implicit
        getField: SelectField.Aux[S, P, FN, FT]
    ) = inhabit_type[S, P::HNil, FieldType[FN,FT]::HNil](
        (d: S) => getField(d) :: HNil
    )

    /** Introduction rule that creates a term of type SelectMany in the case where there is
    * one field named FN (as a singleton type) with type FT to select in S through path FP,
    * but also other fields forming a new schema OF to select in S through other paths OP.
    *
    * @param getField Selects implicitly the first field and computes its type.
    * @param getOthers Selects implicitly all the other fields and computes a new schema with them.
    *
    * In this case, the term is a function using the implicit parameter getField to select the first field in the data,
    * then the parameter getOther to select all the other fields and create a new tuple with them,
    * and finally merging all the different fields into one final tuple.
    */
    implicit def multiple_paths[S <: HList, FP, OP <: HList, FN, FT, OF <: HList](
        implicit
        getField: SelectField.Aux[S, FP, FN, FT],
        getOthers: SelectMany.Aux[S, OP, OF]
    ) = inhabit_type[S, FP::OP, FieldType[FN,FT]::OF](
        (d: S) => getField(d) :: getOthers(d)
    )

    trait As[Schema <: HList, Paths <: HList] { type Out <: HList ; def apply(data: Schema): Out }
    object As {
        type Aux[Schema <: HList, Paths <: HList, Out0 <: HList] = As[Schema, Paths] { type Out = Out0 }

        protected def inhabit_type[Schema <: HList, Paths <: HList, Out0 <: HList](
            f: Schema => Out0
        ): Aux[Schema, Paths, Out0] = new As[Schema, Paths] {
            type Out = Out0
            def apply(data: Schema): Out = f(data)
        }

        implicit def only_one_path[S <: HList, P, NN <: Symbol, FN, FT](
            implicit
            selectField: SelectField.Aux[S, P, FN, FT]
        ) = inhabit_type[S, (P, NN)::HNil, FieldType[NN, FT] :: HNil](
            (d: S) => field[NN](fieldToValue(selectField(d))) :: HNil
        )

        implicit def multiple_paths[S <: HList, P, NN <: Symbol, T <: HList, FN, FT, Out0 <: HList](
            implicit
            selectField: SelectField.Aux[S, P, FN, FT],
            selectOthers: SelectMany.As.Aux[S, T, Out0]
        ) = inhabit_type[S, (P, NN)::T, FieldType[NN, FT]::Out0](
            (d: S) => field[NN](fieldToValue(selectField(d))) :: selectOthers(d)
        )
    }
}