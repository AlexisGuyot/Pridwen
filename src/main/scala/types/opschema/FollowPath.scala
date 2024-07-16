package pridwen.types.opschema

import scala.annotation.implicitNotFound

import shapeless.{HList, ::, HNil}
//import shapeless.ops.record.{Selector}

import pridwen.types.support.{AsHList, Selector}

/** Goes through the nested structure of a schema via a path
* and compute the sub-schema formed by the fields at the level of the last field in the path.
*
* @tparam Schema The schema to traverse.
* @tparam Path The path to follow to traverse the nested structure of the schema. Can be a HList or a field name (as a singleton type).
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
* Path = a :: b :: c :: HNil
* Out = c ->> String :: d ->> Int :: HNil
*/
@implicitNotFound("[Pridwen / FollowPath] Impossible to select any sub-schema Out through path ${Path} in schema ${Schema}.") 
trait FollowPath[Schema <: HList, Path] { 

    /** The sub-schema formed by the fields at the level of the last field in the path.
    */
    type Out <: HList 

    /** Returns the values associated with the computed sub-schema.
    */
    def apply(data: Schema): Out 
}

object FollowPath {
    type Aux[Schema <: HList, Path, Out0 <: HList] = FollowPath[Schema, Path] { type Out = Out0 }

    protected def inhabit_type[Schema <: HList, Path, Out0 <: HList](
        f: Schema => Out0
    ): Aux[Schema, Path, Out0] = new FollowPath[Schema, Path] {
        type Out = Out0
        def apply(data: Schema): Out = f(data)
    }

    implicit def path_is_empty[S <: HList] = inhabit_type[S, HNil, S]((d: S) => d)

    implicit def path_as_hlist[S <: HList, P, HP <: HList, Out0 <: HList](
        implicit
        asHList: AsHList.Aux[P, HP],
        followPath: FollowNotEmptyPath.Aux[S, HP, Out0]
    ) = inhabit_type[S, P, Out0]((d: S) => followPath(d))
    

    /** Same as FollowPath but with a schema that is necessarly not empty.
    * The field at the end of the path must exist in the nested schema.
    */
    private trait FollowNotEmptyPath[Schema <: HList, Path <: HList] { type Out <: HList ; def apply(data: Schema): Out }
    private object FollowNotEmptyPath {
        type Aux[Schema <: HList, Path <: HList, Out0 <: HList] = FollowNotEmptyPath[Schema, Path] { type Out = Out0 }

        /** Creates a term of type FollowNotEmptyPath (encapsulated function that returns the elements at the end of the path).
        */
        protected def inhabit_type[Schema <: HList, Path <: HList, Out0 <: HList](
            f: Schema => Out0
        ): Aux[Schema, Path, Out0] = new FollowNotEmptyPath[Schema, Path] {
            type Out = Out0
            def apply(data: Schema): Out = f(data)
        }

        /** Introduction rule that creates a term of type FollowNotEmptyPath in the case where the path
        * only consists of a single field name FN (which must exist in the schema S).
        *
        * @param getField Can be implictly created if the schema S contains a field named FN (as a singleton type).
        *
        * In this case, the term is a function returning all the fields existing at the field level (in other words the fields that form S).
        */
        implicit def path_is_over[S <: HList, FN](
            implicit
            getField: Selector[S, FN]
        ) = inhabit_type[S, FN::HNil, S](
            (d: S) => d
        )

        /** Introduction rule that creates a term of type FollowNotEmptyPath in the case where the path
        * consists of a first field name FN (which must exist in the schema S) and some other field names forming a sub-path SP.
        *
        * @param getField Can be implictly created if the schema S contains a field named FN (as a singleton type).
        * @param follow Computes implicitly the sub-schema Out formed by the fields at the same level as the last attribute in the sub-path SP.
        *
        * In this case, the term is a function that selects the values nested in the field named FN and that returns the values obtained by following the sub-path in their schema.
        */
        implicit def path_is_not_over[S <: HList, F, FN, FT <: HList, SP <: HList, Out0 <: HList](
            implicit
            getField: Selector.Aux[S, F, FN, FT],
            follow: FollowNotEmptyPath.Aux[FT, SP, Out0]
        ) = inhabit_type[S, F::SP, Out0](
            (d: S) => follow(getField(d))
        )
    }
}