package pridwen.types.opschema

import scala.annotation.implicitNotFound

import shapeless.{HList, HNil, ::}

import pridwen.types.support.Updater

/** Replace the field at the end of a path with a new one in an existing schema. 
* Can be used to rename a field or to change its type.
*
* @tparam Schema The schema to update.
* @tparam Path The path to follow to find the field to replace. Can be a HList or a single field name.
* @tparam NewName The new name of the field to replace (as a singleton type).
* @tparam NewType The new type of the field to replace.
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
* Path = a :: b :: c :: HNil
* NewName = Witness.`'x`.T and FType = Boolean
*
* Out = a ->> (
                b ->> (
                    x ->> Boolean :: d ->> Int :: HNil
                ) :: 
                e ->> String :: 
                HNil
            ) :: 
            f ->> Int :: 
            HNil 
*/
@implicitNotFound("[Pridwen / ReplaceField] Impossible to create a new schema Out by replacing the field at the end of path ${Path} by the field ${NewName} ->> ${NewType} in schema ${Schema}.") 
trait ReplaceField[Schema <: HList, Path, NewName <: Symbol, NewType] {
    /** The new schema created by replacing the field inside the previous schema.
    */
    type Out <: HList

    /** Updates the value of the replaced attribute with a value of the new type.
    */
    def apply(data: Schema, value: NewType): Out
}

object ReplaceField {
    type Aux[Schema <: HList, Path, NewName <: Symbol, NewType, Out0 <: HList] = ReplaceField[Schema, Path, NewName, NewType] { type Out = Out0 }

    /** Creates a term of type ReplaceField (encapsulated function that updates the value of a certain field).
    */
    protected def inhabit_type[Schema <: HList, Path, NewName <: Symbol, NewType, Out0 <: HList](
        f: (Schema, NewType) => Out0
    ): Aux[Schema, Path, NewName, NewType, Out0] = new ReplaceField[Schema, Path, NewName, NewType] {
        type Out = Out0
        def apply(data: Schema, value: NewType): Out = f(data, value)
    }

    /** Proxy of ReplaceWithHPath that encaspulates the path in a hlist.
    */
    implicit def path_is_not_hlist[S <: HList, P, NName <: Symbol, NType, NewS <: HList](
        implicit
        replaceField: ReplaceWithHPath.Aux[S, P :: HNil, NName, NType, NewS]
    ) = inhabit_type[S, P, NName, NType, NewS](
        (d: S, v: NType) => replaceField(d, v)
    )

    /** Proxy of ReplaceWithHPath that just passes the path as is.
    */
    implicit def path_is_hlist[S <: HList, PH, PT <: HList, NName <: Symbol, NType, NewS <: HList](
        implicit
        replaceField: ReplaceWithHPath.Aux[S, PH :: PT, NName, NType, NewS]
    ) = inhabit_type[S, PH :: PT, NName, NType, NewS](
        (d: S, v: NType) => replaceField(d, v)
    )

    /** Same as ReplaceField but the path parameter is always a HList.
    */
    private trait ReplaceWithHPath[Schema <: HList, Path <: HList, NewName <: Symbol, NewType] { type Out <: HList ; def apply(data: Schema, value: NewType): Out }
    private object ReplaceWithHPath {
        type Aux[Schema <: HList, Path <: HList, NewName <: Symbol, NewType, Out0 <: HList] = ReplaceWithHPath[Schema, Path, NewName, NewType] { type Out = Out0 }

        /** Introduction rule that creates a term of type ReplaceWithHPath when the path P just contains one field name.
        *
        * @param update Replaces implicitly from schema S the field FN (end of path P) by a field NN of type NT and computes the resulting new schema Out0.
        */
        implicit def path_is_over[S <: HList, FN, NN <: Symbol, NT, Out0 <: HList](
            implicit
            update: Updater.Aux[S, FN, NN, NT, Out0]
        ): Aux[S, FN::HNil, NN, NT, Out0] = new ReplaceWithHPath[S, FN::HNil, NN, NT] {
            type Out = Out0
            def apply(data: S, value: NT): Out = update(data, value)
        }

        /** Introduction rule that creates a term of type ReplaceWithHPath when the path P contains multiple field names.
        *
        * @param selectField Selects implicitly from schema S the field FN providing an access to the next nested level associated with the sub-schema SubS.
        * @param updateSP Updates implicitly the sub-schema SubS to obtain a new sub-schema Out0.
        * @param updateS Updates implicitly the schema S by replacing the sub-schema reachable through field FN with the new sub-schema Out0.
        */
        implicit def path_is_not_over[S <: HList, SubS <: HList, FN, SP <: HList, NN <: Symbol, NT, Out0 <: HList, NewS <: HList](
            implicit
            selectField: SelectField.Aux[S, FN, _, SubS],
            updateSP: ReplaceWithHPath.Aux[SubS, SP, NN, NT, Out0],
            updateS: Updater.Aux[S, FN, FN, Out0, NewS]
        ): Aux[S, FN::SP, NN, NT, NewS] = new ReplaceWithHPath[S, FN::SP, NN, NT] {
            type Out = NewS
            def apply(data: S, value: NT): Out = updateS(data, updateSP(selectField(data), value))
        }
    }
}