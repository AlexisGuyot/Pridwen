package pridwen.types.opschema

import scala.annotation.implicitNotFound

import shapeless.{HList, HNil, ::}
import shapeless.ops.record.{Remover}

import pridwen.types.support.{Updater}

/** Removes a field included in an existing schema.
*
* @tparam Schema The schema to update.
* @tparam Path The path to follow to find the field to remove. Can be a HList or a single field name.
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
*
* Out = a ->> (
                b ->> (
                    d ->> Int :: HNil
                ) :: 
                e ->> String :: 
                HNil
            ) :: 
            f ->> Int :: 
            HNil 
*/
@implicitNotFound("[Pridwen / RemoveField] Impossible to create a new schema Out by removing the field at the end of path ${Path} in schema ${Schema}.") 
trait RemoveField[Schema <: HList, Path] {
    /** The new schema created by removing the field in the existing schema.
    */
    type Out <: HList

    /** Removes the value in the data.
    */
    def apply(data: Schema): Out
}

trait RemoveField1 {
    type Aux[Schema <: HList, Path, Out0 <: HList] = RemoveField[Schema, Path] { type Out = Out0 }

    /** Creates a term of type RemoveField (encapsulated function that removes the value associated 
    * with a certain field in a certain schema).
    */
    protected def inhabit_type[Schema <: HList, Path, Out0 <: HList](
        f: Schema => Out0
    ): Aux[Schema, Path, Out0] = new RemoveField[Schema, Path] {
        type Out = Out0
        def apply(data: Schema): Out = f(data)
    }

    // Rules with lower priority

    /** Introduction rule that creates a term of type RemoveField in the case where the path is not a hlist but a single field name.
    * The field must be deleted on this level of nesting.
    *
    * @param removeField Removes implicitly the field and computes the new schema.
    */
    implicit def path_is_not_hlist[S <: HList, FName, FType, NewS <: HList](
        implicit
        removeField: Remover.Aux[S, FName, (FType, NewS)]
    ) = inhabit_type[S, FName, NewS](
        (d: S) => removeField(d)._2
    )
}
object RemoveField extends RemoveField1 {
    // Rules with higher priority

    /** Introduction rule that creates a term of type RemoveField in the case where the path is over (one field).
    * The field must be deleted on this level of nesting.
    *
    * @param removeField Removes implicitly the field and computes the new schema.
    */
    implicit def path_is_over[S <: HList, FName, FType, NewS <: HList](
        implicit
        removeField: Remover.Aux[S, FName, (FType, NewS)]
    ) = inhabit_type[S, FName::HNil, NewS](
        (d: S) => removeField(d)._2
    )

    /** Introduction rule that creates a term of type RemoveField in the case where the path is not empty.
    * The field has to be removed in a lower level of nesting.
    *
    * @param selectLowerLvl Selects implicitly the lower level of nesting.
    * @param removeField Removes implicitly the field to the lower level of nesting and computes the new schema.
    * @param updateLowLvl Updates implicitly the field that provides an access to the lower level of nesting.
    */
    implicit def path_is_not_over[S <: HList, PH, PT <: HList, PHName, SubS <: HList, FName, FType, NewSubS <: HList, NewS <: HList](
        implicit
        selectLowerLvl: SelectField.Aux[S, PH, _, SubS],
        removeField: RemoveField.Aux[SubS, PT, NewSubS],
        updateLowLvl: Updater.Aux[S, PH, PH, NewSubS, NewS]
    ) = inhabit_type[S, PH::PT, NewS](
        (d: S) => updateLowLvl(d, removeField(selectLowerLvl(d)))
    )
}