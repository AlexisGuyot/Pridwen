package pridwen.types.opschema

import scala.annotation.implicitNotFound

import shapeless.{HList, HNil, ::}
import shapeless.labelled.{FieldType, field}
import shapeless.ops.hlist.{Prepend}

import pridwen.types.support.{Updater}

/** Adds a new field at the end of an existing schema.
*
* @tparam Schema The schema to update.
* @tparam Path The path to follow to traverse the nested structure of the schema. Can be a HList or a single field name.
* @tparam FieldName The name of the new field (as a singleton type).
* @tparam FieldType The type of the new field.
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
* FieldName = Witness.`'x`.T and FType = Boolean
*
* Out = a ->> (
            b ->> (
                c ->> String :: d ->> Int :: x ->> Boolean :: HNil
            ) :: 
            e ->> String :: 
            HNil
        ) :: 
        f ->> Int :: 
        HNil 
*/
//@implicitNotFound("[Pridwen / AddField] Impossible to add field ${FieldName} ->> ${FieldType} into the sub-schema reachable through path ${Path} in schema ${Schema}.") 
trait AddField[Schema <: HList, Path, FieldName <: Symbol, FieldType] {
    /** The new schema created by adding the field at the end of the previous schema.
    */
    type Out <: HList

    /** Adds a value to the data to create new data with the updated schema.
    */
    def apply(data: Schema, value: FieldType): Out
}

trait AddField1 {
    type Aux[Schema <: HList, Path, FieldName <: Symbol, FieldType, Out0 <: HList] = AddField[Schema, Path, FieldName, FieldType] { type Out = Out0 }

    /** Creates a term of type AddField (encapsulated function that adds a new value to data with a certain schema).
    */
    protected def inhabit_type[Schema <: HList, Path, FieldName <: Symbol, FieldType, Out0 <: HList](
        f: (Schema, FieldType) => Out0
    ): Aux[Schema, Path, FieldName, FieldType, Out0] = new AddField[Schema, Path, FieldName, FieldType] {
        type Out = Out0
        def apply(data: Schema, value: FieldType): Out = f(data, value)
    }

    // Rules with lower priority

    /** Introduction rule that creates a term of type AddField in the case where the path only contains one element.
    * The new field has to be added in the lower level of nesting.
    *
    * @param selectLowerLvl Selects implicitly the lower level of nesting.
    * @param addField Adds implicitly the field to the lower level of nesting and computes the new schema.
    * @param updateLowLvl Updates implicitly the field that provides an access to the lower level of nesting.
    */
    implicit def path_with_one_element[S <: HList, P, FName <: Symbol, FN, FType, SubS <: HList, NewSubS <: HList, NewS <: HList](
        implicit
        selectLowerLvl: SelectField.Aux[S, P, FN, SubS],
        addField: AddField.Aux[SubS, HNil, FName, FType, NewSubS],
        updateLowLvl: Updater.Aux[S, FN, FN, NewSubS, NewS]
    ) = inhabit_type[S, P, FName, FType, NewS](
        (d: S, v: FType) => updateLowLvl(d, addField(selectLowerLvl(d), v))
    )
}
object AddField extends AddField1 {
    // Rules with higher priority

    /** Introduction rule that creates a term of type AddField in the case where the path is empty.
    * The new field must be added to this level of nesting.
    *
    * @param addField Adds implicitly the field and computes its type.
    */
    implicit def path_is_empty[S <: HList, FName <: Symbol, FType, NewS <: HList](
        implicit
        addField: Prepend.Aux[S, FieldType[FName, FType]::HNil, NewS]
    ) = inhabit_type[S, HNil, FName, FType, NewS](
        (d: S, v: FType) => addField(d, field[FName](v) :: HNil)
    )

    /** Introduction rule that creates a term of type AddField in the case where the path is not empty.
    * The new field has to be added in a lower level of nesting.
    *
    * @param selectLowerLvl Selects implicitly the lower level of nesting.
    * @param addField Adds implicitly the field to the lower level of nesting and computes the new schema.
    * @param updateLowLvl Updates implicitly the field that provides an access to the lower level of nesting.
    */
    implicit def path_is_not_empty[S <: HList, PH, PT <: HList, PHName, SubS <: HList, FName <: Symbol, FType, NewSubS <: HList, NewS <: HList](
        implicit
        selectLowerLvl: SelectField.Aux[S, PH, _, SubS],
        addField: AddField.Aux[SubS, PT, FName, FType, NewSubS],
        updateLowLvl: Updater.Aux[S, PH, PH, NewSubS, NewS]
    ) = inhabit_type[S, PH::PT, FName, FType, NewS](
        (d: S, v: FType) => updateLowLvl(d, addField(selectLowerLvl(d), v))
    )
}