package pridwen.types.opschema

import pridwen.types.support.DecompPath

import shapeless.{HList, ::, HNil}
import shapeless.labelled.FieldType

trait AddMany[S <: HList, P <: HList, F <: HList] { type Out <: HList }
object AddMany {
    type Aux[S <: HList, P <: HList, F <: HList, Out0 <: HList] = AddMany[S, P, F] { type Out = Out0 }

    implicit def nothing_to_add[S <: HList]: Aux[S, HNil, HNil, S] = new AddMany[S, HNil, HNil] { type Out = S }
    implicit def something_to_add[S <: HList, P <: HList, OP <: HList, FN <: Symbol, FT, OF <: HList, NS1 <: HList, NS2 <: HList](
        implicit
        add_others: AddMany.Aux[S, OP, OF, NS1],
        add_field: AddField.Aux[NS1, P, FN, FT, NS2]
    ): Aux[S, P::OP, FieldType[FN,FT]::OF, NS2] = new AddMany[S, P::OP, FieldType[FN,FT]::OF] { type Out = NS2 }
}