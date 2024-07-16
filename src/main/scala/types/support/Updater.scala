package pridwen.types.support

import shapeless.{HList, ::, HNil}
import shapeless.labelled.{FieldType, field}

trait Updater[L <: HList, N, NN, NT] { type Out <: HList ; def apply(l: L, v: NT): Out }
object Updater {
    type Aux[L <: HList, N, NN, NT, Out0 <: HList] = Updater[L, N, NN, NT] { type Out = Out0 }
    //type KeepName[L <: HList, N, NT, Out0 <: HList] = Updater[L, N, N, NT] { type Out = Out0 }

    protected def inhabit_type[L <: HList, N, NN, NT, Out0 <: HList](
        f: (L, NT) => Out0
    ): Aux[L, N, NN, NT, Out0] = new Updater[L, N, NN, NT] {
        type Out = Out0
        def apply(l: L, v: NT): Out = f(l,v)
    }

    implicit def update_first_element[N, T, SL <: HList, NN, NT] = inhabit_type[FieldType[N, T]::SL, N, NN, NT, FieldType[NN, NT]::SL]((l: FieldType[N, T]::SL, v: NT) => field[NN](v) :: l.tail)

    implicit def update_other_elements[M, N, T, SL <: HList, NN, NT, Out0 <: HList](
        implicit
        updateTail: Updater.Aux[SL, N, NN, NT, Out0]
    ) = inhabit_type[FieldType[M, T]::SL, N, NN, NT, FieldType[M, T]::Out0]((l: FieldType[M, T]::SL, v: NT) => l.head :: updateTail(l.tail, v))

    /* implicit def update_and_keep_name[L <: HList, N, NT, Out0 <: HList](
        implicit
        update: Updater.Aux[L, N, N, NT, Out0]
    ): KeepName[L, N, NT, Out0] = new Updater[L, N, N, NT] {
        type Out = Out0
        def apply(l: L, v: NT): Out = update(l,v)
    } */
}