package pridwen.types.support

import shapeless.{HList, Witness}
import shapeless.ops.record.{Selector => RSelector}

trait Selector[L <: HList, F] { type K ; type V ; def apply(l: L): V }
trait Selector1 {
    type Aux[L <: HList, F, K0, V0] = Selector[L, F] { type K = K0 ; type V = V0 }

    implicit def f_is_a_field_name[L <: HList, FN, FT](
        implicit
        selectF: RSelector.Aux[L, FN, FT]
    ): Aux[L, FN, FN, FT] = new Selector[L, FN] {
        type K = FN ; type V = FT
        def apply(l: L): V = selectF(l)
    }
}
object Selector extends Selector1 {
    implicit def f_is_a_singleton[L <: HList, W <: Witness, FN, FT](
        implicit
        selectF: RSelector.Aux[L, FN, FT]
    ): Aux[L, Witness.Aux[FN], FN, FT] = new Selector[L, Witness.Aux[FN]] {
        type K = FN ; type V = FT
        def apply(l: L): V = selectF(l)
    }
}