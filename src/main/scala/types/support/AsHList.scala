package pridwen.types.support

import shapeless.{HList, HNil, ::}

trait AsHList[T] { type Out <: HList ; def apply(v: T): Out }
trait AsHList1 {
    type Aux[T, Out0 <: HList] = AsHList[T] { type Out = Out0 }

    protected def inhabit_type[T, Out0 <: HList](f: T => Out0): Aux[T, Out0] = new AsHList[T] {
        type Out = Out0
        def apply(v: T): Out = f(v)
    }
    
    implicit def t_is_not_hlist[T] = inhabit_type[T, T::HNil]((v: T) => v :: HNil)
}
object AsHList extends AsHList1 {
    implicit def t_is_hlist[H, T <: HList] = inhabit_type[H::T, H::T]((v: H::T) => v)
    implicit def t_is_hnil = inhabit_type[HNil, HNil]((v: HNil) => HNil)
}