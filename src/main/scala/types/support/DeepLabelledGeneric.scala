package pridwen.types.support

import shapeless.{LabelledGeneric, HList, ::, HNil}
import shapeless.labelled.{FieldType, field}

/** Similar to Shapeless' LabelledGeneric but with nesting.
*
* Example :
* case class MyHList(aString: String, aInt: Int)
* case class MyNestedHList(aNested: MyHList)
*
* Shapeless LabelledGeneric[MyNestedHList].Repr =:= FieldType[Witness.`'aNested`.T, MyHList] :: HNil
* Pridwen DeepLabelledGeneric[MyNestedHList].Repr =:= FieldType[Witness.`'aNested`.T, FieldType[Witness.`'aString`.T, String] :: FieldType[Witness.`'aInt`.T, Int] :: HNil] :: HNil
*
* @tparam T Product (case class) to transform to a HList.
*/
trait DeepLabelledGeneric[T] {
    /** The generic representation type for {T}, which will be composed of {HList} types. Can be referenced through the Aux auxiliary type of the companion object. */
    type Repr

    /** Convert an instance of the concrete type to the generic value representation */
    def to(t: T): Repr
}

object DeepLabelledGeneric {
    type Aux[T, Repr0] = DeepLabelledGeneric[T] { type Repr = Repr0 }

    /** Provides an instance of DeepLabelledGeneric for the given T. As with [[shapeless.LabelledGeneric]],
    * use this method to obtain an instance for suitable given T. */
    def apply[T](implicit lgen: DeepLabelledGeneric[T]): Aux[T, lgen.Repr] = lgen

    /** Creates a Generic Representation of T with Shapeless and then recursively goes through the result to create deep Generic Representations of nested HList if necessary. */
    implicit def first_repr[T, ReprT, Out](
        implicit
        lgen: LabelledGeneric.Aux[T, ReprT],
        unnest: DigNested.Aux[ReprT, Out]
    ): Aux[T, Out] = new DeepLabelledGeneric[T] {
        type Repr = Out
        def to(t: T): Repr = unnest(lgen.to(t))
    }

    /** Recursively goes through a hlist to transform its head into a hlist if necessary. */
    private trait DigNested[Repr] { type Out ; def apply(r: Repr): Out }
    private trait DigNested1 {
        type Aux[Repr, Out0] = DigNested[Repr] { type Out = Out0 }

        protected def inhabit_type[Repr, Out0](
            f: Repr => Out0
        ): Aux[Repr, Out0] = new DigNested[Repr] {
            type Out = Out0
            def apply(r: Repr): Out = f(r)
        }

        /** If the first field (H) IS NOT a nested HList (default). 
        * Just keep going through the remaining elements of the HList (T) and merge the current head as-is with the result (H::TOut).
        */
        implicit def not_nested_first_field[H, T <: HList, TOut <: HList](
            implicit
            unnestT: DigNested.Aux[T, TOut]
        ) = inhabit_type[H::T, H::TOut](
            (r: H::T) => r.head :: unnestT(r.tail)
        )

        /** If the hlist to go through is empty or if the recursion is over. 
        * Just returns an empty HList.
        */
        implicit def empty_hlist = inhabit_type[HNil, HNil](_ => HNil)
    }
    private object DigNested extends DigNested1 {
        /** If the first field (FieldType[FName,FType]) IS a nested HList (checked first). 
        * Transform the head type (FType) into a HList (FRepr), creates a new head with the result (FieldType[FName,FRepr]),
        * keep going through the remaining elements of the HList (T), and merge the new head with the result (FieldType[FName,FRepr]::TOut).
        */
        implicit def nested_first_field[FName, FType, T <: HList, FRepr, TOut <: HList](
            implicit
            lgen: DeepLabelledGeneric.Aux[FType, FRepr],
            unnestT: DigNested.Aux[T, TOut]
        ) = inhabit_type[FieldType[FName,FType]::T, FieldType[FName,FRepr]::TOut](
            (r: FieldType[FName,FType]::T) => field[FName](lgen.to(r.head)) :: unnestT(r.tail)
        )
    }
}