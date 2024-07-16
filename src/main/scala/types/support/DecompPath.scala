package pridwen.types.support

import shapeless.{HList, ::, HNil}
import shapeless.ops.hlist.{Init, Last}
import java.lang.Character.Subset

/**
  * Extracts the last element of a path.
  * 
  * Exemple (general) :
    P = a :: b :: c :: HNil
    SubPath = a :: b :: HNil ; Last = c
  * 
  * Exemple (path with one element)
  * P = a :: HNil
  * SubPath = HNil ; Last = a
  */
trait DecompPath[P] { type SubPath <: HList ; type Last }
object DecompPath {
    type Aux[P, SubPath0 <: HList, Last0] = DecompPath[P] { type SubPath = SubPath0 ; type Last = Last0 }

    implicit def path_as_hlist[P, HP <: HList, SP0 <: HList, L0](
      implicit
      asHList: AsHList.Aux[P, HP],
      decompHPath: DecompHPath.Aux[HP, SP0, L0]
    ): Aux[P, SP0, L0] = new DecompPath[P] {
      type SubPath = SP0 ; type Last = L0
    }

    private trait DecompHPath[P <: HList] { type SubPath <: HList ; type Last }
    private object DecompHPath {
      type Aux[P <: HList, SubPath0 <: HList, Last0] = DecompHPath[P] { type SubPath = SubPath0 ; type Last = Last0 }

      protected def inhabit_type[P <: HList, SubPath0 <: HList, Last0]: Aux[P, SubPath0, Last0] = new DecompHPath[P] {
          type SubPath = SubPath0 ; type Last = Last0
      }

      implicit def path_with_one_element[E] = inhabit_type[E::HNil, HNil, E]

      implicit def path_with_multiple_elements[H, T <: HList, SP <: HList, L](
          implicit    
          getSubPath: Init.Aux[H::T, SP],
          getLast: Last.Aux[H::T, L]
      ) = inhabit_type[H::T, SP, L]
    }
}