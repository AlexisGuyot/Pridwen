file:///C:/Users/Alexis.DESKTOP-K9CA12K/Documents/Recherche/Code/pridwen/src/main/scala/types/support/DecompPath.scala.scala
### java.lang.AssertionError: NoDenotation.owner

occurred in the presentation compiler.

presentation compiler configuration:
Scala version: 3.3.3
Classpath:
<HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\scala-lang\scala3-library_3\3.3.3\scala3-library_3-3.3.3.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\scala-lang\scala-library\2.13.12\scala-library-2.13.12.jar [exists ]
Options:



action parameters:
offset: 657
uri: file:///C:/Users/Alexis.DESKTOP-K9CA12K/Documents/Recherche/Code/pridwen/src/main/scala/types/support/DecompPath.scala.scala
text:
```scala
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

    implicit def path_as_hlist[P, SP0 <: HList, L0](
      implicit
      asHList: AsHList.Aux[@@]
      decompHPath: DecompHPath[]
    ): Aux[P, SP0, L0] = new DecompPath[P] {
      type SubPath = SP0 ; type Last = L0
    }

    private trait DecompHPath[P <: HList] { type SubPath <: HList ; type Last }
    object DecompHPath {
      type Aux[P <: HList, SubPath0 <: HList, Last0] = DecompPath[P] { type SubPath = SubPath0 ; type Last = Last0 }

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
```



#### Error stacktrace:

```
dotty.tools.dotc.core.SymDenotations$NoDenotation$.owner(SymDenotations.scala:2607)
	scala.meta.internal.pc.SignatureHelpProvider$.isValid(SignatureHelpProvider.scala:83)
	scala.meta.internal.pc.SignatureHelpProvider$.notCurrentApply(SignatureHelpProvider.scala:94)
	scala.meta.internal.pc.SignatureHelpProvider$.$anonfun$1(SignatureHelpProvider.scala:48)
	scala.collection.StrictOptimizedLinearSeqOps.dropWhile(LinearSeq.scala:280)
	scala.collection.StrictOptimizedLinearSeqOps.dropWhile$(LinearSeq.scala:278)
	scala.collection.immutable.List.dropWhile(List.scala:79)
	scala.meta.internal.pc.SignatureHelpProvider$.signatureHelp(SignatureHelpProvider.scala:48)
	scala.meta.internal.pc.ScalaPresentationCompiler.signatureHelp$$anonfun$1(ScalaPresentationCompiler.scala:412)
```
#### Short summary: 

java.lang.AssertionError: NoDenotation.owner