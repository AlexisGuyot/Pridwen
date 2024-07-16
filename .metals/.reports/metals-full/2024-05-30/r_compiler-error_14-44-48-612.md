file:///C:/Users/Alexis.DESKTOP-K9CA12K/Documents/Recherche/Code/pridwen/src/main/scala/types/opschema/SelectField.scala
### java.lang.AssertionError: NoDenotation.owner

occurred in the presentation compiler.

presentation compiler configuration:
Scala version: 3.3.3
Classpath:
<HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\scala-lang\scala3-library_3\3.3.3\scala3-library_3-3.3.3.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\scala-lang\scala-library\2.13.12\scala-library-2.13.12.jar [exists ]
Options:



action parameters:
offset: 2277
uri: file:///C:/Users/Alexis.DESKTOP-K9CA12K/Documents/Recherche/Code/pridwen/src/main/scala/types/opschema/SelectField.scala
text:
```scala
package pridwen.types.opschema

import shapeless.{HList, Witness}
import shapeless.labelled.{FieldType, field}
import shapeless.ops.record.{Selector => RSelector}
import shapeless.ops.hlist.{Last}

import pridwen.types.support.AsHList

/** Same as [[pridwen.types.opschema.FollowPath]] but computes the FieldType of the last field of the path (name and type).
*
* @tparam Schema The schema to traverse.
* @tparam Path The path to follow to traverse the nested structure of the schema. Can be a HList or a single field name.
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
* FName = Witness.`'a`.T and FType = String
*
* or
*
* Path = f
* FName = Witness.`'f`.T and FType = Int
*/
trait SelectField[Schema <: HList, Path] { 
    /** Name (as a singleton type) and type of the last field in the path. */
    type FName ; type FType ; 

    /** Selects the field in data corresponding to the input Schema. */
    def apply(data: Schema): FieldType[FName, FType] 
}

object SelectField {
    type Aux[Schema <: HList, Path, FN, FT] = SelectField[Schema, Path] { type FName = FN ; type FType = FT }

    /** Introduction rule that creates a term of type SelectField in the case where the path
    * is a hlist of field names P.
    *
    * @param follow Computes implicitly the sub-schema NS formed by all the fields at the same level as the last field name of the path P.
    * @param getFieldName Selects implicitly the last field name FN of the path P.
    * @param getField Can be implictly created if the sub-schema NS contains a field named FN (as a singleton type).
    *
    * In this case, the term is a function using the implicit parameter follow to select the values in the data associated with the sub-schema NS 
    * and the implicit parameter getField to select the field in these values.
    */
    implicit def path_is_a_hlist[S <: HList, P, SubS <: HList, SubP <: HList, L](
        implicit
        follow: FollowPath.Aux[S, P, SubS],
        decompPath: DecompPath[P, SubP, @@]
    ): Aux[S, P, FN, FT] = new SelectField[S, P] {
        type FName = FN ; type FType = FT
        def apply(data: S): FieldType[FName, FType] = field[FName](follow.getField(data))
    }


    /** Same as [[pridwen.types.opschema.SelectField]] but changes the name of the computed FieldType.
    *
    * @tparam Schema The schema to traverse.
    * @tparam Path The path to follow to traverse the nested structure of the schema. Can be a HList or a single field name.
    * @tparam FName The new name of the selected field.
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
    * (f ->> Int <=> FieldType[Witness.`'f`.T, Int]).
    *
    * Path = a :: b :: c :: HNil
    * FName = newC
    * FType = String, apply returns values of type FieldName[Witness`'newC`.T, String]
    *
    * or
    *
    * Path = f
    * FName = newF
    * FType = Int, apply returns values of type FieldName[Witness`'newF`.T, Int]
    */
    trait As[Schema <: HList, Path, FName] { type FType ; def apply(data: Schema): FieldType[FName, FType] }
    
    object As {
        type Aux[Schema <: HList, Path, FName, FT] = As[Schema, Path, FName] { type FType = FT }

        /** Introduction rule that creates a term of type SelectField.As (encapsulated function).
        *
        * @param getField Computes implictly the name FN (as a singleton type) and the type FT of the last field of the path P.
        *
        * In this case, the term is a function using the implicit parameter getField
        * to select the field in the data, and using its value to create a new field named NN.
        */
        implicit def select_field_as[S <: HList, P, NN, FN, FT](
            implicit
            getField: SelectField.Aux[S, P, FN, FT]
        ): Aux[S, P, NN, FT] = new As[S, P, NN] {
            type FType = FT
            def apply(data: S): FieldType[NN, FType] = field[NN](getField(data))
        }
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