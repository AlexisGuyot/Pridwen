file:///C:/Users/Alexis.DESKTOP-K9CA12K/Documents/Recherche/Code/pridwen/src/main/scala/types/opschema/SelectMany.scala
### java.lang.AssertionError: NoDenotation.owner

occurred in the presentation compiler.

presentation compiler configuration:
Scala version: 3.3.3
Classpath:
<HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\scala-lang\scala3-library_3\3.3.3\scala3-library_3-3.3.3.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\scala-lang\scala-library\2.13.12\scala-library-2.13.12.jar [exists ]
Options:



action parameters:
uri: file:///C:/Users/Alexis.DESKTOP-K9CA12K/Documents/Recherche/Code/pridwen/src/main/scala/types/opschema/SelectMany.scala
text:
```scala
package pridwen.types.opschema

import shapeless.{HList, ::, HNil}
import shapeless.labelled.{FieldType}

/** Same as [[pridwen.types.opschema.SelectField]] but with multiple paths included in a HList.
* Computes a new schema formed by all the selected attributes.
*
* @tparam Schema The schema to traverse.
* @tparam Paths The paths to follow to traverse the nested structure of the schema.
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
* Paths = (a :: b :: c) :: (a :: e :: HNil) :: f :: HNil
* Out = c ->> String :: e ->> String :: f ->> Int :: HNil
*/
trait SelectMany[Schema <: HList, Paths <: HList] { 
    /** Schema formed by all the selected attributes. */
    type Out <: HList 

    /** Creates a new tuple with the selected attributes. */
    def apply(data: Schema): Out 
}

object SelectMany {
    type Aux[Schema <: HList, Paths <: HList, Out0 <: HList] = SelectMany[Schema, Paths] { type Out = Out0 }

    /** Creates a term of type SelectMany (encapsulated function that creates a new tuple with the selected fields).
    */
    protected def inhabit_type[Schema <: HList, Paths <: HList, Out0 <: HList](
        f: Schema => Out0
    ): Aux[Schema, Paths, Out0] = new SelectMany[Schema, Paths] {
        type Out = Out0
        def apply(data: Schema): Out = f(data)
    }

    /** Introduction rule that creates a term of type SelectMany in the case where there is only
    * one single field named FN (as a singleton type) with type FT to select in S through path P.
    *
    * @param getField Selects implicitly the field and computes its type.
    *
    * In this case, the term is a function using the implicit parameter getField to select the field in the data,
    * and using the result to create a new tuple with only one element.
    */
    implicit def only_one_path[S <: HList, P, FN, FT](
        implicit
        getField: SelectField.Aux[S, P, FN, FT]
    ) = inhabit_type[S, P::HNil, FieldType[FN,FT]::HNil](
        (d: S) => getField(d) :: HNil
    )

    /** Introduction rule that creates a term of type SelectMany in the case where there is
    * one field named FN (as a singleton type) with type FT to select in S through path FP,
    * but also other fields forming a new schema OF to select in S through other paths OP.
    *
    * @param getField Selects implicitly the first field and computes its type.
    * @param getOthers Selects implicitly all the other fields and computes a new schema with them.
    *
    * In this case, the term is a function using the implicit parameter getField to select the first field in the data,
    * then the parameter getOther to select all the other fields and create a new tuple with them,
    * and finally merging all the different fields into one final tuple.
    */
    implicit def multiple_paths[S <: HList, FP, OP <: HList, FN, FT, OF <: HList](
        implicit
        getField: SelectField.Aux[S, FP, FN, FT],
        getOthers: SelectMany.Aux[S, OP, OF]
    ) = inhabit_type[S, FP::OP, FieldType[FN,FT]::OF](
        (d: S) => getField(d) :: getOthers(d)
    )

    trait As[Schema <: HList, Paths <: HList] { type Out <: HList ; def apply(data: Schema): Out }
    object As {
        type Aux ()
    }
}
```



#### Error stacktrace:

```
dotty.tools.dotc.core.SymDenotations$NoDenotation$.owner(SymDenotations.scala:2607)
	dotty.tools.dotc.core.SymDenotations$SymDenotation.isSelfSym(SymDenotations.scala:714)
	dotty.tools.dotc.semanticdb.ExtractSemanticDB$Extractor.traverse(ExtractSemanticDB.scala:160)
	dotty.tools.dotc.ast.Trees$Instance$TreeTraverser.apply(Trees.scala:1767)
	dotty.tools.dotc.ast.Trees$Instance$TreeTraverser.apply(Trees.scala:1767)
	dotty.tools.dotc.ast.Trees$Instance$TreeAccumulator.fold$1(Trees.scala:1633)
	dotty.tools.dotc.ast.Trees$Instance$TreeAccumulator.apply(Trees.scala:1635)
	dotty.tools.dotc.ast.Trees$Instance$TreeAccumulator.foldOver(Trees.scala:1701)
	dotty.tools.dotc.ast.Trees$Instance$TreeTraverser.traverseChildren(Trees.scala:1768)
	dotty.tools.dotc.semanticdb.ExtractSemanticDB$Extractor.traverse(ExtractSemanticDB.scala:281)
	dotty.tools.dotc.ast.Trees$Instance$TreeTraverser.apply(Trees.scala:1767)
	dotty.tools.dotc.ast.Trees$Instance$TreeTraverser.apply(Trees.scala:1767)
	dotty.tools.dotc.ast.Trees$Instance$TreeAccumulator.foldOver(Trees.scala:1725)
	dotty.tools.dotc.ast.Trees$Instance$TreeTraverser.traverseChildren(Trees.scala:1768)
	dotty.tools.dotc.semanticdb.ExtractSemanticDB$Extractor.traverse(ExtractSemanticDB.scala:181)
	dotty.tools.dotc.semanticdb.ExtractSemanticDB$Extractor.traverse$$anonfun$11(ExtractSemanticDB.scala:207)
	scala.runtime.function.JProcedure1.apply(JProcedure1.java:15)
	scala.runtime.function.JProcedure1.apply(JProcedure1.java:10)
	scala.collection.immutable.List.foreach(List.scala:333)
	dotty.tools.dotc.semanticdb.ExtractSemanticDB$Extractor.traverse(ExtractSemanticDB.scala:207)
	dotty.tools.dotc.ast.Trees$Instance$TreeTraverser.apply(Trees.scala:1767)
	dotty.tools.dotc.ast.Trees$Instance$TreeTraverser.apply(Trees.scala:1767)
	dotty.tools.dotc.ast.Trees$Instance$TreeAccumulator.foldOver(Trees.scala:1725)
	dotty.tools.dotc.ast.Trees$Instance$TreeTraverser.traverseChildren(Trees.scala:1768)
	dotty.tools.dotc.semanticdb.ExtractSemanticDB$Extractor.traverse(ExtractSemanticDB.scala:181)
	dotty.tools.dotc.semanticdb.ExtractSemanticDB$Extractor.traverse$$anonfun$11(ExtractSemanticDB.scala:207)
	scala.runtime.function.JProcedure1.apply(JProcedure1.java:15)
	scala.runtime.function.JProcedure1.apply(JProcedure1.java:10)
	scala.collection.immutable.List.foreach(List.scala:333)
	dotty.tools.dotc.semanticdb.ExtractSemanticDB$Extractor.traverse(ExtractSemanticDB.scala:207)
	dotty.tools.dotc.ast.Trees$Instance$TreeTraverser.apply(Trees.scala:1767)
	dotty.tools.dotc.ast.Trees$Instance$TreeTraverser.apply(Trees.scala:1767)
	dotty.tools.dotc.ast.Trees$Instance$TreeAccumulator.foldOver(Trees.scala:1725)
	dotty.tools.dotc.ast.Trees$Instance$TreeAccumulator.foldOver(Trees.scala:1639)
	dotty.tools.dotc.ast.Trees$Instance$TreeTraverser.traverseChildren(Trees.scala:1768)
	dotty.tools.dotc.semanticdb.ExtractSemanticDB$Extractor.traverse(ExtractSemanticDB.scala:181)
	dotty.tools.dotc.semanticdb.ExtractSemanticDB$Extractor.traverse$$anonfun$1(ExtractSemanticDB.scala:145)
	scala.runtime.function.JProcedure1.apply(JProcedure1.java:15)
	scala.runtime.function.JProcedure1.apply(JProcedure1.java:10)
	scala.collection.immutable.List.foreach(List.scala:333)
	dotty.tools.dotc.semanticdb.ExtractSemanticDB$Extractor.traverse(ExtractSemanticDB.scala:145)
	scala.meta.internal.pc.SemanticdbTextDocumentProvider.textDocument(SemanticdbTextDocumentProvider.scala:39)
	scala.meta.internal.pc.ScalaPresentationCompiler.semanticdbTextDocument$$anonfun$1(ScalaPresentationCompiler.scala:217)
```
#### Short summary: 

java.lang.AssertionError: NoDenotation.owner