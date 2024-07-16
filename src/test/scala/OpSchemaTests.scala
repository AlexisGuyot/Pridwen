import org.scalatest.flatspec._
import org.scalatest.matchers.should.Matchers._

import shapeless.{HNil, ::, Witness}
import shapeless.labelled.{FieldType, field}

import pridwen.types.opschema._

class OpSchemaTests extends AnyFlatSpec {
    val a = Witness('a) ; val aa = Witness('aa) ; val ab = Witness('ab) 
    val b = Witness('b) ; val ba = Witness('ba) ; val bb = Witness('bb) ; val bba = Witness('bba)
    val c = Witness('c)
    val _a = Witness('_a) ; val _b = Witness('_b)

    type UnnestedSchema = FieldType[_a.T, Int] :: FieldType[_b.T, String] :: HNil
    type NestedSchema = FieldType[a.T, 
            FieldType[aa.T, String] :: 
            FieldType[ab.T, Int] :: 
            HNil] :: 
        FieldType[b.T, 
            FieldType[ba.T, String] :: 
            FieldType[bb.T, 
                FieldType[bba.T, Boolean] :: 
                HNil] :: 
            HNil] :: 
        FieldType[c.T, List[Boolean]] ::
        HNil
    val emptySchema: HNil = HNil
    val unnestedSchema: UnnestedSchema = field[_a.T](0) :: field[_b.T]("") :: HNil 
    val nestedSchema: NestedSchema = 
        field[a.T](field[aa.T]("a") :: field[ab.T](0) :: HNil) :: 
        field[b.T](field[ba.T]("b") :: field[bb.T](field[bba.T](true) :: HNil) :: HNil) :: 
        field[c.T](List()) :: HNil 
    
    /* ------ Tests with FollowPath ------*/
    
    "FollowPath" should "compile if the schema includes all the field of the path" in {
        implicitly[FollowPath.Aux[FieldType[a.T, String] :: HNil, a.T :: HNil, FieldType[a.T, String] :: HNil]]
        implicitly[FollowPath.Aux[nestedSchema.type, b.T :: bb.T :: bba.T :: HNil, FieldType[bba.T, Boolean] :: HNil]]
    }

    it should "not compile if an element of the path does not exist in the schema" in {
        "implicitly[FollowPath[FieldType[a.T, String] :: HNil, b.T :: HNil]]" shouldNot typeCheck
    }

    it should "compile if the path is empty and should compute the whole schema" in {
        implicitly[FollowPath.Aux[FieldType[a.T, String] :: HNil, HNil, FieldType[a.T, String] :: HNil]]
    }

    it should "compile if the path is a field name instead of a HList" in {
        implicitly[FollowPath.Aux[FieldType[a.T, String] :: HNil, a.T, FieldType[a.T, String] :: HNil]]
    }

    /* ------ Tests with SelectField ------*/

    "SelectField" should "compile if the last element of the path can be selected in the corresponding sub-path" in {
        implicitly[SelectField.Aux[nestedSchema.type, b.T :: bb.T :: bba.T :: HNil, bba.T, Boolean]]
        implicitly[SelectField.Aux[nestedSchema.type, c.T :: HNil, c.T, List[Boolean]]]
    }

    it should "not compile if the last element of the path does not exist in the corresponding sub-path" in {
        "implicitly[SelectField[nestedSchema.type, b.T :: bb.T :: c.T :: HNil]]" shouldNot typeCheck
    }

    it should "not compile if the path is erroneous" in {
        "implicitly[SelectField[nestedSchema.type, b.T :: c.T :: bba.T :: HNil]]" shouldNot typeCheck
    }

    it should "not compile if the schema is empty" in {
        "implicitly[SelectField[HNil, b.T :: c.T :: bba.T :: HNil]]" shouldNot typeCheck
        "implicitly[SelectField[HNil, HNil]]" shouldNot typeCheck
    }

    it should "not compile if the path is empty" in {
        "implicitly[SelectField[nestedSchema.type, HNil]]" shouldNot typeCheck
    }

    it should "compile if the path is a field name instead of a HList" in {
        implicitly[SelectField.Aux[FieldType[a.T, String] :: HNil, a.T, a.T, String]]
    }

    "SelectField.As" should "compile when SelectField compiles" in {
        implicitly[SelectField.As.Aux[nestedSchema.type, b.T :: bb.T :: bba.T :: HNil, c.T, Boolean]]
        implicitly[SelectField.As.Aux[nestedSchema.type, c.T :: HNil, a.T, List[Boolean]]]
        implicitly[SelectField.As.Aux[FieldType[a.T, String] :: HNil, a.T, c.T, String]]
    }

    it should "not compile when SelectField does not compile" in {
        "implicitly[SelectField.As[nestedSchema.type, b.T :: bb.T :: c.T :: HNil, a.T]]" shouldNot typeCheck
        "implicitly[SelectField.As[nestedSchema.type, b.T :: c.T :: bba.T :: HNil, a.T]]" shouldNot typeCheck
        "implicitly[SelectField.As[HNil, b.T :: c.T :: bba.T :: HNil, a.T]]" shouldNot typeCheck
        "implicitly[SelectField.As[HNil, HNil, a.T]]" shouldNot typeCheck
        "implicitly[SelectField.As[nestedSchema.type, HNil, a.T]]" shouldNot typeCheck
    }

    it should "not compile if the new field name is not a symbol" in {
        "implicitly[SelectField.As[nestedSchema.type, c.T :: HNil, String]]" shouldNot typeCheck
    }

    /* ------ Tests with SelectMany ------*/

    "SelectMany" should "compile if there is only one field to select" in {
        implicitly[SelectMany.Aux[
            nestedSchema.type, 
            (c.T :: HNil) :: HNil, 
            FieldType[c.T, List[Boolean]] :: HNil
        ]]
    }

    it should "compile if there are multiple fields to select" in {
        implicitly[SelectMany.Aux[
            nestedSchema.type, 
            (c.T :: HNil) :: (b.T :: bb.T :: bba.T :: HNil) :: HNil, 
            FieldType[c.T, List[Boolean]] :: FieldType[bba.T, Boolean] :: HNil
        ]]
    }

    it should "compile if one of the path is just a field name" in {
        implicitly[SelectMany.Aux[
            nestedSchema.type, 
            c.T :: (b.T :: bb.T :: bba.T :: HNil) :: HNil, 
            FieldType[c.T, List[Boolean]] :: FieldType[bba.T, Boolean] :: HNil
        ]]
    }

    it should "not compile if there is no field to select" in {
        "implicitly[SelectMany[nestedSchema.type, HNil]]" shouldNot typeCheck
    }

    it should "not compile if one of the selection is erroneous" in {
        """implicitly[SelectMany[
            nestedSchema.type, 
            (c.T :: HNil) :: (b.T :: c.T :: bba.T :: HNil) :: HNil
        ]]""" shouldNot typeCheck
    }

    "SelectMany.As" should "compile when SelectMany compiles" in {
        implicitly[SelectMany.As.Aux[
            nestedSchema.type, 
            (c.T :: HNil, a.T) :: HNil, 
            FieldType[a.T, List[Boolean]] :: HNil
        ]]
        implicitly[SelectMany.As.Aux[
            nestedSchema.type, 
            (c.T :: HNil, a.T) :: (b.T :: bb.T :: bba.T :: HNil, c.T) :: HNil, 
            FieldType[a.T, List[Boolean]] :: FieldType[c.T, Boolean] :: HNil
        ]]
        implicitly[SelectMany.As.Aux[
            nestedSchema.type, 
            (c.T, a.T) :: (b.T :: bb.T :: bba.T :: HNil, c.T) :: HNil, 
            FieldType[a.T, List[Boolean]] :: FieldType[c.T, Boolean] :: HNil
        ]]
    }

    it should "not compile when SelectMany does not compile" in {
        "implicitly[SelectMany.As[nestedSchema.type, HNil]]" shouldNot typeCheck
        """implicitly[SelectMany.As[
            nestedSchema.type, 
            (c.T :: HNil, a.T) :: (b.T :: c.T :: bba.T :: HNil, c.T) :: HNil
        ]]""" shouldNot typeCheck
    }

    it should "not compile if one of the new field names is not a symbol" in {
        """implicitly[SelectMany.As.Aux[
            nestedSchema.type, 
            (c.T :: HNil, String) :: HNil, 
            FieldType[a.T, List[Boolean]] :: HNil
        ]]""" shouldNot typeCheck
    }
    
    /* ------ Tests with AddField ------*/

    "AddField" should "compile if the original schema is empty and should compute a new schema only containing the field" in {
        implicitly[AddField.Aux[HNil, HNil, a.T, String, FieldType[a.T, String] :: HNil]]
    }

    it should "compile if the original schema is not empty and should add the new field at the end of this schema" in {
        implicitly[AddField.Aux[FieldType[a.T, String] :: HNil, HNil, b.T, Int, FieldType[a.T, String] :: FieldType[b.T, Int] :: HNil]]
    }

    it should "compile if the path is not empty and should compute the right new nested schema" in {
        implicitly[AddField.Aux[NestedSchema, b.T :: HNil, _a.T, Float, FieldType[a.T, 
            FieldType[aa.T, String] :: 
            FieldType[ab.T, Int] :: 
            HNil] :: 
        FieldType[b.T, 
            FieldType[ba.T, String] :: 
            FieldType[bb.T, 
                FieldType[bba.T, Boolean] :: 
                HNil] :: 
            FieldType[_a.T, Float] :: 
            HNil] :: 
        FieldType[c.T, List[Boolean]] ::
        HNil]]
    }

    it should "compile if the path only contains one element and should compute the right new nested schema" in {
        implicitly[AddField.Aux[NestedSchema, b.T, _a.T, Float, FieldType[a.T, 
            FieldType[aa.T, String] :: 
            FieldType[ab.T, Int] :: 
            HNil] :: 
        FieldType[b.T, 
            FieldType[ba.T, String] :: 
            FieldType[bb.T, 
                FieldType[bba.T, Boolean] :: 
                HNil] :: 
            FieldType[_a.T, Float] :: 
            HNil] :: 
        FieldType[c.T, List[Boolean]] ::
        HNil]]
    }

    it should "not compile if the field at the end of the path is not a HList" in {
        "implicitly[AddField[NestedSchema, c.T :: HNil, _a.T, Float]]" shouldNot typeCheck
    }

    it should "not compile if the new field name is not a field name" in {
        "implicitly[AddField[NestedSchema, c.T :: HNil, String, Float]]" shouldNot typeCheck
    }

    /* ------ Tests with RemoveField ------*/

    "RemoveField" should "compile if the schema contains the field to remove" in {
        implicitly[RemoveField.Aux[NestedSchema, b.T :: bb.T :: HNil, FieldType[a.T, 
            FieldType[aa.T, String] :: 
            FieldType[ab.T, Int] :: 
            HNil] :: 
        FieldType[b.T, 
            FieldType[ba.T, String] :: 
            HNil] :: 
        FieldType[c.T, List[Boolean]] ::
        HNil]]
    }

    it should "not compile if the schema does not contain the field to remove" in {
        "implicitly[RemoveField[NestedSchema, _a.T]]" shouldNot typeCheck
    }

    it should "not compile if the schema is empty" in {
        "implicitly[RemoveField[HNil, _a.T]]" shouldNot typeCheck
    }

    it should "not compile if the path is erroneous" in {
        "implicitly[RemoveField[NestedSchema, b.T :: a.T :: HNil]]" shouldNot typeCheck
    }

    /* ------ Tests with ReplaceField ------*/

    "ReplaceField" should "compile if the schema contains the field to replace" in {
        implicitly[ReplaceField.Aux[NestedSchema, b.T :: bb.T :: HNil, bb.T, Float, FieldType[a.T, 
            FieldType[aa.T, String] :: 
            FieldType[ab.T, Int] :: 
            HNil] :: 
        FieldType[b.T, 
            FieldType[ba.T, String] :: 
            FieldType[bb.T, Float] :: 
            HNil] :: 
        FieldType[c.T, List[Boolean]] ::
        HNil]]
    }

    it should "not compile if the schema does not contain the field to remove" in {
        "implicitly[ReplaceField[NestedSchema, _a.T, bb.T, Float]]" shouldNot typeCheck
    }

    it should "not compile if the schema is empty" in {
        "implicitly[ReplaceField[HNil, _a.T, bb.T, Float]]" shouldNot typeCheck
    }

    it should "not compile if the path is erroneous" in {
        "implicitly[ReplaceField[NestedSchema, b.T :: a.T :: HNil, bb.T, Float]]" shouldNot typeCheck
    }
    
    /* ------ Tests with MergeSchema ------*/

    "MergeSchema" should "compile if the left schema is empty and should compute the right schema with all three merge modes" in {
        implicitly[MergeSchema.Aux[HNil, NestedSchema, HNil, HNil, NestedSchema]]
        implicitly[MergeSchema.InLeft[HNil, NestedSchema, HNil, HNil, NestedSchema]]
        implicitly[MergeSchema.InRight[HNil, NestedSchema, HNil, HNil, NestedSchema]]
    }

    it should "compile if the right schema is empty and should compute the left schema with all three merge modes" in {
        implicitly[MergeSchema.Aux[UnnestedSchema, HNil, HNil, HNil, UnnestedSchema]]
        implicitly[MergeSchema.InLeft[UnnestedSchema, HNil, HNil, HNil, UnnestedSchema]]
        implicitly[MergeSchema.InRight[UnnestedSchema, HNil, HNil, HNil, UnnestedSchema]]
    }

    it should "compile if both schemas are empty and should compute an empty schema with all three merge modes" in {
        implicitly[MergeSchema.Aux[HNil, HNil, HNil, HNil, HNil]]
        implicitly[MergeSchema.InLeft[HNil, HNil, HNil, HNil, HNil]]
        implicitly[MergeSchema.InRight[HNil, HNil, HNil, HNil, HNil]]
    }

    it should "compile if both schemas are not empty and should be able to compute a default merge" in {
        implicitly[MergeSchema.Aux[UnnestedSchema, NestedSchema, HNil, HNil, 
            FieldType[_a.T, Int] :: 
            FieldType[_b.T, String] :: 
            FieldType[a.T, 
                FieldType[aa.T, String] :: 
                FieldType[ab.T, Int] :: 
                HNil] :: 
            FieldType[b.T, 
                FieldType[ba.T, String] :: 
                FieldType[bb.T, 
                    FieldType[bba.T, Boolean] :: 
                    HNil] :: 
                HNil] :: 
            FieldType[c.T, List[Boolean]] ::
            HNil
        ]]
        implicitly[MergeSchema.Aux[UnnestedSchema, NestedSchema, HNil, b.T :: bb.T :: HNil, 
            FieldType[_a.T, Int] :: 
            FieldType[_b.T, String] :: 
            FieldType[bba.T, Boolean] :: 
            HNil
        ]]
        implicitly[MergeSchema.Aux[NestedSchema, NestedSchema, a.T, b.T :: bb.T :: HNil, 
            FieldType[aa.T, String] :: 
            FieldType[ab.T, Int] :: 
            FieldType[bba.T, Boolean] :: 
            HNil
        ]]
    }

    it should "compile if both schemas are not empty and should be able to compute a inLeft merge" in {
        implicitly[MergeSchema.InLeft[UnnestedSchema, NestedSchema, HNil, HNil, 
            FieldType[_a.T, Int] :: 
            FieldType[_b.T, String] :: 
            FieldType[a.T, 
                FieldType[aa.T, String] :: 
                FieldType[ab.T, Int] :: 
                HNil] :: 
            FieldType[b.T, 
                FieldType[ba.T, String] :: 
                FieldType[bb.T, 
                    FieldType[bba.T, Boolean] :: 
                    HNil] :: 
                HNil] :: 
            FieldType[c.T, List[Boolean]] ::
            HNil
        ]]
        implicitly[MergeSchema.InLeft[UnnestedSchema, NestedSchema, HNil, b.T :: bb.T :: HNil, 
            FieldType[_a.T, Int] :: 
            FieldType[_b.T, String] :: 
            FieldType[bba.T, Boolean] :: 
            HNil
        ]]
        implicitly[MergeSchema.InLeft[NestedSchema, NestedSchema, a.T, b.T :: bb.T :: HNil, 
            FieldType[a.T, 
                FieldType[aa.T, String] :: 
                FieldType[ab.T, Int] :: 
                FieldType[bba.T, Boolean] :: 
                HNil] :: 
            FieldType[b.T, 
                FieldType[ba.T, String] :: 
                FieldType[bb.T, 
                    FieldType[bba.T, Boolean] :: 
                    HNil] :: 
                HNil] :: 
            FieldType[c.T, List[Boolean]] ::
            HNil
        ]]
    }

    it should "compile if both schemas are not empty and should be able to compute a inRight merge" in {
        implicitly[MergeSchema.InRight[UnnestedSchema, NestedSchema, HNil, HNil, 
            FieldType[a.T, 
                FieldType[aa.T, String] :: 
                FieldType[ab.T, Int] :: 
                HNil] :: 
            FieldType[b.T, 
                FieldType[ba.T, String] :: 
                FieldType[bb.T, 
                    FieldType[bba.T, Boolean] :: 
                    HNil] :: 
                HNil] :: 
            FieldType[c.T, List[Boolean]] ::
            FieldType[_a.T, Int] :: 
            FieldType[_b.T, String] :: 
            HNil
        ]]
        implicitly[MergeSchema.InRight[UnnestedSchema, NestedSchema, HNil, b.T :: bb.T :: HNil, 
            FieldType[a.T, 
                FieldType[aa.T, String] :: 
                FieldType[ab.T, Int] :: 
                HNil] :: 
            FieldType[b.T, 
                FieldType[ba.T, String] :: 
                FieldType[bb.T, 
                    FieldType[bba.T, Boolean] :: 
                    FieldType[_a.T, Int] :: 
                    FieldType[_b.T, String] :: 
                    HNil] :: 
                HNil] :: 
            FieldType[c.T, List[Boolean]] ::
            HNil
        ]]
        implicitly[MergeSchema.InRight[NestedSchema, NestedSchema, a.T, b.T :: bb.T :: HNil, 
            FieldType[a.T, 
                FieldType[aa.T, String] :: 
                FieldType[ab.T, Int] :: 
                HNil] :: 
            FieldType[b.T, 
                FieldType[ba.T, String] :: 
                FieldType[bb.T, 
                    FieldType[bba.T, Boolean] :: 
                    FieldType[aa.T, String] :: 
                    FieldType[ab.T, Int] :: 
                    HNil] :: 
                HNil] :: 
            FieldType[c.T, List[Boolean]] ::
            HNil
        ]]
    }

    /* ------ Tests with AsString ------*/

    "Schema.AsString" should "compile and compute a string describing the schema of data" in {
        val sprintHNil = implicitly[Schema.AsString[HNil]] ; println(sprintHNil()) ; assert(sprintHNil() == "")
        val sprintUnnested = implicitly[Schema.AsString[UnnestedSchema]] ; println(sprintUnnested()) ; assert(sprintUnnested() == s"- _a: Int\n- _b: String\n")
        val sprintNested = implicitly[Schema.AsString[NestedSchema]] ; println(sprintNested())
    }
}