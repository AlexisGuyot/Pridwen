package pridwen.types.opschema

import scala.reflect.runtime.universe.TypeTag

import scala.annotation.implicitNotFound

import shapeless.{HList, HNil, ::, Witness}
import shapeless.labelled.{FieldType}

object Schema {
    /** Creates a string describing the fields of a schema expressed as a type.
    *
    * @tparam Schema The schema to describe.
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
    * apply returns :
    * - a:
        - b:
          - c: String
          - d: Int
        - e: String
      - f: Int
    */
    @implicitNotFound("[Pridwen / Schema.AsString] Impossible to create a string describing the fields of schema ${Schema}.") 
    trait AsString[Schema <: HList] { def apply(prefix: String = ""): String }

    object AsString {
        val tab = "  "

        implicit def empty_schema: AsString[HNil] = new AsString[HNil]{ def apply(prefix: String = ""): String = "" }
        implicit def multiple_fields[F, OF <: HList](
            implicit
            sprintField: FieldAsString[F],
            sprintOthers: AsString[OF]
        ): AsString[F::OF] = new AsString[F::OF]{ def apply(prefix: String = ""): String = s"${prefix}${sprintField(prefix)}${sprintOthers(prefix)}" }

        private trait FieldAsString[F] { def apply(prefix: String = ""): String }
        private trait FieldAsString1 {
            implicit def flat_field[FN <: Symbol, FT](
                implicit
                fieldName: Witness.Aux[FN],
                fieldType: TypeTag[FT]
            ): FieldAsString[FieldType[FN, FT]] = new FieldAsString[FieldType[FN, FT]] {
                def apply(prefix: String = ""): String = s"${prefix}- ${fieldName.value.name}: ${fieldType.tpe}\n"
            }
        }
        private object FieldAsString extends FieldAsString1 {
            implicit def nested_field[FN <: Symbol, FT <: HList](
                implicit
                fieldName: Witness.Aux[FN],
                sprintFT: AsString[FT]
            ): FieldAsString[FieldType[FN, FT]] = new FieldAsString[FieldType[FN, FT]] {
                def apply(prefix: String = ""): String = s"${prefix}- ${fieldName.value.name}:\n${sprintFT(prefix + tab)}"
            }
        }
    }
}