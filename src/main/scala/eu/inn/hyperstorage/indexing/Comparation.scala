package eu.inn.parser.ast

import eu.inn.binders.{value ⇒ bn}
import eu.inn.parser.HEval
import eu.inn.parser.eval.EvaluatorEngine

object AstComparation extends Enumeration {
  type AstComparation = Value
  val NotEqual, Equal, Wider = Value
}

object AstComparator {
  import AstComparation._
  val pureEval = new HEval()

  def compare(a: Expression, b: Expression): AstComparation.Value = {
    a match {
      case UnaryOperation(opA, argA) ⇒
        b match {
          case UnaryOperation(opB, argB) if opA == opB ⇒
            compare(argA, argB)
          case _ ⇒
            NotEqual
        }

      case bopA @ BinaryOperation(leftA, opA, rightA) ⇒
        b match {
          case bopB @ BinaryOperation(leftB, opB, rightB) ⇒
            compareBinaryOperations(bopA, bopB)

          case _ ⇒
            compareBinaryOperationWithExpression(bopA, b)
        }

      case Function(nameA, argumentsA) ⇒
        b match {
          case Function(nameB, argumentsB) if nameA == nameB && argumentsA.size == argumentsB.size ⇒
            aggregate(argumentsA zip argumentsB map(p ⇒ compare(p._1, p._2)))
          case _ ⇒
            NotEqual
        }

      case _ if a == b ⇒ Equal
      case _ ⇒ b match {
        case bopB@BinaryOperation(leftB, opB, rightB) ⇒
          compareExpressionWithBinaryOperation(a, bopB)

        case _ ⇒ NotEqual
      }
    }
  }

  private def aggregate(results: Seq[AstComparation.Value]): AstComparation.Value = {
    results.foldLeft(Equal) { case (result, right) ⇒
      result match {
        case Equal ⇒ right match {
          case Equal ⇒ Equal
          case Wider ⇒ Wider
          case NotEqual ⇒ NotEqual
        }

        case Wider ⇒ right match {
          case Equal ⇒ Wider
          case Wider ⇒ Wider
          case NotEqual ⇒ NotEqual
        }

        case NotEqual ⇒ NotEqual
      }
    }
  }

  private def compareBinaryOperations(bopA: BinaryOperation, bopB: BinaryOperation): AstComparation.Value = {
    if (bopA.op.segments.tail.isEmpty && bopB.op.segments.tail.isEmpty) {
      if (bopA == bopB)
        Equal
      else {
        val a = bopA.op.segments.head
        val b = bopB.op.segments.head

        a match {
          case ">" if b == ">" ⇒ compareBOp(bopA,bopB,(ac,bc) ⇒ bc > ac)
          case ">=" if b == ">=" ⇒ compareBOp(bopA,bopB,(ac,bc) ⇒ bc >= ac)
          case "<" if b == "<" ⇒ compareBOp(bopB, bopA,(ac,bc) ⇒ bc > ac)
          case "<=" if b == "<=" ⇒ compareBOp(bopB, bopA,(ac,bc) ⇒ bc >= ac)
          case "has" if b == "has" ⇒ compareBOp(bopA, bopB,(ac,bc) ⇒ EvaluatorEngine.hasBop(ac,bc))
          case "has not" if b == "has not" ⇒ compareBOp(bopA, bopB,(ac,bc) ⇒ EvaluatorEngine.hasBop(bc,ac))
          case _ ⇒
            compareBinaryOperationWithExpression(bopA, bopB) match {
              case NotEqual ⇒  compareExpressionWithBinaryOperation(bopA, bopB)
              case other ⇒ other
            }
        }
      }
    } else {
      NotEqual
    }
  }

  private def compareBOp(a:BinaryOperation, b:BinaryOperation, op: (bn.Value,bn.Value) ⇒ Boolean): AstComparation.Value = {
    aggregate(Seq(
      comparePureOp(a.rightArgument,b.rightArgument, op),
      comparePureOp(a.leftArgument, b.leftArgument, op)
    ))
  }

  private def comparePureOp(a:Expression, b:Expression, op: (bn.Value,bn.Value) ⇒ Boolean): AstComparation.Value = {
    if (isConstantExpression(a) && isConstantExpression(b)) {
      val ac = pureEval.eval(a)
      val bc = pureEval.eval(b)
      if (ac == bc) {
        Equal
      }
      else if (op(ac,bc)) {
        Wider
      }
      else {
        NotEqual
      }
    }
    else {
      compare(a,b) match {
        case Wider ⇒ NotEqual // a and be are exchanged, so it's not possible to sure about subset here
        case other ⇒ other
      }
    }
  }
  private def isConstantExpression(expression: Expression): Boolean = {
    expression match {
      case _: Constant ⇒ true
      case _: Identifier ⇒ false
      case UnaryOperation(_, argument) ⇒ isConstantExpression(argument)
      case BinaryOperation(left, _, right) ⇒ isConstantExpression(left) && isConstantExpression(right)
      case Function(_, args) ⇒ args.forall(isConstantExpression)
    }
  }

  private def compareBinaryOperationWithExpression(a: BinaryOperation, b: Expression): AstComparation.Value = {
    if (a.op.segments.tail.isEmpty && a.op.segments.head == "or") {
      compare(a.leftArgument, b) match {
        case Equal ⇒ Wider
        case Wider ⇒ Wider
        case NotEqual ⇒ compare(a.rightArgument, b) match {
          case Equal ⇒ Wider
          case other ⇒ other
        }
      }
    }
    else {
      NotEqual
    }
  }

  private def compareExpressionWithBinaryOperation(a: Expression, b: BinaryOperation): AstComparation.Value = {
    if (b.op.segments.tail.isEmpty && b.op.segments.head == "and") {
      compare(a, b.leftArgument) match {
        case Equal ⇒ Wider
        case Wider ⇒ Wider
        case NotEqual ⇒ compare(a, b.rightArgument) match {
          case Equal ⇒ Wider
          case other ⇒ other
        }
      }
    }
    else {
      NotEqual
    }
  }
}
