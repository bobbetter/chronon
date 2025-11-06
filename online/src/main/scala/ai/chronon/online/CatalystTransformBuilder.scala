package ai.chronon.online

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.sql.catalyst.expressions.{
  Attribute,
  AttributeSet,
  BindReferences,
  Expression,
  Generator,
  GenericInternalRow,
  JoinedRow,
  Nondeterministic,
  Predicate,
  UnsafeProjection
}
import org.apache.spark.sql.execution.{
  BufferedRowIterator,
  FilterExec,
  GenerateExec,
  InputAdapter,
  LocalTableScanExec,
  ProjectExec,
  RDDScanExec,
  WholeStageCodegenExec
}
import org.apache.spark.sql.internal.SQLConf
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object CatalystTransformBuilder {

  @transient private lazy val logger = LoggerFactory.getLogger(this.getClass)

  private class IteratorWrapper[T] extends Iterator[T] {
    def put(elem: T): Unit = elemArr.enqueue(elem)

    override def hasNext: Boolean = elemArr.nonEmpty

    override def next(): T = elemArr.dequeue()

    private val elemArr: mutable.Queue[T] = mutable.Queue.empty[T]
  }

  /** Recursively builds a chain of transformation functions from a SparkPlan
    */
  def buildTransformChain(plan: org.apache.spark.sql.execution.SparkPlan): InternalRow => Seq[InternalRow] = {
    logger.info(s"Building transform chain for plan: ${plan.getClass.getSimpleName}")

    // Helper function to inspect plan structures
    def describePlan(plan: org.apache.spark.sql.execution.SparkPlan, depth: Int = 0): String = {
      val indent = "  " * depth
      val childrenDesc = plan.children.map(c => describePlan(c, depth + 1)).mkString("\n")
      s"${indent}${plan.getClass.getSimpleName}: ${plan.output.map(_.name).mkString(", ")}\n${childrenDesc}"
    }

    // Log detailed plan structure for complex plans
    if (plan.children.size > 1 || plan.isInstanceOf[WholeStageCodegenExec]) {
      logger.info(s"Detailed plan structure:\n${describePlan(plan)}")
    }

    plan match {
      case whc: WholeStageCodegenExec =>
        logger.info(s"WholeStageCodegenExec child plan: ${whc.child}")

        // Check for tooManyFields issue and emit a more helpful diagnostic
        if (WholeStageCodegenExec.isTooManyFields(SQLConf.get, whc.child.schema)) {
          logger.warn("WholeStageCodegenExec has too many fields which may lead to code generation issues")
          logger.warn(s"Schema has ${whc.child.schema.size} fields, max is ${SQLConf.get.wholeStageMaxNumFields}")
        }

        // First check if the WholeStageCodegenExec has InputAdapter in its plan tree
        // If so, we need to handle the stages separately
        if (containsInputAdapter(whc)) {
          logger.info("WholeStageCodegenExec contains InputAdapter nodes - processing as cascading stages")

          // Process the child plan, which will handle the InputAdapter recursively
          // This is the critical step that implements proper cascading codegen
          val childTransformer = buildTransformChain(whc.child)

          // Return the child transformer directly - the cascading will happen
          // through the InputAdapter case which will process the next stage
          childTransformer
        } else {
          // If no InputAdapter is found, this is a single WholeStageCodegenExec
          // that we can process with the extracted code
          try {
            logger.info("Processing WholeStageCodegenExec as a single stage")
            extractCodegenStageTransformer(whc)
          } catch {
            case e: Exception =>
              // If codegen fails, fall back to processing the child plans without codegen
              logger.warn(s"Failed to use WholeStageCodegenExec, falling back to child plan execution: ${e.getMessage}")
              logger.info("Building transform chain for child plan instead")

              // Recursively build a transform chain from the child plans
              buildTransformChain(whc.child)
          }
        }

      case project: ProjectExec =>
        logger.info(s"Processing ProjectExec with expressions: ${project.projectList}")

        project.child match {
          // Special handling for direct RDD scans - no need to process through child
          case _: RDDScanExec | _: LocalTableScanExec =>
            // When the child is a simple scan, we can directly apply the projection
            extractProjectTransformer(project)

          // Special handling when child is InputAdapter
          case inputAdapter: InputAdapter =>
            logger.info("ProjectExec has an InputAdapter child - using special handling")

            // Get the child transformer
            val childTransformer = buildTransformChain(project.child)
            val proj = UnsafeProjection.create(project.projectList, project.child.output)
            // Apply the project to each generated row independently
            row => {
              // Get rows from the generate transformer
              val childRows = childTransformer(row)

              // Apply the projection to each row individually with memory isolation
              val safeRows = childRows.zipWithIndex.map { case (childRow, idx) =>
                // Create a specialized projection for each row

                val projected = proj(childRow)

                // Create a deep copy of the projected row
                val safeRow = new GenericInternalRow(project.output.size)
                for (i <- project.output.indices) {
                  try {
                    val dataType = project.output(i).dataType
                    val value = projected.get(i, dataType)
                    safeRow.update(i, value)
                  } catch {
                    case e: Exception =>
                      logger.error(s"Error copying field ${project.output(i).name}: ${e.getMessage}")
                  }
                }

                safeRow
              }

              safeRows
            }

          // Special handling for WholeStageCodegenExec child - we need to be careful about schema alignment
          case whc: WholeStageCodegenExec =>
            try {
              // Try to use both the WholeStageCodegenExec and then the projection
              val codegenTransformer = buildTransformChain(whc)
              val projectTransformer = extractProjectTransformer(project)

              row => {
                val intermediateRows = codegenTransformer(row)
                intermediateRows.flatMap(projectTransformer)
              }
            } catch {
              case e: Exception =>
                logger.error(s"Error processing ProjectExec with WholeStageCodegenExec child: ", e)
                throw e
            }

          case _ =>
            // For complex children, we need to chain the transformations
            val childTransformer = buildTransformChain(project.child)
            val projectTransformer = extractProjectTransformer(project)

            row => {
              val intermediateRows = childTransformer(row)
              intermediateRows.flatMap(projectTransformer)
            }
        }

      case filter: FilterExec =>
        logger.info(s"Processing FilterExec with condition: ${filter.condition}")

        // For a filter, first process the child and then apply filter
        val childTransformer = buildTransformChain(filter.child)
        val filterTransformer = extractFilterTransformer(filter)

        row => childTransformer(row).flatMap(filterTransformer)

      case input: InputAdapter =>
        logger.info(
          s"Processing InputAdapter with child: ${input.child.getClass.getSimpleName}. " +
            s"This is a split point between codegen stages")

        // InputAdapter is a boundary between codegen regions
        // We need to recursively process its child, which might be another WholeStageCodegenExec
        val childTransformer = buildTransformChain(input.child)

        // Special handling when the child is a GenerateExec
        if (input.child.isInstanceOf[GenerateExec]) {
          logger.info("InputAdapter has a GenerateExec child - using special handling to ensure row memory isolation")

          // Return a function that carefully preserves the independence of rows
          row => {
            // Get rows from the child transformer
            val childRows = childTransformer(row)

            // Create deep copies of each row to ensure memory isolation
            val safeRows = childRows.zipWithIndex.map { case (childRow, idx) =>
              // Create a new row with copied values
              val safeRow = new GenericInternalRow(input.output.size)

              // Copy all fields from the child row
              for (i <- input.output.indices) {
                try {
                  val dataType = input.output(i).dataType
                  val value = childRow.get(i, dataType)
                  safeRow.update(i, value)
                } catch {
                  case e: Exception =>
                    logger.error(s"Error copying field ${input.output(i).name}: ${e.getMessage}")
                }
              }

              safeRow
            }

            safeRows
          }
        } else {
          // Standard handling for other cases
          row =>
            {
              val childRows = childTransformer(row)
              childRows
            }
        }

      case ltse: LocalTableScanExec =>
        logger.info(s"Processing LocalTableScanExec with schema: ${ltse.schema}")

        // Input row is unused for LocalTableScanExec
        _ => ArrayBuffer(ltse.executeCollect(): _*).toSeq

      case rddse: RDDScanExec =>
        logger.info(s"Processing RDDScanExec with schema: ${rddse.schema}")

        val unsafeProjection = UnsafeProjection.create(rddse.schema)
        row => Seq(unsafeProjection.apply(row))

      case generateExec: GenerateExec =>
        logger.info(s"Processing GenerateExec with generator: ${generateExec.generator}")
        // Get transformer for the child plan
        val childTransformer = buildTransformChain(generateExec.child)

        // Get transformer for the generate operation
        val generateTransformer = extractGenerateTransformer(generateExec)
        val generateOutput = generateExec.output
        // Chain them together
        row => {
          val intermediateRows = childTransformer(row)

          val results = intermediateRows.flatMap { ir =>
            // Get the generated rows
            val genRows = generateTransformer(ir)

            // Create deep copies of each row to prevent memory reuse
            val safeRows = genRows.zipWithIndex.map { case (genRow, idx) =>
              // Create a new row with copied values
              val safeRow = new GenericInternalRow(generateOutput.size)

              // Copy all fields from the generator row
              for (i <- generateOutput.indices) {
                try {
                  val dataType = generateOutput(i).dataType
                  val value = genRow.get(i, dataType)
                  safeRow.update(i, value)
                } catch {
                  case e: Exception =>
                    logger.error(s"Error copying field ${generateOutput(i).name}: ${e.getMessage}")
                }
              }

              // Create an UnsafeRow copy to ensure memory isolation
              val finalRow = new GenericInternalRow(safeRow.numFields)
              for (i <- 0 until safeRow.numFields) {
                val dataType = generateOutput(i).dataType
                val value = safeRow.get(i, dataType)
                finalRow.update(i, value)
              }

              finalRow
            }

            safeRows
          }
          results
        }

      case unsupported =>
        logger.warn(s"Unrecognized plan node: ${unsupported.getClass.getName}")
        throw new RuntimeException(s"Unrecognized stage in codegen: ${unsupported.getClass}")
    }
  }

  /** Extracts a transformation function from WholeStageCodegenExec
    * This method only handles the code generation part - the fallback to
    * child plans is handled in buildTransformChain
    */
  private def extractCodegenStageTransformer(whc: WholeStageCodegenExec): InternalRow => Seq[InternalRow] = {
    logger.info(s"Extracting codegen stage transformer for: ${whc}")

    // Generate and compile the code
    val (ctx, cleanedSource) = whc.doCodeGen()

    // Log a snippet of the generated code for debugging
    val codeSnippet = cleanedSource.body.split("\n").take(20).mkString("\n")
    logger.debug(s"Generated code snippet: \n$codeSnippet\n...")

    val (clazz, compilationTime) = CodeGenerator.compile(cleanedSource)
    logger.info(s"Compiled code in ${compilationTime}ms")

    val references = ctx.references.toArray
    val buffer = clazz.generate(references).asInstanceOf[BufferedRowIterator]
    val iteratorWrapper: IteratorWrapper[InternalRow] = new IteratorWrapper[InternalRow]
    buffer.init(0, Array(iteratorWrapper))

    def codegenFunc(row: InternalRow): Seq[InternalRow] = {
      iteratorWrapper.put(row)
      val result = ArrayBuffer.empty[InternalRow]
      while (buffer.hasNext) {
        result.append(buffer.next())
      }
      result.toSeq
    }

    codegenFunc
  }

  private def extractProjectTransformer(project: ProjectExec): InternalRow => Seq[InternalRow] = {
    // Use project.child.output as input schema instead of project.output
    // This ensures expressions like int32s#8 can be properly resolved
    val unsafeProjection = UnsafeProjection.create(project.projectList, project.child.output)

    row => Seq(unsafeProjection.apply(row))
  }

  private def extractFilterTransformer(filter: FilterExec): InternalRow => Seq[InternalRow] = {
    val predicate = Predicate.create(filter.condition, filter.child.output)
    predicate.initialize(0)
    val func = { row: InternalRow =>
      val passed = predicate.eval(row)
      if (passed) Seq(row) else Seq.empty
    }
    func
  }

  private def extractGenerateTransformer(generate: GenerateExec): InternalRow => Seq[InternalRow] = {
    logger.info(s"Extracting transformer for GenerateExec with generator: ${generate.generator}")

    // Create a bound generator
    val boundGenerator = BindReferences
      .bindReference(
        generate.generator.asInstanceOf[Expression],
        generate.child.output
      )
      .asInstanceOf[Generator]

    // Initialize any nondeterministic expressions
    boundGenerator match {
      case n: Nondeterministic => n.initialize(0)
      case _                   => // No initialization needed
    }

    // Create a null row for outer join case
    val generatorNullRow = new GenericInternalRow(boundGenerator.elementSchema.length)

    val needsPruning = generate.child.outputSet != AttributeSet(generate.requiredChildOutput)
    lazy val pruneChildForResult: InternalRow => InternalRow = if (needsPruning) {
      UnsafeProjection.create(generate.requiredChildOutput, generate.child.output)
    } else {
      identity
    }

    // Return the transformer function
    row => {
      try {
        if (generate.requiredChildOutput.nonEmpty) {
          extractGenerateNonEmptyChildren(generate, boundGenerator, generatorNullRow, row, pruneChildForResult)
        } else {
          extractGenerateEmptyChildren(generate, boundGenerator, generatorNullRow, row)
        }
      } catch {
        case e: Exception =>
          logger.error(s"Error evaluating generator: ${e.getMessage}", e)
          throw e
      }
    }
  }

  // No required child outputs, simpler case
  private def extractGenerateEmptyChildren(generate: GenerateExec,
                                           boundGenerator: Generator,
                                           generatorNullRow: GenericInternalRow,
                                           row: InternalRow) = {
    val generatedRows = boundGenerator.eval(row)

    if (generate.outer && generatedRows.isEmpty) {
      // Return a single null row for outer case
      Seq(generatorNullRow)
    } else {
      // Use the generated rows directly
      generatedRows.toSeq
    }
  }

  // If there are required child outputs, we need to join them with generated values
  private def extractGenerateNonEmptyChildren(generate: GenerateExec,
                                              boundGenerator: Generator,
                                              generatorNullRow: GenericInternalRow,
                                              row: InternalRow,
                                              pruneChildForResult: InternalRow => InternalRow) = {
    // Prune the child row if needed
    val prunedChildRow = pruneChildForResult(row)

    // Evaluate the generator against the input row
    val generatedRows = boundGenerator.eval(row)

    // handle the outer case if no rows were generated
    if (generate.outer && generatedRows.isEmpty) {
      val joined = new JoinedRow(prunedChildRow, generatorNullRow)
      Seq(joined)
    } else {
      val results = new ArrayBuffer[InternalRow](generatedRows.size)

      for (generatedRow <- generatedRows) {
        // Use JoinedRow to handle type conversions properly
        val joined = new JoinedRow(prunedChildRow, generatedRow)
        results += joined
      }

      results.toSeq
    }
  }

  /** Helper method to check if a plan tree contains any InputAdapter nodes
    * which indicate split points for WholeStageCodegenExec
    */
  private def containsInputAdapter(plan: org.apache.spark.sql.execution.SparkPlan): Boolean = {
    if (plan.isInstanceOf[InputAdapter]) {
      return true
    }
    plan.children.exists(containsInputAdapter)
  }

}
