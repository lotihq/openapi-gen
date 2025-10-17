import type {
  OpenAPISpec,
  OpenAPISpecMethodName,
  OpenAPISpecPathItem,
} from "@effect/platform/OpenApi"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as JsonSchemaGen from "./JsonSchemaGen.js"
import type * as JsonSchema from "@effect/platform/OpenApiJsonSchema"
import type { DeepMutable } from "effect/Types"
import { camelize, identifier, nonEmptyString, toComment } from "./Utils.js"
import { convertObj } from "swagger2openapi"
import * as Context from "effect/Context"
import * as Option from "effect/Option"

const methodNames: ReadonlyArray<OpenAPISpecMethodName> = [
  "get",
  "put",
  "post",
  "delete",
  "options",
  "head",
  "patch",
  "trace",
]

const httpClientMethodNames: Record<OpenAPISpecMethodName, string> = {
  get: "get",
  put: "put",
  post: "post",
  delete: "del",
  options: "options",
  head: "head",
  patch: "patch",
  trace: `make("TRACE")`,
}

const structuralOmitKeys = new Set([
  "description",
  "title",
  "summary",
  "externalDocs",
  "examples",
  "example",
])

const canonicalizeSchema = (value: unknown): unknown => {
  if (Array.isArray(value)) {
    return value.map(canonicalizeSchema)
  }
  if (value && typeof value === "object") {
    const entries = Object.entries(value as Record<string, unknown>)
      .filter(
        ([key, v]) =>
          v !== undefined && !structuralOmitKeys.has(key) && !key.startsWith("x-"),
      )
      .map(([key, v]) => [key, canonicalizeSchema(v)] as const)
      .sort(([a], [b]) => a.localeCompare(b))
    const out: Record<string, unknown> = {}
    for (const [key, v] of entries) {
      out[key] = v
    }
    return out
  }
  return value
}

const schemaHash = (schema: JsonSchema.JsonSchema): string =>
  JSON.stringify(canonicalizeSchema(schema))

const dedupeComponentSchemas = (
  schemas: Record<string, JsonSchema.JsonSchema | { readonly $ref: string }>,
) => {
  const seen = new Map<string, string>()
  for (const [name, schema] of Object.entries(schemas)) {
    if (!schema || typeof schema !== "object" || "$ref" in schema) {
      continue
    }
    if (
      !(
        name.endsWith("Attributes") ||
        name.endsWith("Relationships")
      )
    ) {
      continue
    }
    const key = schemaHash(schema as JsonSchema.JsonSchema)
    const canonical = seen.get(key)
    if (canonical && canonical !== name) {
      schemas[name] = { $ref: `#/components/schemas/${canonical}` }
    } else {
      seen.set(key, name)
    }
  }
}

interface ParsedOperation {
  readonly id: string
  readonly method: OpenAPISpecMethodName
  readonly description: Option.Option<string>
  readonly params?: string
  readonly paramsOptional: boolean
  readonly urlParams: ReadonlyArray<string>
  readonly headers: ReadonlyArray<string>
  readonly cookies: ReadonlyArray<string>
  readonly payload?: string
  readonly payloadFormData: boolean
  readonly pathIds: ReadonlyArray<string>
  readonly pathTemplate: string
  readonly successSchemas: ReadonlyMap<string, string>
  readonly errorSchemas: ReadonlyMap<string, string>
  readonly voidSchemas: ReadonlySet<string>
}

export const make = Effect.gen(function* () {
  const isV2 = (spec: object) => "swagger" in spec

  const convert = Effect.fn("OpenApi.convert")((v2Spec: unknown) =>
    Effect.async<OpenAPISpec>((resume) => {
      convertObj(
        v2Spec as any,
        { laxDefaults: true, laxurls: true, patch: true, warnOnly: true },
        (err, result) => {
          if (err) {
            resume(Effect.die(err))
          } else {
            resume(Effect.succeed(result.openapi as any))
          }
        },
      )
    }),
  )

  const generate = Effect.fnUntraced(
    function* (
      spec: OpenAPISpec,
      options: {
        readonly name: string
        readonly typeOnly: boolean
      },
    ) {
      if (isV2(spec)) {
        spec = yield* convert(spec)
      }
      if (spec.components?.schemas) {
        dedupeComponentSchemas(
          spec.components.schemas as Record<
            string,
            JsonSchema.JsonSchema | { readonly $ref: string }
          >,
        )
      }
      const gen = yield* JsonSchemaGen.JsonSchemaGen
      const components = spec.components
        ? { ...spec.components }
        : { schemas: {} }
      const context = { components }
      const operations: Array<ParsedOperation> = []

      function resolveRef(ref: string) {
        const parts = ref.split("/").slice(1)
        let current: any = spec
        for (const part of parts) {
          current = current[part]
        }
        return current
      }

      const handlePath = (path: string, methods: OpenAPISpecPathItem) =>
        methodNames
          .filter((method) => !!methods[method])
          .forEach((method) => {
            const { ids: pathIds, path: pathTemplate } = processPath(path)
            const operation = methods[method]!
            const id = operation.operationId
              ? camelize(operation.operationId!)
              : `${method.toUpperCase()}${path}`
            const op: DeepMutable<ParsedOperation> & {
              description: Option.Option<string>
            } = {
              id,
              method,
              description: nonEmptyString(operation.description).pipe(
                Option.orElse(() => nonEmptyString(operation.summary)),
              ) as any,
              pathIds,
              pathTemplate,
              urlParams: [],
              headers: [],
              cookies: [],
              payloadFormData: false,
              successSchemas: new Map(),
              errorSchemas: new Map(),
              voidSchemas: new Set(),
              paramsOptional: true,
            }
            const schemaId = identifier(operation.operationId ?? path)
            const validParameters =
              operation.parameters?.filter(
                (_) => _.in !== "path" && _.in !== "cookie",
              ) ?? []
            if (validParameters.length > 0) {
              const schema: JsonSchema.Object = {
                type: "object",
                properties: {},
                required: [],
              }
              validParameters.forEach((parameter) => {
                if ("$ref" in parameter) {
                  parameter = resolveRef(parameter.$ref as string)
                }
                if (parameter.in === "path") {
                  return
                }
                const paramSchema = parameter.schema
                const added: Array<string> = []
                if ("properties" in paramSchema) {
                  const required = paramSchema.required ?? []
                  Object.entries(paramSchema.properties).forEach(
                    ([name, propSchema]) => {
                      const adjustedName = `${parameter.name}[${name}]`
                      schema.properties[adjustedName] = propSchema
                      if (required.includes(name)) {
                        schema.required.push(adjustedName)
                      }
                      added.push(adjustedName)
                    },
                  )
                } else {
                  schema.properties[parameter.name] = parameter.schema
                  parameter.required && schema.required.push(parameter.name)
                  added.push(parameter.name)
                }
                if (parameter.in === "query") {
                  op.urlParams.push(...added)
                } else if (parameter.in === "header") {
                  op.headers.push(...added)
                } else if (parameter.in === "cookie") {
                  op.cookies.push(...added)
                }
              })
              op.params = gen.addSchema(
                `${schemaId}Params`,
                schema,
                context,
                true,
              )
              op.paramsOptional =
                !schema.required || schema.required.length === 0
            }
            if (operation.requestBody?.content?.["application/json"]?.schema) {
              op.payload = gen.addSchema(
                `${schemaId}Request`,
                operation.requestBody.content["application/json"].schema,
                context,
              )
            } else if (
              operation.requestBody?.content?.["multipart/form-data"]
            ) {
              op.payload = gen.addSchema(
                `${schemaId}Request`,
                operation.requestBody.content["multipart/form-data"].schema,
                context,
              )
              op.payloadFormData = true
            }
            let defaultSchema: string | undefined
            Object.entries(operation.responses ?? {}).forEach(
              ([status, response]) => {
                while ("$ref" in response) {
                  response = resolveRef(response.$ref as string)
                }
                if (response.content?.["application/json"]?.schema) {
                  const schemaName = gen.addSchema(
                    `${schemaId}${status}`,
                    response.content["application/json"].schema,
                    context,
                    true,
                  )
                  if (status === "default") {
                    defaultSchema = schemaName
                    return
                  }
                  const statusLower = status.toLowerCase()
                  const statusMajorNumber = Number(status[0])
                  if (isNaN(statusMajorNumber)) {
                    return
                  } else if (statusMajorNumber < 4) {
                    op.successSchemas.set(statusLower, schemaName)
                  } else {
                    op.errorSchemas.set(statusLower, schemaName)
                  }
                }
                if (!response.content) {
                  op.voidSchemas.add(status.toLowerCase())
                }
              },
            )
            if (op.successSchemas.size === 0 && defaultSchema) {
              op.successSchemas.set("2xx", defaultSchema)
            }
            operations.push(op)
          })

      Object.entries(spec.paths).forEach(([path, methods]) =>
        handlePath(path, methods),
      )

      const transformer = yield* OpenApiTransformer
      const schemaModuleName = "Models"
      const primitivesModuleName = "Primitives"
      const schemas = yield* gen.generate("S", {
        hoistReferencePrefix: `${primitivesModuleName}.`,
      })

      type Definition = {
        readonly name: string
        readonly source: string
        readonly dependencies: ReadonlyArray<string>
        readonly index: number
      }

      type FileInfo = {
        readonly key: string
        readonly filename: string
        definitions: Array<Definition>
        earliestIndex: number
      }

      const definitionList: Array<Definition> = schemas.sources.map(
        (definition, index) => ({
          ...definition,
          index,
        }),
      )

      const definitionByName = new Map(
        definitionList.map((definition) => [definition.name, definition] as const),
      )

      const depthCache = new Map<string, number>()
      const computeDepth = (name: string): number => {
        const cached = depthCache.get(name)
        if (cached !== undefined) {
          return cached
        }
        const definition = definitionByName.get(name)
        if (!definition) {
          depthCache.set(name, 0)
          return 0
        }
        if (definition.dependencies.length === 0) {
          depthCache.set(name, 0)
          return 0
        }
        let depth = 0
        for (const dep of definition.dependencies) {
          depth = Math.max(depth, computeDepth(dep) + 1)
        }
        depthCache.set(name, depth)
        return depth
      }

      const categoryConfig: Record<
        string,
        {
          readonly filename: string
        }
      > = {
        foundations: { filename: "foundations" },
        entities: { filename: "entities" },
        details: { filename: "details" },
        params: { filename: "params" },
        requests: { filename: "requests" },
        references: { filename: "references" },
        data: { filename: "data" },
      }

      const categorize = (name: string): string => {
        if (name.endsWith("Params")) {
          return "params"
        }
        if (name.endsWith("Request") || name.endsWith("Requests")) {
          return "requests"
        }
        if (name.endsWith("Response") || name.endsWith("Responses")) {
          return "requests"
        }
        if (name.startsWith("Referenced") || name.startsWith("NullableReferenced")) {
          return "references"
        }
        if (name.endsWith("Data")) {
          return "data"
        }
        const depth = computeDepth(name)
        if (depth === 0) {
          return "foundations"
        }
        if (depth === 1) {
          return "entities"
        }
        return "details"
      }

      const filesByKey = new Map<string, FileInfo>()
      const definitionToFile = new Map<string, string>()

      const getOrCreateFile = (key: string, index: number): FileInfo => {
        const config = categoryConfig[key] ?? categoryConfig.entities
        const resolvedKey = categoryConfig[key] ? key : "entities"
        if (!filesByKey.has(resolvedKey)) {
          filesByKey.set(resolvedKey, {
            key: resolvedKey,
            filename: config.filename,
            definitions: [],
            earliestIndex: index,
          })
        }
        const file = filesByKey.get(resolvedKey)!
        file.earliestIndex = Math.min(file.earliestIndex, index)
        return file
      }

      for (const definition of definitionList) {
        const desiredKey = categorize(definition.name)
        const file = getOrCreateFile(desiredKey, definition.index)
        file.definitions.push(definition)
        definitionToFile.set(definition.name, file.key)
      }

      const rebuildFileGraph = () => {
        const graph = new Map<string, Set<string>>()
        for (const [key, file] of filesByKey) {
          const deps = new Set<string>()
          for (const definition of file.definitions) {
            for (const dep of definition.dependencies) {
              const targetFile = definitionToFile.get(dep)
              if (targetFile && targetFile !== key) {
                deps.add(targetFile)
              }
            }
          }
          graph.set(key, deps)
        }
        return graph
      }

      const mergeFiles = (target: string, sources: ReadonlyArray<string>) => {
        const targetFile = filesByKey.get(target)
        if (!targetFile) {
          return
        }
        for (const sourceKey of sources) {
          if (sourceKey === target) {
            continue
          }
          const sourceFile = filesByKey.get(sourceKey)
          if (!sourceFile) {
            continue
          }
          for (const definition of sourceFile.definitions) {
            targetFile.definitions.push(definition)
            definitionToFile.set(definition.name, target)
          }
          targetFile.earliestIndex = Math.min(
            targetFile.earliestIndex,
            sourceFile.earliestIndex,
          )
          filesByKey.delete(sourceKey)
        }
      }

      const computeStronglyConnectedComponents = (
        graph: Map<string, Set<string>>,
      ) => {
        let index = 0
        const indices = new Map<string, number>()
        const lowlinks = new Map<string, number>()
        const stack: Array<string> = []
        const onStack = new Set<string>()
        const components: Array<Array<string>> = []

        const strongConnect = (node: string) => {
          indices.set(node, index)
          lowlinks.set(node, index)
          index++
          stack.push(node)
          onStack.add(node)

          for (const dep of graph.get(node) ?? new Set<string>()) {
            if (!indices.has(dep)) {
              strongConnect(dep)
              lowlinks.set(
                node,
                Math.min(
                  lowlinks.get(node)!,
                  lowlinks.get(dep)!,
                ),
              )
            } else if (onStack.has(dep)) {
              lowlinks.set(
                node,
                Math.min(
                  lowlinks.get(node)!,
                  indices.get(dep)!,
                ),
              )
            }
          }

          if (lowlinks.get(node) === indices.get(node)) {
            const component: Array<string> = []
            while (true) {
              const item = stack.pop()!
              onStack.delete(item)
              component.push(item)
              if (item === node) {
                break
              }
            }
            components.push(component)
          }
        }

        for (const node of graph.keys()) {
          if (!indices.has(node)) {
            strongConnect(node)
          }
        }

        return components
      }

      let changed = true
      while (changed) {
        changed = false
        const graph = rebuildFileGraph()
        const components = computeStronglyConnectedComponents(graph)
        for (const component of components) {
          if (component.length <= 1) {
            continue
          }
          changed = true
          const [first, ...rest] = component.sort()
          mergeFiles(first, rest)
          break
        }
      }

      for (const file of filesByKey.values()) {
        file.definitions.sort((a, b) => a.index - b.index)
      }

      const orderedFiles = Array.from(filesByKey.values()).sort((a, b) => {
        if (a.earliestIndex !== b.earliestIndex) {
          return a.earliestIndex - b.earliestIndex
        }
        return a.key.localeCompare(b.key)
      })

      const fileOrder = new Map<string, number>()
      orderedFiles.forEach((file, index) => {
        fileOrder.set(file.key, index)
      })

      const fileImports = new Map<
        string,
        Map<string, Set<string>>
      >()
      for (const file of orderedFiles) {
        const depMap = new Map<string, Set<string>>()
        for (const definition of file.definitions) {
          for (const dep of definition.dependencies) {
            const depFileKey = definitionToFile.get(dep)
            if (!depFileKey || depFileKey === file.key) {
              continue
            }
          const set = depMap.get(depFileKey)
            if (set) {
              set.add(dep)
            } else {
              depMap.set(depFileKey, new Set([dep]))
            }
          }
        }
        fileImports.set(file.key, depMap)
      }

      const warningBlock =
        schemas.warnings.length > 0
          ? schemas.warnings.join("\n")
          : undefined

      const modelFiles = orderedFiles.map((file, index) => {
        const imports: Array<string> = []
        imports.push('import * as S from "effect/Schema"')

        const usesPrimitives = file.definitions.some((definition) =>
          definition.source.includes("Primitives."),
        )
        if (usesPrimitives && schemas.hoists.length > 0) {
          imports.push('import * as Primitives from "../primitives"')
        }

        const dependencyMap = fileImports.get(file.key)
        if (dependencyMap) {
          const entries = Array.from(dependencyMap.entries()).sort(
            ([a], [b]) =>
              (fileOrder.get(a) ?? 0) - (fileOrder.get(b) ?? 0),
          )
          for (const [depFileKey, names] of entries) {
            const depFile = filesByKey.get(depFileKey)
            if (!depFile || names.size === 0) {
              continue
            }
            const sortedNames = Array.from(names).sort()
            imports.push(
              `import { ${sortedNames.join(", ")} } from "./${depFile.filename}"`,
            )
          }
        }

        const body = file.definitions.map((definition) => definition.source).join("\n\n")
        const sections: Array<string> = []
        if (warningBlock && index === 0) {
          sections.push(warningBlock)
        }
        sections.push(imports.join("\n"))
        sections.push(body)
        const contents = sections.filter((section) => section.length > 0).join("\n\n")
        return {
          path: `models/${file.filename}.ts`,
          contents,
        } as const
      })

      const primitivesContent =
        schemas.hoists.length > 0
          ? [
              'import * as S from "effect/Schema"',
              schemas.hoists
                .map(({ name, expression }) => `export const ${name} = ${expression}`)
                .join("\n"),
            ].join("\n\n")
          : undefined

      const modelsIndexSections: Array<string> = orderedFiles.map(
        (file) => `export * from "./${file.filename}"`,
      )

      if (schemas.aliases.length > 0) {
        for (const { alias, target } of schemas.aliases) {
          const targetFileKey = definitionToFile.get(target)
          if (!targetFileKey) {
            continue
          }
          const targetFile = filesByKey.get(targetFileKey)
          if (!targetFile) {
            continue
          }
          modelsIndexSections.push(
            `export { ${target} as ${alias} } from "./${targetFile.filename}"`,
          )
        }
      }

      const modelsIndexContent = Array.from(new Set(modelsIndexSections)).join("\n")

      const modelsFacadeContent = `export * from "./models/index"`

      const clientImports = [
        transformer.imports,
        `import * as ${schemaModuleName} from "./models"`,
      ].join("\n")

      const clientImplementation = transformer.toImplementation(
        options.name,
        operations,
        { schemaQualifier: `${schemaModuleName}.` },
      )
      const clientTypes = transformer.toTypes(options.name, operations, {
        schemaQualifier: `${schemaModuleName}.`,
      })
      const clientContent = [clientImports, clientImplementation, clientTypes]
        .filter((_) => _.length > 0)
        .join("\n\n")

      const indexLines = [
        `export * as ${schemaModuleName} from "./models"`,
        schemas.hoists.length > 0
          ? `export * as ${primitivesModuleName} from "./primitives"`
          : undefined,
        'export * as Client from "./client"',
        'export * from "./models"',
        'export * from "./client"',
      ].filter((_) => _ !== undefined) as Array<string>
      const indexContent = indexLines.join("\n")

      const files: Array<{ readonly path: string; readonly contents: string }> = [
        ...modelFiles,
        { path: "models/index.ts", contents: modelsIndexContent },
        { path: "models.ts", contents: modelsFacadeContent },
        { path: "client.ts", contents: clientContent },
        { path: "index.ts", contents: indexContent },
      ]
      if (primitivesContent) {
        files.push({ path: "primitives.ts", contents: primitivesContent })
      }

      return files
    },
    JsonSchemaGen.with,
    (effect, _, options) =>
      Effect.provide(
        effect,
        options?.typeOnly ? layerTransformerTs : layerTransformerSchema,
      ),
  )

  return { generate } as const
})

export class OpenApi extends Effect.Tag("OpenApi")<
  OpenApi,
  Effect.Effect.Success<typeof make>
>() {
  static Live = Layer.effect(OpenApi, make)
}

export class OpenApiTransformer extends Context.Tag("OpenApiTransformer")<
  OpenApiTransformer,
  {
    readonly imports: string
    readonly toTypes: (
      name: string,
      operations: ReadonlyArray<ParsedOperation>,
      options?: {
        readonly schemaQualifier?: string
      },
    ) => string
    readonly toImplementation: (
      name: string,
      operations: ReadonlyArray<ParsedOperation>,
      options?: {
        readonly schemaQualifier?: string
      },
    ) => string
  }
>() {}

export const layerTransformerSchema = Layer.sync(OpenApiTransformer, () => {
  const operationsToInterface = (
    name: string,
    operations: ReadonlyArray<ParsedOperation>,
    options?: {
      readonly schemaQualifier?: string
    },
  ) => {
    const qualifier = options?.schemaQualifier ?? ""
    return `export interface ${name} {
  readonly httpClient: HttpClient.HttpClient
  ${operations.map((op) => operationToMethod(name, op, qualifier)).join("\n  ")}
}

${clientErrorSource(name)}`
  }

  const operationToMethod = (
    name: string,
    operation: ParsedOperation,
    qualifier: string,
  ) => {
    const args: Array<string> = []
    if (operation.pathIds.length > 0) {
      args.push(...operation.pathIds.map((id) => `${id}: string`))
    }
    let options: Array<string> = []
    if (operation.params && !operation.payload) {
      args.push(
        `options${operation.paramsOptional ? "?" : ""}: typeof ${qualifier}${operation.params}.Encoded${operation.paramsOptional ? " | undefined" : ""}`,
      )
    } else if (operation.params) {
      options.push(
        `readonly params${operation.paramsOptional ? "?" : ""}: typeof ${qualifier}${operation.params}.Encoded${operation.paramsOptional ? " | undefined" : ""}`,
      )
    }
    if (operation.payload) {
      const type = `typeof ${qualifier}${operation.payload}.Encoded`
      if (!operation.params) {
        args.push(`options: ${type}`)
      } else {
        options.push(`readonly payload: ${type}`)
      }
    }
    if (options.length > 0) {
      args.push(`options: { ${options.join("; ")} }`)
    }
    let success = "void"
    if (operation.successSchemas.size > 0) {
      success = Array.from(operation.successSchemas.values())
        .map((schema) => `typeof ${qualifier}${schema}.Type`)
        .join(" | ")
    }
    const errors = ["HttpClientError.HttpClientError", "ParseError"]
    if (operation.errorSchemas.size > 0) {
      errors.push(
        ...Array.from(operation.errorSchemas.values()).map(
          (schema) =>
            `${name}Error<"${schema}", typeof ${qualifier}${schema}.Type>`,
        ),
      )
    }
    return `${toComment(operation.description)}readonly "${operation.id}": (${args.join(", ")}) => Effect.Effect<${success}, ${errors.join(" | ")}>`
  }

  const operationsToImpl = (
    name: string,
    operations: ReadonlyArray<ParsedOperation>,
    options?: {
      readonly schemaQualifier?: string
    },
  ) => {
    const qualifier = options?.schemaQualifier ?? ""
    return `export const make = (
  httpClient: HttpClient.HttpClient, 
  options: {
    readonly transformClient?: ((client: HttpClient.HttpClient) => Effect.Effect<HttpClient.HttpClient>) | undefined
  } = {}
): ${name} => {
  ${commonSource}
  const decodeSuccess =
    <A, I, R>(schema: S.Schema<A, I, R>) =>
    (response: HttpClientResponse.HttpClientResponse) => {
      return HttpClientResponse.schemaBodyJson(schema)(response) as unknown as Effect.Effect<
        A,
        any,
        never
      >
    }
  const decodeError =
    <const Tag extends string, A, I, R>(tag: Tag, schema: S.Schema<A, I, R>) =>
    (response: HttpClientResponse.HttpClientResponse) => {
      return Effect.flatMap(
        HttpClientResponse.schemaBodyJson(schema)(response),
        (cause) => Effect.fail(${name}Error(tag, cause, response)),
      ) as unknown as Effect.Effect<
        A,
        any,
        never
      >
    }
  return {
    httpClient,
    ${operations.map((operation) => operationToImpl(operation, qualifier)).join(",\n  ")}
  }
}`
  }

  const operationToImpl = (
    operation: ParsedOperation,
    qualifier: string,
  ) => {
    const args: Array<string> = [...operation.pathIds]
    const hasOptions = operation.params || operation.payload
    if (hasOptions) {
      args.push("options")
    }
    const params = `${args.join(", ")}`

    const pipeline: Array<string> = []

    if (operation.params) {
      const varName = operation.payload ? "options.params?." : "options?."
      if (operation.urlParams.length > 0) {
        const props = operation.urlParams.map(
          (param) => `"${param}": ${varName}["${param}"] as any`,
        )
        pipeline.push(`HttpClientRequest.setUrlParams({ ${props.join(", ")} })`)
      }
      if (operation.headers.length > 0) {
        const props = operation.headers.map(
          (param) => `"${param}": ${varName}["${param}"] ?? undefined`,
        )
        pipeline.push(`HttpClientRequest.setHeaders({ ${props.join(", ")} })`)
      }
    }

    const payloadVarName = operation.params ? "options.payload" : "options"
    if (operation.payloadFormData) {
      pipeline.push(
        `HttpClientRequest.bodyFormDataRecord(${payloadVarName} as any)`,
      )
    } else if (operation.payload) {
      pipeline.push(`HttpClientRequest.bodyUnsafeJson(${payloadVarName})`)
    }

    const decodes: Array<string> = []
    const singleSuccessCode = operation.successSchemas.size === 1
    operation.successSchemas.forEach((schema, status) => {
      const statusCode =
        singleSuccessCode && status.startsWith("2") ? "2xx" : status
      decodes.push(`"${statusCode}": decodeSuccess(${qualifier}${schema})`)
    })
    operation.errorSchemas.forEach((schema, status) => {
      decodes.push(
        `"${status}": decodeError("${schema}", ${qualifier}${schema})`,
      )
    })
    operation.voidSchemas.forEach((status) => {
      decodes.push(`"${status}": () => Effect.void`)
    })
    decodes.push(`orElse: unexpectedStatus`)

    pipeline.push(`withResponse(HttpClientResponse.matchStatus({
      ${decodes.join(",\n      ")}
    }))`)

    return (
      `"${operation.id}": (${params}) => ` +
      `HttpClientRequest.${httpClientMethodNames[operation.method]}(${operation.pathTemplate})` +
      `.pipe(\n    ${pipeline.join(",\n    ")}\n  )`
    )
  }

  return OpenApiTransformer.of({
    imports: [
      'import type * as HttpClient from "@effect/platform/HttpClient"',
      'import * as HttpClientError from "@effect/platform/HttpClientError"',
      'import * as HttpClientRequest from "@effect/platform/HttpClientRequest"',
      'import * as HttpClientResponse from "@effect/platform/HttpClientResponse"',
      'import * as Data from "effect/Data"',
      'import * as Effect from "effect/Effect"',
      'import type { ParseError } from "effect/ParseResult"',
      'import * as S from "effect/Schema"',
    ].join("\n"),
    toTypes: operationsToInterface,
    toImplementation: operationsToImpl,
  })
}).pipe(Layer.merge(JsonSchemaGen.layerTransformerSchema))

export const layerTransformerTs = Layer.sync(OpenApiTransformer, () => {
  const operationsToInterface = (
    name: string,
    operations: ReadonlyArray<ParsedOperation>,
  ) => `export interface ${name} {
  readonly httpClient: HttpClient.HttpClient
  ${operations.map((s) => operationToMethod(name, s)).join("\n  ")}
}

${clientErrorSource(name)}`

  const operationToMethod = (name: string, operation: ParsedOperation) => {
    const args: Array<string> = []
    if (operation.pathIds.length > 0) {
      args.push(...operation.pathIds.map((id) => `${id}: string`))
    }
    let options: Array<string> = []
    if (operation.params && !operation.payload) {
      args.push(
        `options${operation.paramsOptional ? "?" : ""}: ${operation.params}${operation.paramsOptional ? " | undefined" : ""}`,
      )
    } else if (operation.params) {
      options.push(
        `readonly params${operation.paramsOptional ? "?" : ""}: ${operation.params}${operation.paramsOptional ? " | undefined" : ""}`,
      )
    }
    if (operation.payload) {
      const type = operation.payload
      if (!operation.params) {
        args.push(`options: ${type}`)
      } else {
        options.push(`readonly payload: ${type}`)
      }
    }
    if (options.length > 0) {
      args.push(`options: { ${options.join("; ")} }`)
    }
    let success = "void"
    if (operation.successSchemas.size > 0) {
      success = Array.from(operation.successSchemas.values()).join(" | ")
    }
    const errors = ["HttpClientError.HttpClientError"]
    if (operation.errorSchemas.size > 0) {
      for (const schema of operation.errorSchemas.values()) {
        errors.push(`${name}Error<"${schema}", ${schema}>`)
      }
    }
    return `${toComment(operation.description)}readonly "${operation.id}": (${args.join(", ")}) => Effect.Effect<${success}, ${errors.join(" | ")}>`
  }

  const operationsToImpl = (
    name: string,
    operations: ReadonlyArray<ParsedOperation>,
  ) => `export const make = (
  httpClient: HttpClient.HttpClient, 
  options: {
    readonly transformClient?: ((client: HttpClient.HttpClient) => Effect.Effect<HttpClient.HttpClient>) | undefined
  } = {}
): ${name} => {
  ${commonSource}
  const decodeSuccess = <A>(response: HttpClientResponse.HttpClientResponse) =>
    response.json as Effect.Effect<A, HttpClientError.ResponseError>
  const decodeVoid = (_response: HttpClientResponse.HttpClientResponse) =>
    Effect.void
  const decodeError =
    <Tag extends string, E>(tag: Tag) =>
    (
      response: HttpClientResponse.HttpClientResponse,
    ): Effect.Effect<
      never,
      ${name}Error<Tag, E> | HttpClientError.ResponseError
    > =>
      Effect.flatMap(
        response.json as Effect.Effect<E, HttpClientError.ResponseError>,
        (cause) => Effect.fail(${name}Error(tag, cause, response)),
      )
  const onRequest = (
    successCodes: ReadonlyArray<string>,
    errorCodes?: Record<string, string>,
  ) => {
    const cases: any = { orElse: unexpectedStatus }
    for (const code of successCodes) {
      cases[code] = decodeSuccess
    }
    if (errorCodes) {
      for (const [code, tag] of Object.entries(errorCodes)) {
        cases[code] = decodeError(tag)
      }
    }
    if (successCodes.length === 0) {
      cases["2xx"] = decodeVoid
    }
    return withResponse(HttpClientResponse.matchStatus(cases) as any)
  }
  return {
    httpClient,
    ${operations.map(operationToImpl).join(",\n  ")}
  }
}`

  const operationToImpl = (operation: ParsedOperation) => {
    const args: Array<string> = [...operation.pathIds]
    const hasOptions = operation.params || operation.payload
    if (hasOptions) {
      args.push("options")
    }
    const params = `${args.join(", ")}`

    const pipeline: Array<string> = []

    if (operation.params) {
      const varName = operation.payload ? "options.params?." : "options?."
      if (operation.urlParams.length > 0) {
        const props = operation.urlParams.map(
          (param) => `"${param}": ${varName}["${param}"] as any`,
        )
        pipeline.push(`HttpClientRequest.setUrlParams({ ${props.join(", ")} })`)
      }
      if (operation.headers.length > 0) {
        const props = operation.headers.map(
          (param) => `"${param}": ${varName}["${param}"] ?? undefined`,
        )
        pipeline.push(`HttpClientRequest.setHeaders({ ${props.join(", ")} })`)
      }
    }

    const payloadVarName = operation.params ? "options.payload" : "options"
    if (operation.payloadFormData) {
      pipeline.push(
        `HttpClientRequest.bodyFormDataRecord(${payloadVarName} as any)`,
      )
    } else if (operation.payload) {
      pipeline.push(`HttpClientRequest.bodyUnsafeJson(${payloadVarName})`)
    }

    const successCodesRaw = Array.from(operation.successSchemas.keys())
    const successCodes = successCodesRaw
      .map((_) => JSON.stringify(_))
      .join(", ")
    const singleSuccessCode =
      successCodesRaw.length === 1 && successCodesRaw[0].startsWith("2")
    const errorCodes =
      operation.errorSchemas.size > 0 &&
      Object.fromEntries(operation.errorSchemas.entries())
    pipeline.push(
      `onRequest([${singleSuccessCode ? `"2xx"` : successCodes}]${errorCodes ? `, ${JSON.stringify(errorCodes)}` : ""})`,
    )

    return (
      `"${operation.id}": (${params}) => ` +
      `HttpClientRequest.${httpClientMethodNames[operation.method]}(${operation.pathTemplate})` +
      `.pipe(\n    ${pipeline.join(",\n    ")}\n  )`
    )
  }

  return OpenApiTransformer.of({
    imports: [
      'import type * as HttpClient from "@effect/platform/HttpClient"',
      'import * as HttpClientError from "@effect/platform/HttpClientError"',
      'import * as HttpClientRequest from "@effect/platform/HttpClientRequest"',
      'import * as HttpClientResponse from "@effect/platform/HttpClientResponse"',
      'import * as Data from "effect/Data"',
      'import * as Effect from "effect/Effect"',
    ].join("\n"),
    toTypes: operationsToInterface,
    toImplementation: operationsToImpl,
  })
}).pipe(Layer.merge(JsonSchemaGen.layerTransformerTs))

const processPath = (path: string) => {
  const ids: Array<string> = []
  path = path.replace(/{([^}]+)}/g, (_, name) => {
    const id = camelize(name)
    ids.push(id)
    return "${" + id + "}"
  })
  return { path: "`" + path + "`", ids } as const
}

const commonSource = `const unexpectedStatus = (response: HttpClientResponse.HttpClientResponse) =>
    Effect.flatMap(
      Effect.orElseSucceed(response.json, () => "Unexpected status code"),
      (description) =>
        Effect.fail(
          new HttpClientError.ResponseError({
            request: response.request,
            response,
            reason: "StatusCode",
            description: typeof description === "string" ? description : JSON.stringify(description),
          }),
        ),
    )
  const withResponse = <A, E, R = never>(
    f: (response: HttpClientResponse.HttpClientResponse) => Effect.Effect<A, E, R>,
  ) => (
    request: HttpClientRequest.HttpClientRequest,
  ): Effect.Effect<A, E, R> => {
    if (options.transformClient) {
      return Effect.flatMap(
        Effect.flatMap(options.transformClient!(httpClient), (client) =>
          client.execute(request),
        ),
        f,
      ) as unknown as Effect.Effect<A, E, R>
    }
    return Effect.flatMap(httpClient.execute(request), f) as unknown as Effect.Effect<A, E, R>
  };`

const clientErrorSource = (
  name: string,
) => `export interface ${name}Error<Tag extends string, E> {
  readonly _tag: Tag
  readonly request: HttpClientRequest.HttpClientRequest
  readonly response: HttpClientResponse.HttpClientResponse
  readonly cause: E
}

class ${name}ErrorImpl extends Data.Error<{
  _tag: string
  cause: any
  request: HttpClientRequest.HttpClientRequest
  response: HttpClientResponse.HttpClientResponse
}> {}

export const ${name}Error = <Tag extends string, E>(
  tag: Tag,
  cause: E,
  response: HttpClientResponse.HttpClientResponse,
): ${name}Error<Tag, E> =>
  new ${name}ErrorImpl({
    _tag: tag,
    cause,
    response,
    request: response.request,
  }) as any`
