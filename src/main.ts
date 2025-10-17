import * as Options from "@effect/cli/Options"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as NodeContext from "@effect/platform-node/NodeContext"
import * as NodeRuntime from "@effect/platform-node/NodeRuntime"
import * as Command from "@effect/cli/Command"
import * as CliConfig from "@effect/cli/CliConfig"
import * as Path from "node:path"
import * as Fs from "node:fs/promises"
import { OpenApi } from "./OpenApi.js"

const spec = Options.fileParse("spec").pipe(
  Options.withAlias("s"),
  Options.withDescription("The OpenAPI spec file to generate the client from"),
)

const name = Options.text("name").pipe(
  Options.withAlias("n"),
  Options.withDescription("The name of the generated client"),
  Options.withDefault("Client"),
)

const typeOnly = Options.boolean("type-only").pipe(
  Options.withAlias("t"),
  Options.withDescription("Generate a type-only client without schemas"),
)

const out = Options.text("out").pipe(
  Options.withAlias("o"),
  Options.withDescription("Directory to write generated files"),
)

const root = Command.make("openapigen", { spec, typeOnly, name, out }).pipe(
  Command.withHandler(({ spec, typeOnly, name, out }) =>
    OpenApi.generate(spec as any, { name, typeOnly }).pipe(
      Effect.flatMap((files) =>
        Effect.flatMap(
          Effect.tryPromise(() => Fs.mkdir(out, { recursive: true })),
          () =>
            Effect.forEach(files, (file) => {
                const targetPath = Path.join(out, file.path)
                return Effect.flatMap(
                  Effect.tryPromise(() =>
                    Fs.mkdir(Path.dirname(targetPath), { recursive: true }),
                  ),
                  () =>
                    Effect.tryPromise(() =>
                      Fs.writeFile(
                        targetPath,
                        file.contents.endsWith("\n")
                          ? file.contents
                          : `${file.contents}\n`,
                      ),
                    ),
                )
              }),
        ),
      ),
    ),
  ),
)

const run = Command.run(root, {
  name: "openapigen",
  version: "0.0.0",
})

const Env = Layer.mergeAll(
  NodeContext.layer,
  OpenApi.Live,
  CliConfig.layer({
    showBuiltIns: false,
  }),
)

run(process.argv).pipe(Effect.provide(Env), NodeRuntime.runMain)
