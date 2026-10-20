// Match Wrangler's Text rule without installing another test bundler.
import { readFileSync } from "node:fs";
import { registerHooks } from "node:module";

registerHooks({
  load(url, context, nextLoad) {
    if (url === new URL("../start_reader.py", import.meta.url).href) {
      return {
        format: "module", shortCircuit: true,
        source: `export default ${JSON.stringify(readFileSync(new URL(url), "utf8"))};`,
      };
    }
    return nextLoad(url, context);
  },
});
