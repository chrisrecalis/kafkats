---
'@kafkats/client': minor
'@kafkats/flow': minor
'@kafkats/codec-zod': minor
'@kafkats/flow-state-lmdb': minor
---

Ship dual ESM + CommonJS builds. All packages now publish both `dist/index.js` (ESM) and `dist/index.cjs` (CJS) with matching `.d.ts`/`.d.cts` declarations and conditional `exports`, so `require('@kafkats/...')` works in CommonJS projects. The build pipeline moved from `tsc` + `tsc-alias` to `tsdown`.
