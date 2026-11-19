# @kafkats/codec-zod

Zod schema validation codecs for @kafkats/client and @kafkats/flow.

## Features

- **Runtime Validation** - Validate messages at encode and decode
- **Type Inference** - TypeScript types from Zod schemas
- **Error Messages** - Detailed validation error reporting
- **Zero Configuration** - Just pass your Zod schema

## Installation

```bash
pnpm add @kafkats/codec-zod zod
```

## Quick Example

```typescript
import { flow, topic } from '@kafkats/flow'
import { string } from '@kafkats/client'
import { zodCodec } from '@kafkats/codec-zod'
import { z } from 'zod'

// Define schema
const UserSchema = z.object({
	id: z.string().uuid(),
	email: z.string().email(),
	age: z.number().min(0).max(150),
	role: z.enum(['admin', 'user', 'guest']),
})

// Create codec from schema
const userCodec = zodCodec(UserSchema)

// Use in topic definition
const users = topic('users', {
	key: string(),
	value: userCodec,
})

// Type is inferred from schema
type User = z.infer<typeof UserSchema>
```

## How It Works

The codec:

1. **On encode**: JSON stringifies the value as it is, without validating it (the value being produced is already typed)
2. **On decode**: JSON parses, then validates the result with `schema.parse`

```typescript
// Encoding does not validate
const bytes = userCodec.encode({ id: 'not-a-uuid', email: 'invalid', age: -5, role: 'unknown' } as User)

// Decoding does: this throws a ZodError (invalid id, email, age and role)
userCodec.decode(bytes)
```

## Next Steps

- [Usage Guide](/flow-codec-zod/usage) - Detailed usage patterns
