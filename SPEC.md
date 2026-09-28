# SPEC — Compact Stellar Historical Index (V0)

Estado: **Etapa 1 (extractor) y Etapa 2 (builder + reader + benchmark) ejecutadas sobre `w3_recent` únicamente.** `w1_early`/`w2_soroban_launch` (§4) siguen sin correr — no forman parte de este alcance. Resultados reales en `data/out/build_report.json`, `data/out/benchmark_report.json` y `data/out/benchmark_table.txt`.

Este documento define, sin ambigüedad, qué significa "la cuenta X aparece en el ledger L" y cómo se construye/mide el índice. No es una spec de producto ni un reemplazo de Horizon: es el contrato mínimo necesario para que el experimento sea reproducible.

> El índice acelera localizar los datos, no obtener/descomprimir los datos subyacentes del ledger.

---

## 1. `definition_of_appearance` — versión congelada: `v1-broad-classic`

Un `AccountId` X "aparece" en el ledger L si se cumple **cualquiera** de las siguientes reglas, evaluadas sobre el `LedgerCloseMeta` de L (todas las variantes v0/v1/v2, con `TransactionMeta` v0..v4):

| Regla | Cuándo aplica | Campo XDR exacto |
|---|---|---|
| **A — Account** | Cualquier `LedgerEntryChange` de tipo `ACCOUNT` en `changes` de cualquier `OperationMeta`/`OperationMetaV2` (o en `txChangesBefore`/`txChangesAfter` cuando la versión de meta los trae) | `AccountEntry.accountId` (variantes CREATED/UPDATED/STATE/RESTORED, que traen `LedgerEntry`) o `LedgerKey.account().accountId` (variante REMOVED, que sólo trae `LedgerKey`) |
| **B — Trustline** | Igual que A pero tipo `TRUSTLINE` | `TrustLineEntry.accountId` o `LedgerKey.trustLine().accountId` |
| **C — Offer** | Igual que A pero tipo `OFFER` | `OfferEntry.sellerId` o `LedgerKey.offer().sellerId` |
| **D — Data** | Igual que A pero tipo `DATA` | `DataEntry.accountId` o `LedgerKey.data().accountId` |
| **E — Claimant de Claimable Balance** | Sólo en variantes `CREATED`/`UPDATED` (nunca en `REMOVED`, que sólo trae `balanceId`, sin `AccountId`) | Cada `Claimant` en `ClaimableBalanceEntry.claimants`, tomando `Claimant.v0().destination` |
| **F — Source account** | Toda transacción incluida en el ledger (**éxito o fallo, ver §1.2**), y toda operación con override de source | `TransactionEnvelope` → `tx.sourceAccount`; `Operation.sourceAccount` cuando está presente; `FeeBumpTransactionEnvelope.tx.feeSource` cuando la tx es fee-bumped |

**No se implementa** la semántica completa de Horizon por tipo de operación (payment→destination, path payment→intermedios, etc.) — eso es una definición de producto, fuera de alcance de V0. Tampoco se reduce a "sólo ACCOUNT" — eso subestima la densidad real y no es lo que se decidió medir.

### 1.1 Normalización de `MuxedAccount` → `AccountId`

Tanto `tx.sourceAccount` como `Operation.sourceAccount` y `feeSource` son `MuxedAccount` (pueden traer un `id` de sub-cuenta, M...). Para todas las reglas de esta spec, un `MuxedAccount` se reduce a su `AccountId` ed25519 subyacente, descartando el id de muxing. El índice es por cuenta clásica (G...), no por sub-cuenta muxeada (M...). Esto se documenta explícitamente en el manifest (`account_granularity: "ed25519_account_id"`).

### 1.2 Transacciones fallidas: SÍ cuentan

Una transacción incluida en el ledger cuenta bajo la regla F **independientemente de si tuvo éxito**. "Aparecer" significa "participó" (firmó/pagó), no "tuvo efecto". Esta es una decisión explícita, no un descuido — se documenta en el manifest (`include_failed_transactions: true`) porque afecta la densidad medible y alguien podría razonablemente elegir lo contrario.

### 1.3 Deduplicación dentro de un mismo ledger

Una cuenta puede disparar varias reglas (A-F) múltiples veces dentro del mismo ledger (ej: es source de 3 operaciones y además tiene su trustline actualizada). El builder debe:

1. Por cada ledger L, acumular el conjunto de `AccountId` tocados por cualquier regla A-F en un `HashSet<AccountId>` (o estructura equivalente) — **no** una lista con repetidos.
2. Para cada cuenta en ese conjunto, emitir **un único** par `(account, L)` al dataset canónico (§3).

Esto garantiza que la posting list de una cuenta tenga como máximo una entrada por ledger, y que esas entradas salgan en orden creciente por construcción (se procesan los ledgers en orden), que es la precondición que Elias-Fano necesita (secuencia monótona).

---

## 2. Formato binario del índice — V0, encoding híbrido congelado

**Hallazgo de Etapa 1** (ventana `w3_recent`, 5000 ledgers reales): la distribución de longitud de posting list es fuertemente bimodal — mediana=p95=1, max=4996/5000. Un Elias-Fano puro por lista pierde en el caso común (`len==1`) por overhead fijo de la estructura serializada.

**Decisión (aprobada):** `encoding = "raw_if_len1_else_ef"`

- `len == 1` → el valor crudo (`ledger_seq: u32`, zero-extendido a u64) se guarda **inline en el directorio**. No se construye ninguna estructura Elias-Fano para este caso — ni siquiera la más chica.
- `len >= 2` → Elias-Fano (`sux::EfSeqDict<u64>`, con índice de rank/select tanto en unos como en ceros — necesario para soportar `get`, `succ`/`pred` y por tanto range queries) sobre `(ledger_seq - ledger_start)`, universo = `ledger_end - ledger_start + 1`, serializado con epserde y embebido en un blob aparte.

Esto **no esconde** el overhead: el benchmark (§6 y `data/out/benchmark_report.json`) mide el resultado real, bucket por bucket, incluyendo los casos donde el índice compacto pierde.

### Buckets de reporte (nunca promediar)

| Bucket | Rango de longitud | Universo real (`w3_recent`) | Muestra |
|---|---|---|---|
| `cold` | `len == 1` | 1.261.301 | 200 (muestreo sistemático) |
| `warm` | `2..=32` | 40.432 | 50 (muestreo sistemático) |
| `hot` | `len > 32` | 10.863 | top 100 por longitud |

### Layout binario

```
[header, tamaño fijo = 26 bytes]
  magic: b"CSIHYBV1"              (8 bytes)
  format_version: u16 = 1
  ledger_start: u32
  ledger_end: u32
  account_count: u64

[account directory — 48 bytes/entrada, ordenado por account_id]
  account_id: [u8; 32]
  len: u32          // 1 => raw inline; >=2 => chunk EF
  payload: u64      // len==1: el ledger_seq crudo. len>=2: offset (bytes) dentro del ef_blob
  payload_len: u32  // len==1: 0 (sin uso). len>=2: tamaño en bytes del chunk EF serializado

[padding de alineación + ef_blob]
  cada chunk EF (uno por cuenta con len>=2) es un `EfSeqDict<u64>` serializado
  con epserde de forma independiente. epserde valida en `deserialize_eps` que
  la dirección de memoria del chunk esté alineada (requerido por sus campos
  internos tipo `Box<[usize]>`/`Box<[u64]>`); como varios chunks conviven en
  el mismo mmap, CADA chunk arranca en un offset de archivo múltiplo de 64
  bytes. El offset absoluto del blob se deriva de forma determinista
  (`HEADER_LEN + account_count*48`, redondeado hacia arriba a 64) — no se
  guarda aparte, reader y writer lo recalculan igual.
```

Localización de una cuenta = binary search sobre el directorio (48 bytes/entrada) — igual que en el diseño original, sin perfect hashing.

**No implementado a propósito:** ninguna estructura EF para `len==1` (ni siquiera "más chica" — se probó y el overhead fijo de epserde es ~600+ bytes por chunk independientemente de si `n=1` o `n=5`, ver `data/out/benchmark_report.json` y el hallazgo del §7 abajo).

**Manifest** (`manifest.json`, separado del binario):

```json
{
  "format_version": 1,
  "network": "pubnet",
  "window_id": "w1_early | w2_soroban_launch | w3_recent",
  "ledger_start": 0,
  "ledger_end": 0,
  "definition_of_appearance": "v1-broad-classic",
  "account_granularity": "ed25519_account_id",
  "include_failed_transactions": true,
  "source": "s3://aws-public-blockchain/v1.1/stellar/ledgers/pubnet",
  "source_object_keys_hash": "sha256:...",
  "canonical_dataset_hash": "sha256:...",
  "stellar_xdr_crate_version": "28.0.0",
  "index_hash": "sha256:...",
  "account_count": 0,
  "created_at": "..."
}
```

`canonical_dataset_hash` es la pieza clave de la garantía de fairness (§3): es el hash del dataset intermedio único, y las tres representaciones (raw, Postgres, Elias-Fano) deben declarar el mismo valor para ser comparables.

### Crate para Elias-Fano: `sux` + `epserde` (usado, confirmado en Etapa 2)

Soporta mmap real vía feature `mmap`, aunque el propio proyecto se describe como "ε-copy" (casi-zero-copy, no zero-copy puro) — eso se reporta así en el benchmark, sin vender "latencia cero". `EfSeqDict<u64>` (builder `build_with_seq_and_dict()`) da `get`/`iter`/`iter_from`/`succ`/`pred` — necesario para el range query.

**Gotcha real encontrado y resuelto:** `deserialize_eps` valida que la dirección de memoria del chunk esté alineada (campos internos `Box<[usize]>`/`Box<[u64]>`). Como el índice V0 embebe muchos chunks EF independientes uno detrás de otro en el mismo archivo/mmap, el primer intento (offsets sin alinear) reventó con `AlignmentError` en la primera cuenta `warm` real. Solución: cada chunk arranca en un offset de archivo múltiplo de 64 bytes (`format::hybrid::CHUNK_ALIGN`), calculado de forma determinista por writer y reader. Esto es parte de por qué el índice compacto terminó más grande de lo esperado (ver resultados).

Alternativa de respaldo no usada: `sucds` (más maduro, pero sin mmap real — copia a heap al deserializar).

---

## 3. Dataset canónico intermedio — garantía de fairness entre baselines

Para que raw `Vec<u64>`, PostgreSQL y Elias-Fano midan **solo** la diferencia de representación/motor de almacenamiento (y no diferencias de extracción), el builder se divide en dos etapas estrictamente separadas:

**Etapa 1 — Extractor** (una sola vez por ventana): parsea los `.xdr.zst` de Galexie para el rango de la ventana, aplica las reglas §1, deduplica por ledger (§1.3), y escribe **un único archivo canónico**: una lista ordenada de tuplas `(account_id: [u8;32], ledger_seq: u32)`, sin duplicados, en un archivo binario plano de registros de 36 bytes. Se calcula y persiste su hash (`canonical_dataset_hash`).

**Etapa 2 — Encoders** (tres, independientes): raw-encoder, postgres-loader, y ef-encoder. **Ninguno de los tres vuelve a tocar XDR de Galexie** — los tres leen exclusivamente el archivo canónico de la Etapa 1 y producen su representación. Cada uno registra en su propio log/manifest el `canonical_dataset_hash` que consumió, para poder verificar después que los tres midieron sobre exactamente los mismos datos.

Esta separación es la respuesta directa a "cómo se garantiza que los tres reciban exactamente el mismo conjunto de datos": no es una promesa, es una restricción de arquitectura — los encoders no tienen acceso al parser de Galexie en absoluto.

---

## 4. Ventanas de benchmark

Tres ventanas de épocas distintas, cada una con un **conteo fijo de ledgers** (a definir el número exacto al implementar, ej. 5.000) — fijo en cantidad de ledgers, no en duración de reloj, porque el ritmo de cierre de ledger varió históricamente:

| Ventana | Época objetivo | Cómo se resuelve el rango |
|---|---|---|
| `w1_early` | Red clásica pre-Soroban (ej. ~2019) | Resolver fecha calendario → ledger sequence más cercano vía una API pública (ej. Horizon `/ledgers?order=asc&cursor=...` filtrando por `closed_at`), **no asumir** 5s/ledger constante — el ritmo temprano de la red no era uniforme. Registrar el ledger exacto y su `closed_at` en el manifest. |
| `w2_soroban_launch` | Alrededor del lanzamiento de Soroban en pubnet (~2023) | Mismo método de resolución fecha→ledger. |
| `w3_recent` | Actual | Los ledgers más recientes disponibles en el datastore al momento de correr el experimento. |

Los resultados se reportan **por ventana**, nunca promediados entre ventanas — igual que el benchmark ya exige reportar por categoría de cuenta (fría/p50/p95/más activa) en vez de un agregado ciego.

### Pendiente antes de implementar el extractor
Resolver los ledgers exactos de anclaje para `w1_early` y `w2_soroban_launch` (fechas calendario específicas) es parte de la Etapa 1, no de este documento — se hace con una consulta real, no se estima a ojo.

---

## 5. Supuestos explícitos que quedan registrados (no escondidos)

- `include_failed_transactions: true` (§1.2) — cambia densidad medible, se puede repetir el experimento con `false` si el resultado es sensible a esto.
- `account_granularity: ed25519_account_id` (§1.1) — sub-cuentas muxeadas colapsan a la cuenta base.
- Elección de crate `sux`+`epserde` sobre `sucds` — trade-off mmap real vs. madurez del crate.
- La regla E (claimant) sólo captura al destinatario declarado en `CREATE`/`UPDATE` de un claimable balance, no a quien efectivamente lo reclama (ese queda cubierto por la regla F, como source account de la operación `ClaimClaimableBalance`).
