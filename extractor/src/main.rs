//! Etapa 1 (Extractor) del Compact Stellar Historical Index — V0.
//!
//! Baja una ventana real de ledgers de pubnet desde el datastore público de
//! Galexie, aplica `definition_of_appearance: v1-broad-classic` (ver SPEC.md)
//! y emite el dataset canónico `tuples.bin` + `tuples.hash`, más estadísticas
//! de la ventana. No construye Elias-Fano ni toca Postgres (eso es Etapa 2).

use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::PathBuf;
use std::time::Instant;

use rayon::prelude::*;
use sha2::{Digest, Sha256};
use stellar_xdr::{
    Claimant, LedgerCloseMeta, LedgerCloseMetaBatch, LedgerEntryChange, LedgerEntryData,
    LedgerKey, Limits, MuxedAccount, PublicKey, ReadXdr, TransactionEnvelope,
    TransactionMeta, TransactionPhase,
};

const DATASTORE_BASE: &str = "https://aws-public-blockchain.s3.amazonaws.com/v1.1/stellar/ledgers/pubnet";
const LEDGERS_PER_PARTITION: u32 = 64_000;

fn env_u32(name: &str, default: u32) -> u32 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Ventana w3_recent: los 5000 ledgers mas recientes vistos en el datastore
/// al momento de resolver el rango (2026-09-27, ledger 64653374 = tip).
/// Overridable via WINDOW_START/WINDOW_END para correr otras ventanas.
fn window() -> (u32, u32) {
    let end = env_u32("WINDOW_END", 64_653_374);
    let start = env_u32("WINDOW_START", end - 4_999);
    (start, end)
}

fn partition_bounds(ledger_seq: u32) -> (u32, u32) {
    let start = (ledger_seq / LEDGERS_PER_PARTITION) * LEDGERS_PER_PARTITION;
    (start, start + LEDGERS_PER_PARTITION - 1)
}

fn hex_inv(n: u32) -> String {
    format!("{:08X}", u32::MAX - n)
}

fn galexie_url(ledger_seq: u32) -> String {
    let (p_start, p_end) = partition_bounds(ledger_seq);
    format!(
        "{DATASTORE_BASE}/{}--{}-{}/{}--{}.xdr.zst",
        hex_inv(p_start),
        p_start,
        p_end,
        hex_inv(ledger_seq),
        ledger_seq
    )
}

fn raw_cache_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../data/raw/pubnet")
}

fn cache_path(ledger_seq: u32) -> PathBuf {
    raw_cache_dir().join(format!("{ledger_seq}.xdr.zst"))
}

fn download_window(start: u32, end: u32) {
    fs::create_dir_all(raw_cache_dir()).expect("create raw cache dir");

    let missing: Vec<u32> = (start..=end)
        .filter(|&seq| !cache_path(seq).exists())
        .collect();
    if missing.is_empty() {
        println!("[download] {} archivos ya en cache, nada que bajar", end - start + 1);
        return;
    }
    println!(
        "[download] {} archivos en cache, {} a descargar",
        (end - start + 1) as usize - missing.len(),
        missing.len()
    );

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(24)
        .build()
        .expect("build download thread pool");

    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .expect("build http client");

    let done = std::sync::atomic::AtomicUsize::new(0);
    let total = missing.len();
    let started = Instant::now();

    pool.install(|| {
        missing.par_iter().for_each(|&seq| {
            let url = galexie_url(seq);
            let mut attempt = 0;
            loop {
                attempt += 1;
                match client.get(&url).send().and_then(|r| r.error_for_status()) {
                    Ok(resp) => match resp.bytes() {
                        Ok(bytes) => {
                            fs::write(cache_path(seq), &bytes).expect("write cache file");
                            break;
                        }
                        Err(e) if attempt < 5 => {
                            eprintln!("[download] retry ledger {seq} (body read: {e})");
                            continue;
                        }
                        Err(e) => panic!("ledger {seq}: failed reading body after retries: {e}"),
                    },
                    Err(e) if attempt < 5 => {
                        eprintln!("[download] retry ledger {seq} ({e})");
                        continue;
                    }
                    Err(e) => panic!("ledger {seq}: download failed after retries: {e} (url={url})"),
                }
            }
            let n = done.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
            if n % 500 == 0 || n == total {
                println!("[download] {n}/{total} ({:.1}s)", started.elapsed().as_secs_f64());
            }
        });
    });
}

fn account_bytes(pk: &PublicKey) -> [u8; 32] {
    match pk {
        PublicKey::PublicKeyTypeEd25519(u) => u.0,
    }
}

fn muxed_account_bytes(m: &MuxedAccount) -> [u8; 32] {
    match m {
        MuxedAccount::Ed25519(u) => u.0,
        MuxedAccount::MuxedEd25519(m) => m.ed25519.0,
    }
}

/// Reglas A-D: cualquier LedgerEntryChange de tipo ACCOUNT/TRUSTLINE/OFFER/DATA.
/// Regla E: claimants de CLAIMABLE_BALANCE en variantes Created/Updated/State/Restored.
fn accounts_from_change(change: &LedgerEntryChange, out: &mut HashSet<[u8; 32]>) {
    match change {
        LedgerEntryChange::Removed(key) => match key {
            LedgerKey::Account(k) => {
                out.insert(account_bytes(&k.account_id.0));
            }
            LedgerKey::Trustline(k) => {
                out.insert(account_bytes(&k.account_id.0));
            }
            LedgerKey::Offer(k) => {
                out.insert(account_bytes(&k.seller_id.0));
            }
            LedgerKey::Data(k) => {
                out.insert(account_bytes(&k.account_id.0));
            }
            // CLAIMABLE_BALANCE removed: la key solo trae balanceId, sin AccountId
            // (ver SPEC.md §1, regla E) — deliberadamente no capturado aca.
            _ => {}
        },
        LedgerEntryChange::Created(e) | LedgerEntryChange::Updated(e)
        | LedgerEntryChange::State(e) | LedgerEntryChange::Restored(e) => match &e.data {
            LedgerEntryData::Account(a) => {
                out.insert(account_bytes(&a.account_id.0));
            }
            LedgerEntryData::Trustline(t) => {
                out.insert(account_bytes(&t.account_id.0));
            }
            LedgerEntryData::Offer(o) => {
                out.insert(account_bytes(&o.seller_id.0));
            }
            LedgerEntryData::Data(d) => {
                out.insert(account_bytes(&d.account_id.0));
            }
            LedgerEntryData::ClaimableBalance(cb) => {
                for c in cb.claimants.iter() {
                    let Claimant::ClaimantTypeV0(v0) = c;
                    out.insert(account_bytes(&v0.destination.0));
                }
            }
            _ => {}
        },
    }
}

/// Regla F: source account de tx/operacion, incluyendo tx fallidas y fee-bump.
fn accounts_from_envelope(env: &TransactionEnvelope, out: &mut HashSet<[u8; 32]>) {
    match env {
        TransactionEnvelope::TxV0(v0) => {
            out.insert(v0.tx.source_account_ed25519.0);
            for op in v0.tx.operations.iter() {
                if let Some(src) = &op.source_account {
                    out.insert(muxed_account_bytes(src));
                }
            }
        }
        TransactionEnvelope::Tx(v1) => {
            out.insert(muxed_account_bytes(&v1.tx.source_account));
            for op in v1.tx.operations.iter() {
                if let Some(src) = &op.source_account {
                    out.insert(muxed_account_bytes(src));
                }
            }
        }
        TransactionEnvelope::TxFeeBump(fb) => {
            out.insert(muxed_account_bytes(&fb.tx.fee_source));
            let stellar_xdr::FeeBumpTransactionInnerTx::Tx(inner) = &fb.tx.inner_tx;
            accounts_from_envelope(&TransactionEnvelope::Tx(inner.clone()), out);
        }
    }
}

fn envelopes_from_phase(phase: &TransactionPhase, out: &mut Vec<TransactionEnvelope>) {
    match phase {
        TransactionPhase::V0(components) => {
            for c in components.iter() {
                let stellar_xdr::TxSetComponent::TxsetCompTxsMaybeDiscountedFee(c) = c;
                out.extend(c.txs.iter().cloned());
            }
        }
        TransactionPhase::V1(parallel) => {
            for stage in parallel.execution_stages.iter() {
                for cluster in stage.0.iter() {
                    out.extend(cluster.0.iter().cloned());
                }
            }
        }
    }
}

/// Aplica reglas A-F a un ledger y devuelve el set deduplicado de cuentas tocadas
/// (SPEC.md §1.3).
fn touched_accounts(lcm: &LedgerCloseMeta) -> HashSet<[u8; 32]> {
    let mut out = HashSet::new();

    // Reglas A-E: LedgerEntryChanges de cada operacion (+ fee processing).
    let tx_processing_changes: Vec<&TransactionMeta> = match lcm {
        LedgerCloseMeta::V0(v0) => v0
            .tx_processing
            .iter()
            .map(|trm| &trm.tx_apply_processing)
            .collect(),
        LedgerCloseMeta::V1(v1) => v1
            .tx_processing
            .iter()
            .map(|trm| &trm.tx_apply_processing)
            .collect(),
        LedgerCloseMeta::V2(v2) => v2
            .tx_processing
            .iter()
            .map(|trm| &trm.tx_apply_processing)
            .collect(),
    };
    for meta in tx_processing_changes {
        match meta {
            TransactionMeta::V0(ops) => {
                for op in ops.iter() {
                    for ch in op.changes.0.iter() {
                        accounts_from_change(ch, &mut out);
                    }
                }
            }
            TransactionMeta::V1(v1) => {
                for ch in v1.tx_changes.0.iter() {
                    accounts_from_change(ch, &mut out);
                }
                for op in v1.operations.iter() {
                    for ch in op.changes.0.iter() {
                        accounts_from_change(ch, &mut out);
                    }
                }
            }
            TransactionMeta::V2(v2) => {
                for ch in v2.tx_changes_before.0.iter() {
                    accounts_from_change(ch, &mut out);
                }
                for op in v2.operations.iter() {
                    for ch in op.changes.0.iter() {
                        accounts_from_change(ch, &mut out);
                    }
                }
                for ch in v2.tx_changes_after.0.iter() {
                    accounts_from_change(ch, &mut out);
                }
            }
            TransactionMeta::V3(v3) => {
                for ch in v3.tx_changes_before.0.iter() {
                    accounts_from_change(ch, &mut out);
                }
                for op in v3.operations.iter() {
                    for ch in op.changes.0.iter() {
                        accounts_from_change(ch, &mut out);
                    }
                }
                for ch in v3.tx_changes_after.0.iter() {
                    accounts_from_change(ch, &mut out);
                }
            }
            TransactionMeta::V4(v4) => {
                for ch in v4.tx_changes_before.0.iter() {
                    accounts_from_change(ch, &mut out);
                }
                for op in v4.operations.iter() {
                    for ch in op.changes.0.iter() {
                        accounts_from_change(ch, &mut out);
                    }
                }
                for ch in v4.tx_changes_after.0.iter() {
                    accounts_from_change(ch, &mut out);
                }
            }
        }
    }

    // Regla F: source accounts de cada tx/operacion incluida en el ledger,
    // exito o fallo (SPEC.md §1.2), leyendo el tx set (no el tx_processing).
    let mut envelopes = Vec::new();
    match lcm {
        LedgerCloseMeta::V0(v0) => envelopes.extend(v0.tx_set.txs.iter().cloned()),
        LedgerCloseMeta::V1(v1) => match &v1.tx_set {
            stellar_xdr::GeneralizedTransactionSet::V1(set) => {
                for phase in set.phases.iter() {
                    envelopes_from_phase(phase, &mut envelopes);
                }
            }
        },
        LedgerCloseMeta::V2(v2) => match &v2.tx_set {
            stellar_xdr::GeneralizedTransactionSet::V1(set) => {
                for phase in set.phases.iter() {
                    envelopes_from_phase(phase, &mut envelopes);
                }
            }
        },
    }
    for env in &envelopes {
        accounts_from_envelope(env, &mut out);
    }

    out
}

fn percentile(sorted: &[usize], p: f64) -> usize {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn main() {
    let (start, end) = window();
    let ledger_count = end - start + 1;
    println!("[window] w3_recent: ledgers {start}..={end} ({ledger_count} ledgers)");

    download_window(start, end);

    println!("[parse] decodificando LedgerCloseMeta y aplicando definition_of_appearance v1-broad-classic...");
    let t0 = Instant::now();

    // Paso paralelo (CPU-bound: zstd decompress + XDR decode + reglas A-F),
    // uno por ledger, sin estado compartido.
    let per_ledger: Vec<(u32, HashSet<[u8; 32]>)> = (start..=end)
        .into_par_iter()
        .map(|seq| {
            let raw = fs::read(cache_path(seq))
                .unwrap_or_else(|e| panic!("read cache for ledger {seq}: {e}"));
            let decompressed =
                zstd::decode_all(&raw[..]).unwrap_or_else(|e| panic!("zstd decode ledger {seq}: {e}"));
            let batch = LedgerCloseMetaBatch::from_xdr(&decompressed, Limits::none())
                .unwrap_or_else(|e| panic!("xdr decode ledger {seq}: {e}"));
            assert_eq!(batch.start_sequence, seq, "unexpected batch start for ledger {seq}");
            assert_eq!(batch.end_sequence, seq, "unexpected batch end for ledger {seq}");
            assert_eq!(batch.ledger_close_metas.len(), 1, "expected 1 LCM per file for ledger {seq}");
            let touched = touched_accounts(&batch.ledger_close_metas[0]);
            (seq, touched)
        })
        .collect();

    println!(
        "[parse] {} ledgers procesados en {:.1}s",
        per_ledger.len(),
        t0.elapsed().as_secs_f64()
    );

    // Merge secuencial en orden creciente de ledger: garantiza que cada
    // posting list quede ordenada (SPEC.md §1.3) sin necesidad de sort final.
    let mut by_ledger = per_ledger;
    by_ledger.sort_by_key(|(seq, _)| *seq);

    let mut accounts: BTreeMap<[u8; 32], Vec<u32>> = BTreeMap::new();
    for (seq, touched) in &by_ledger {
        for acct in touched {
            accounts.entry(*acct).or_default().push(*seq);
        }
    }

    let out_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../data/out");
    fs::create_dir_all(&out_dir).expect("create out dir");
    let tuples_path = out_dir.join("tuples.bin");

    // tuples.bin: registros de 36 bytes (32 account_id BE + 4 ledger_seq BE),
    // ordenados por account_id asc, y dentro de cada cuenta por ledger asc.
    let mut buf = Vec::with_capacity(accounts.values().map(|v| v.len()).sum::<usize>() * 36);
    let mut num_pairs: u64 = 0;
    for (acct, ledgers) in &accounts {
        for &seq in ledgers {
            buf.extend_from_slice(acct);
            buf.extend_from_slice(&seq.to_be_bytes());
            num_pairs += 1;
        }
    }
    fs::write(&tuples_path, &buf).expect("write tuples.bin");

    let mut hasher = Sha256::new();
    hasher.update(&buf);
    let hash = hasher.finalize();
    let hash_hex = hex::encode(hash);
    fs::write(out_dir.join("tuples.hash"), format!("sha256:{hash_hex}\n"))
        .expect("write tuples.hash");

    let mut lengths: Vec<usize> = accounts.values().map(|v| v.len()).collect();
    lengths.sort_unstable();
    let median = percentile(&lengths, 0.5);
    let p95 = percentile(&lengths, 0.95);
    let max = *lengths.last().unwrap_or(&0);

    let summary = serde_json::json!({
        "window_id": "w3_recent",
        "ledger_start": start,
        "ledger_end": end,
        "ledger_count": ledger_count,
        "definition_of_appearance": "v1-broad-classic",
        "account_count": accounts.len(),
        "pair_count": num_pairs,
        "posting_list_len_median": median,
        "posting_list_len_p95": p95,
        "posting_list_len_max": max,
        "tuples_hash": format!("sha256:{hash_hex}"),
        "tuples_bytes": buf.len(),
    });
    fs::write(
        out_dir.join("manifest.json"),
        serde_json::to_string_pretty(&summary).unwrap(),
    )
    .expect("write manifest.json");

    println!();
    println!("=== Etapa 1 — resultado (ventana w3_recent) ===");
    println!("ledgers procesados     : {ledger_count} ({start}..={end})");
    println!("#accounts (distintas)  : {}", accounts.len());
    println!("#pares (account,ledger): {num_pairs}");
    println!("posting-list len median: {median}");
    println!("posting-list len p95   : {p95}");
    println!("posting-list len max   : {max}");
    println!("tuples.bin              : {} bytes", buf.len());
    println!("tuples.hash             : sha256:{hash_hex}");
    println!("-> {}", tuples_path.display());
}
