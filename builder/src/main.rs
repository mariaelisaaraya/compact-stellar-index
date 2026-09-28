//! Etapa 2 — builder: lee SOLO tuples.bin (el extracto canonico de Etapa 1),
//! verifica su hash, y emite las tres representaciones a comparar:
//!   - raw.bin      (Baseline A: Vec<u64> por cuenta, sin compresion)
//!   - compact.bin  (indice V0 hibrido: encoding = "raw_if_len1_else_ef")
//!   - Postgres     (tabla account_ledgers + indice btree compuesto)
//! No vuelve a tocar Galexie/XDR (SPEC.md §3).

use anyhow::{Context, Result, bail};
use postgres::binary_copy::BinaryCopyInWriter;
use postgres::types::Type;
use postgres::{Client, NoTls};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::PathBuf;
use std::time::Instant;

const EXPECTED_TUPLES_HASH: &str =
    "sha256:b51577aa57e579c52a70f200bbe0366156db1c990467fb3ce7a7fee4a35e6e62";

fn out_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../data/out")
}

/// Grupos (account_id, ledgers ordenados) leidos de tuples.bin, en el mismo
/// orden en que aparecen (account asc, ledger asc dentro de cada cuenta).
fn load_canonical_groups() -> Result<(u32, u32, Vec<([u8; 32], Vec<u32>)>)> {
    let tuples_path = out_dir().join("tuples.bin");
    let buf = fs::read(&tuples_path).context("leyendo tuples.bin")?;
    if buf.len() % 36 != 0 {
        bail!("tuples.bin: tamano no es multiplo de 36 bytes");
    }

    let mut hasher = Sha256::new();
    hasher.update(&buf);
    let hash = format!("sha256:{}", hex::encode(hasher.finalize()));
    if hash != EXPECTED_TUPLES_HASH {
        bail!(
            "tuples.bin no coincide con el hash canonico de Etapa 1.\n  esperado: {EXPECTED_TUPLES_HASH}\n  obtenido: {hash}\nNo se puede garantizar fairness entre representaciones (SPEC.md §3). Abortando."
        );
    }
    println!("[builder] tuples.bin verificado: {hash}");

    let manifest: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(out_dir().join("manifest.json"))?)?;
    let ledger_start = manifest["ledger_start"].as_u64().unwrap() as u32;
    let ledger_end = manifest["ledger_end"].as_u64().unwrap() as u32;

    let mut groups: Vec<([u8; 32], Vec<u32>)> = Vec::new();
    for chunk in buf.chunks_exact(36) {
        let mut account = [0u8; 32];
        account.copy_from_slice(&chunk[0..32]);
        let ledger = u32::from_be_bytes(chunk[32..36].try_into().unwrap());
        match groups.last_mut() {
            Some((acct, ledgers)) if *acct == account => ledgers.push(ledger),
            _ => groups.push((account, vec![ledger])),
        }
    }
    Ok((ledger_start, ledger_end, groups))
}

fn build_raw(ledger_start: u32, ledger_end: u32, groups: &[([u8; 32], Vec<u32>)]) -> Result<(f64, u64)> {
    let t0 = Instant::now();
    let mut w = format::raw::RawWriter::new(ledger_start, ledger_end);
    for (acct, ledgers) in groups {
        w.push_account(acct, ledgers);
    }
    let path = out_dir().join("raw.bin");
    w.write(&path)?;
    let elapsed = t0.elapsed().as_secs_f64();
    let bytes = fs::metadata(&path)?.len();
    Ok((elapsed, bytes))
}

fn build_hybrid(ledger_start: u32, ledger_end: u32, groups: &[([u8; 32], Vec<u32>)]) -> Result<(f64, u64)> {
    let t0 = Instant::now();
    let mut w = format::hybrid::HybridWriter::new(ledger_start, ledger_end);
    for (acct, ledgers) in groups {
        w.push_account(acct, ledgers)?;
    }
    let path = out_dir().join("compact.bin");
    w.write(&path)?;
    let elapsed = t0.elapsed().as_secs_f64();
    let bytes = fs::metadata(&path)?.len();
    Ok((elapsed, bytes))
}

fn build_postgres(groups: &[([u8; 32], Vec<u32>)]) -> Result<serde_json::Value> {
    let user = std::env::var("USER").context("USER env var")?;
    let conn_str = format!("user={user} dbname=postgres host=/tmp port=5432");
    let mut client = Client::connect(&conn_str, NoTls).context("conectando a Postgres local")?;

    client.batch_execute(
        "DROP TABLE IF EXISTS account_ledgers;
         CREATE TABLE account_ledgers (account bytea NOT NULL, ledger integer NOT NULL);",
    )?;

    let t_copy0 = Instant::now();
    {
        let sink = client.copy_in("COPY account_ledgers (account, ledger) FROM STDIN BINARY")?;
        let mut writer = BinaryCopyInWriter::new(sink, &[Type::BYTEA, Type::INT4]);
        for (acct, ledgers) in groups {
            let acct_slice: &[u8] = &acct[..];
            for &l in ledgers {
                let l = l as i32;
                writer.write(&[&acct_slice, &l])?;
            }
        }
        writer.finish()?;
    }
    let copy_secs = t_copy0.elapsed().as_secs_f64();

    let t_idx0 = Instant::now();
    client.batch_execute(
        "CREATE INDEX account_ledgers_account_ledger_idx ON account_ledgers (account, ledger);",
    )?;
    let index_secs = t_idx0.elapsed().as_secs_f64();

    let t_vac0 = Instant::now();
    client.batch_execute("VACUUM ANALYZE account_ledgers;")?;
    let vacuum_secs = t_vac0.elapsed().as_secs_f64();

    let row = client.query_one(
        "SELECT pg_total_relation_size('account_ledgers')",
        &[],
    )?;
    let total_bytes: i64 = row.get(0);

    Ok(serde_json::json!({
        "copy_seconds": copy_secs,
        "create_index_seconds": index_secs,
        "vacuum_analyze_seconds": vacuum_secs,
        "total_build_seconds": copy_secs + index_secs + vacuum_secs,
        "pg_total_relation_size_bytes": total_bytes,
        "schema": "CREATE TABLE account_ledgers (account bytea NOT NULL, ledger integer NOT NULL); CREATE INDEX ON account_ledgers (account, ledger);",
    }))
}

fn main() -> Result<()> {
    println!("[builder] cargando dataset canonico (tuples.bin)...");
    let (ledger_start, ledger_end, groups) = load_canonical_groups()?;
    println!(
        "[builder] {} cuentas cargadas (ledger_start={ledger_start} ledger_end={ledger_end})",
        groups.len()
    );

    println!("[builder] construyendo raw.bin (Baseline A)...");
    let (raw_secs, raw_bytes) = build_raw(ledger_start, ledger_end, &groups)?;
    println!("[builder]   raw.bin: {raw_bytes} bytes en {raw_secs:.2}s");

    println!("[builder] construyendo compact.bin (hibrido V0)...");
    let (hybrid_secs, hybrid_bytes) = build_hybrid(ledger_start, ledger_end, &groups)?;
    println!("[builder]   compact.bin: {hybrid_bytes} bytes en {hybrid_secs:.2}s");

    println!("[builder] cargando Postgres (account_ledgers)...");
    let pg = build_postgres(&groups)?;
    println!("[builder]   postgres: {pg}");

    let report = serde_json::json!({
        "canonical_dataset_hash": EXPECTED_TUPLES_HASH,
        "ledger_start": ledger_start,
        "ledger_end": ledger_end,
        "account_count": groups.len(),
        "raw": {"bytes": raw_bytes, "build_seconds": raw_secs},
        "hybrid": {"bytes": hybrid_bytes, "build_seconds": hybrid_secs, "encoding": format::hybrid::ENCODING_NAME},
        "postgres": pg,
    });
    fs::write(
        out_dir().join("build_report.json"),
        serde_json::to_string_pretty(&report)?,
    )?;
    println!("\n=== Etapa 2 (builder) — storage + build time ===");
    println!("raw.bin      : {raw_bytes:>12} bytes   build={raw_secs:.2}s");
    println!("compact.bin  : {hybrid_bytes:>12} bytes   build={hybrid_secs:.2}s");
    println!(
        "postgres     : {:>12} bytes   build={:.2}s (copy+index+vacuum)",
        pg["pg_total_relation_size_bytes"], pg["total_build_seconds"]
    );
    Ok(())
}
