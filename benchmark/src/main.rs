//! Etapa 2 — benchmark: compara raw.bin / compact.bin / Postgres sobre EL
//! MISMO dataset canonico (tuples.bin), muestreado en 3 buckets (SPEC.md):
//!   cold = len==1, warm = 2..=32, hot = >32
//! Reporta p50/p95 por bucket, nunca un promedio agregado.

use anyhow::{Context, Result};
use format::hybrid::HybridIndex;
use format::raw::RawIndex;
use format::Bucket;
use postgres::types::ToSql;
use postgres::{Client, NoTls};
use std::fs;
use std::path::PathBuf;
use std::time::Instant;

const REPS_PER_ACCOUNT: usize = 10;

fn out_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../data/out")
}

fn load_groups() -> Result<(u32, u32, Vec<([u8; 32], Vec<u32>)>)> {
    let buf = fs::read(out_dir().join("tuples.bin"))?;
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

/// Rango "media ventana central" de la propia posting list de la cuenta:
/// exercita un range query real (subset), no el caso degenerado full-list.
fn mid_range(ledgers: &[u32]) -> (u32, u32) {
    let first = *ledgers.first().unwrap();
    let last = *ledgers.last().unwrap();
    let span = last - first;
    (first + span / 4, last - span / 4)
}

/// Muestreo sistematico (indices espaciados uniformemente sobre la lista ya
/// ordenada por account_id, que no correlaciona con actividad): equivalente
/// a una muestra aleatoria simple sin depender de una crate de RNG.
fn systematic_sample<T: Clone>(items: &[T], want: usize) -> Vec<T> {
    if items.len() <= want {
        return items.to_vec();
    }
    (0..want)
        .map(|i| items[i * items.len() / want].clone())
        .collect()
}

fn percentile(sorted_us: &[f64], p: f64) -> f64 {
    if sorted_us.is_empty() {
        return 0.0;
    }
    let idx = ((sorted_us.len() as f64 - 1.0) * p).round() as usize;
    sorted_us[idx.min(sorted_us.len() - 1)]
}

struct Stats {
    n: usize,
    p50_us: f64,
    p95_us: f64,
}

fn stats(mut samples_us: Vec<f64>) -> Stats {
    samples_us.sort_by(|a, b| a.partial_cmp(b).unwrap());
    Stats {
        n: samples_us.len(),
        p50_us: percentile(&samples_us, 0.5),
        p95_us: percentile(&samples_us, 0.95),
    }
}

fn main() -> Result<()> {
    println!("[benchmark] cargando dataset canonico...");
    let (ledger_start, ledger_end, groups) = load_groups()?;

    let cold: Vec<&([u8; 32], Vec<u32>)> = groups
        .iter()
        .filter(|(_, l): &&([u8; 32], Vec<u32>)| l.len() == 1)
        .collect();
    let warm: Vec<&([u8; 32], Vec<u32>)> = groups
        .iter()
        .filter(|(_, l): &&([u8; 32], Vec<u32>)| (2..=32).contains(&l.len()))
        .collect();
    let mut hot: Vec<&([u8; 32], Vec<u32>)> = groups
        .iter()
        .filter(|(_, l): &&([u8; 32], Vec<u32>)| l.len() > 32)
        .collect();
    hot.sort_by_key(|(_, l): &&([u8; 32], Vec<u32>)| std::cmp::Reverse(l.len()));

    println!(
        "[benchmark] universo: cold={} warm={} hot={}",
        cold.len(),
        warm.len(),
        hot.len()
    );

    let cold_sample = systematic_sample(&cold, 200);
    let warm_sample = systematic_sample(&warm, 50);
    let hot_sample: Vec<&([u8; 32], Vec<u32>)> = hot.iter().take(100).cloned().collect();

    println!(
        "[benchmark] muestra: cold={} warm={} hot={} (reps/cuenta={REPS_PER_ACCOUNT})",
        cold_sample.len(),
        warm_sample.len(),
        hot_sample.len()
    );

    let raw = RawIndex::open(out_dir().join("raw.bin")).context("abriendo raw.bin")?;
    let hybrid = HybridIndex::open(out_dir().join("compact.bin")).context("abriendo compact.bin")?;

    let user = std::env::var("USER")?;
    let mut pg = Client::connect(&format!("user={user} dbname=postgres host=/tmp port=5432"), NoTls)
        .context("conectando a Postgres")?;
    let pg_point = pg.prepare("SELECT ledger FROM account_ledgers WHERE account = $1 ORDER BY ledger")?;
    let pg_range = pg.prepare(
        "SELECT ledger FROM account_ledgers WHERE account = $1 AND ledger >= $2 AND ledger <= $3 ORDER BY ledger",
    )?;

    // Plan real (no cronometrado) para dejar constancia de que Postgres usa
    // el btree compuesto y no un seq scan (SPEC.md exige verificar esto).
    if let Some((acct, _)) = hot.first() {
        let acct_slice: &[u8] = &acct[..];
        let plan_point: Vec<String> = pg
            .query(
                "EXPLAIN (ANALYZE, BUFFERS) SELECT ledger FROM account_ledgers WHERE account = $1 ORDER BY ledger",
                &[&acct_slice],
            )?
            .iter()
            .map(|r: &postgres::Row| r.get::<_, String>(0))
            .collect();
        fs::write(out_dir().join("postgres_explain_point.txt"), plan_point.join("\n"))?;
    }

    let buckets: [(Bucket, &[&([u8; 32], Vec<u32>)]); 3] = [
        (Bucket::Cold, &cold_sample),
        (Bucket::Warm, &warm_sample),
        (Bucket::Hot, &hot_sample),
    ];

    let mut report = serde_json::Map::new();
    report.insert(
        "reps_per_account".into(),
        serde_json::json!(REPS_PER_ACCOUNT),
    );
    report.insert(
        "sample_sizes".into(),
        serde_json::json!({"cold": cold_sample.len(), "warm": warm_sample.len(), "hot": hot_sample.len()}),
    );
    report.insert(
        "universe_sizes".into(),
        serde_json::json!({"cold": cold.len(), "warm": warm.len(), "hot": hot.len()}),
    );

    let mut table = String::new();
    table.push_str(
        "bucket  repr       query  n       p50_us    p95_us\n",
    );

    for (bucket, sample) in &buckets {
        if sample.is_empty() {
            continue;
        }
        // -------- raw.bin --------
        let mut raw_point = Vec::new();
        let mut raw_range = Vec::new();
        for (acct, ledgers) in sample.iter() {
            let (from, to) = mid_range(ledgers);
            for _ in 0..REPS_PER_ACCOUNT {
                let t0 = Instant::now();
                let _ = raw.get_ledgers(acct);
                raw_point.push(t0.elapsed().as_secs_f64() * 1e6);

                let t0 = Instant::now();
                let _ = raw.get_ledgers_range(acct, from, to);
                raw_range.push(t0.elapsed().as_secs_f64() * 1e6);
            }
        }

        // -------- compact.bin (hybrid) --------
        let mut hyb_point = Vec::new();
        let mut hyb_range = Vec::new();
        for (acct, ledgers) in sample.iter() {
            let (from, to) = mid_range(ledgers);
            for _ in 0..REPS_PER_ACCOUNT {
                let t0 = Instant::now();
                let _ = hybrid.get_ledgers(acct);
                hyb_point.push(t0.elapsed().as_secs_f64() * 1e6);

                let t0 = Instant::now();
                let _ = hybrid.get_ledgers_range(acct, from, to);
                hyb_range.push(t0.elapsed().as_secs_f64() * 1e6);
            }
        }

        // -------- postgres --------
        let mut pg_point_us = Vec::new();
        let mut pg_range_us = Vec::new();
        for (acct, ledgers) in sample.iter() {
            let (from, to) = mid_range(ledgers);
            let acct_slice: &[u8] = &acct[..];
            let from_i = from as i32;
            let to_i = to as i32;
            for _ in 0..REPS_PER_ACCOUNT {
                let t0 = Instant::now();
                let _ = pg.query(&pg_point, &[&acct_slice])?;
                pg_point_us.push(t0.elapsed().as_secs_f64() * 1e6);

                let params: [&(dyn ToSql + Sync); 3] = [&acct_slice, &from_i, &to_i];
                let t0 = Instant::now();
                let _ = pg.query(&pg_range, &params)?;
                pg_range_us.push(t0.elapsed().as_secs_f64() * 1e6);
            }
        }

        for (repr, point, range) in [
            ("raw", raw_point, raw_range),
            ("compact", hyb_point, hyb_range),
            ("postgres", pg_point_us, pg_range_us),
        ] {
            let sp = stats(point);
            let sr = stats(range);
            table.push_str(&format!(
                "{:<7} {:<9}  point  {:<6}  {:>8.2}  {:>8.2}\n",
                bucket.label(),
                repr,
                sp.n,
                sp.p50_us,
                sp.p95_us
            ));
            table.push_str(&format!(
                "{:<7} {:<9}  range  {:<6}  {:>8.2}  {:>8.2}\n",
                bucket.label(),
                repr,
                sr.n,
                sr.p50_us,
                sr.p95_us
            ));
            report.insert(
                format!("{}_{}_{}", bucket.label(), repr, "point"),
                serde_json::json!({"n": sp.n, "p50_us": sp.p50_us, "p95_us": sp.p95_us}),
            );
            report.insert(
                format!("{}_{}_{}", bucket.label(), repr, "range"),
                serde_json::json!({"n": sr.n, "p50_us": sr.p50_us, "p95_us": sr.p95_us}),
            );
        }
    }

    println!("\n=== Etapa 2 — benchmark query p50/p95 por bucket (microsegundos) ===");
    print!("{table}");

    fs::write(
        out_dir().join("benchmark_report.json"),
        serde_json::to_string_pretty(&serde_json::Value::Object(report))?,
    )?;
    fs::write(out_dir().join("benchmark_table.txt"), &table)?;

    let _ = (ledger_start, ledger_end);
    Ok(())
}
