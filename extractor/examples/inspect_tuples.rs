//! Sanity check ad-hoc: confirma que las cuentas en tuples.bin se pueden
//! nombrar como direcciones G... reales (strkey), no solo bytes crudos.
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

fn main() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../data/out/tuples.bin");
    let buf = fs::read(&path).expect("read tuples.bin");
    assert_eq!(buf.len() % 36, 0);

    let mut by_account: BTreeMap<[u8; 32], Vec<u32>> = BTreeMap::new();
    for chunk in buf.chunks_exact(36) {
        let mut acct = [0u8; 32];
        acct.copy_from_slice(&chunk[0..32]);
        let seq = u32::from_be_bytes(chunk[32..36].try_into().unwrap());
        by_account.entry(acct).or_default().push(seq);
    }

    let mut entries: Vec<(&[u8; 32], &Vec<u32>)> = by_account.iter().collect();
    entries.sort_by_key(|(_, v)| std::cmp::Reverse(v.len()));

    println!("Top 5 cuentas por longitud de posting list:");
    for (acct, ledgers) in entries.iter().take(5) {
        let g = stellar_strkey::ed25519::PublicKey(**acct).to_string();
        println!(
            "  {g}  len={}  first={}  last={}",
            ledgers.len(),
            ledgers.first().unwrap(),
            ledgers.last().unwrap()
        );
    }

    println!("\n5 cuentas con posting list de longitud 1 (cola larga):");
    for (acct, ledgers) in entries.iter().rev().take(5) {
        let g = stellar_strkey::ed25519::PublicKey(**acct).to_string();
        println!("  {g}  len={}  ledger={}", ledgers.len(), ledgers[0]);
    }
}
