//! Tipos y readers compartidos entre builder y benchmark (Etapa 2).
//!
//! Dos formatos, ambos con un directorio de cuentas (32 bytes raw, ordenado,
//! localizable por binary search) seguido de un blob de valores:
//!
//! - `raw`: Baseline A — cada posting list como `Vec<u64>` sin comprimir.
//! - `hybrid`: el indice compacto V0. encoding = "raw_if_len1_else_ef"
//!   (SPEC.md): len==1 -> valor crudo inline en el directorio; len>=2 ->
//!   Elias-Fano (sux::EfSeqDict, serializado con epserde) en un blob aparte.

use anyhow::{Context, Result, bail};
use memmap2::Mmap;
use std::fs::File;
use std::path::Path;
use sux::dict::elias_fano::{EfSeqDict, EliasFanoBuilder};
use sux::traits::Succ;

pub type Ef = EfSeqDict<u64>;

/// Bucket de reporte (SPEC.md): no promediar cold/warm/hot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Bucket {
    Cold, // len == 1
    Warm, // 2..=32
    Hot,  // > 32
}

impl Bucket {
    pub fn of(len: usize) -> Bucket {
        match len {
            0 => panic!("posting list vacia: no deberia existir en el directorio"),
            1 => Bucket::Cold,
            2..=32 => Bucket::Warm,
            _ => Bucket::Hot,
        }
    }
    pub fn label(self) -> &'static str {
        match self {
            Bucket::Cold => "cold",
            Bucket::Warm => "warm",
            Bucket::Hot => "hot",
        }
    }
}

fn binary_search_directory<'a>(
    dir_bytes: &'a [u8],
    stride: usize,
    account_count: usize,
    account: &[u8; 32],
) -> Option<&'a [u8]> {
    let mut lo = 0usize;
    let mut hi = account_count;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let entry = &dir_bytes[mid * stride..mid * stride + stride];
        let mid_account = &entry[0..32];
        match mid_account.cmp(account.as_slice()) {
            std::cmp::Ordering::Equal => return Some(entry),
            std::cmp::Ordering::Less => lo = mid + 1,
            std::cmp::Ordering::Greater => hi = mid,
        }
    }
    None
}

// ============================= raw.bin (Baseline A) =============================

pub mod raw {
    use super::*;

    pub const MAGIC: &[u8; 8] = b"CSIRAWV1";
    pub const HEADER_LEN: usize = 8 + 4 + 4 + 8; // magic + ledger_start + ledger_end + account_count
    pub const DIR_STRIDE: usize = 32 + 8 + 4; // account_id + offset(bytes) + len(u64 count)

    pub struct RawWriter {
        pub ledger_start: u32,
        pub ledger_end: u32,
        directory: Vec<u8>,
        values: Vec<u8>,
        account_count: u64,
    }

    impl RawWriter {
        pub fn new(ledger_start: u32, ledger_end: u32) -> Self {
            Self {
                ledger_start,
                ledger_end,
                directory: Vec::new(),
                values: Vec::new(),
                account_count: 0,
            }
        }

        /// `ledgers` debe venir ordenado ascendente (garantizado por el
        /// merge secuencial del extractor, SPEC.md 1.3).
        pub fn push_account(&mut self, account_id: &[u8; 32], ledgers: &[u32]) {
            let offset = self.values.len() as u64;
            self.directory.extend_from_slice(account_id);
            self.directory.extend_from_slice(&offset.to_le_bytes());
            self.directory
                .extend_from_slice(&(ledgers.len() as u32).to_le_bytes());
            for &l in ledgers {
                self.values.extend_from_slice(&(l as u64).to_le_bytes());
            }
            self.account_count += 1;
        }

        pub fn write(self, path: impl AsRef<Path>) -> Result<()> {
            use std::io::Write;
            let mut f = File::create(path)?;
            f.write_all(MAGIC)?;
            f.write_all(&self.ledger_start.to_le_bytes())?;
            f.write_all(&self.ledger_end.to_le_bytes())?;
            f.write_all(&self.account_count.to_le_bytes())?;
            f.write_all(&self.directory)?;
            f.write_all(&self.values)?;
            Ok(())
        }
    }

    pub struct RawIndex {
        mmap: Mmap,
        account_count: usize,
    }

    impl RawIndex {
        pub fn open(path: impl AsRef<Path>) -> Result<Self> {
            let f = File::open(path)?;
            let mmap = unsafe { Mmap::map(&f)? };
            if &mmap[0..8] != MAGIC {
                bail!("raw.bin: magic invalida");
            }
            let account_count =
                u64::from_le_bytes(mmap[16..24].try_into().unwrap()) as usize;
            Ok(Self { mmap, account_count })
        }

        fn directory(&self) -> &[u8] {
            &self.mmap[HEADER_LEN..HEADER_LEN + self.account_count * DIR_STRIDE]
        }

        fn values(&self) -> &[u8] {
            &self.mmap[HEADER_LEN + self.account_count * DIR_STRIDE..]
        }

        fn find(&self, account: &[u8; 32]) -> Option<(u64, u32)> {
            let entry = binary_search_directory(
                self.directory(),
                DIR_STRIDE,
                self.account_count,
                account,
            )?;
            let offset = u64::from_le_bytes(entry[32..40].try_into().unwrap());
            let len = u32::from_le_bytes(entry[40..44].try_into().unwrap());
            Some((offset, len))
        }

        pub fn get_ledgers(&self, account: &[u8; 32]) -> Option<Vec<u32>> {
            let (offset, len) = self.find(account)?;
            let values = self.values();
            let start = offset as usize;
            let mut out = Vec::with_capacity(len as usize);
            for i in 0..len as usize {
                let b = &values[start + i * 8..start + i * 8 + 8];
                out.push(u64::from_le_bytes(b.try_into().unwrap()) as u32);
            }
            Some(out)
        }

        pub fn get_ledgers_range(&self, account: &[u8; 32], from: u32, to: u32) -> Option<Vec<u32>> {
            Some(
                self.get_ledgers(account)?
                    .into_iter()
                    .filter(|&l| l >= from && l <= to)
                    .collect(),
            )
        }
    }
}

// ============================= compact.bin (V0 hibrido) =============================

pub mod hybrid {
    use super::*;

    pub const MAGIC: &[u8; 8] = b"CSIHYBV1";
    pub const HEADER_LEN: usize = 8 + 2 + 4 + 4 + 8; // magic+version+ledger_start+ledger_end+account_count
    pub const DIR_STRIDE: usize = 32 + 4 + 8 + 4; // account_id+len+payload+payload_len
    pub const ENCODING_NAME: &str = "raw_if_len1_else_ef";
    /// epserde valida que cada chunk deserializado via `deserialize_eps`
    /// caiga en una direccion de memoria alineada (ver
    /// `MemoryAlignment`/`Aligned64` en epserde). Como varios chunks EF
    /// independientes viven uno detras de otro en el mismo mmap, cada uno
    /// tiene que arrancar en un offset de archivo multiplo de esto.
    const CHUNK_ALIGN: usize = 64;

    fn pad_to(len: usize, align: usize) -> usize {
        (align - (len % align)) % align
    }

    fn ef_blob_file_start(account_count: usize) -> usize {
        let header_and_dir = HEADER_LEN + account_count * DIR_STRIDE;
        header_and_dir + pad_to(header_and_dir, CHUNK_ALIGN)
    }

    pub struct HybridWriter {
        pub ledger_start: u32,
        pub ledger_end: u32,
        directory: Vec<u8>,
        ef_blob: Vec<u8>,
        account_count: u64,
    }

    impl HybridWriter {
        pub fn new(ledger_start: u32, ledger_end: u32) -> Self {
            Self {
                ledger_start,
                ledger_end,
                directory: Vec::new(),
                ef_blob: Vec::new(),
                account_count: 0,
            }
        }

        /// `ledgers` ordenado ascendente, sin duplicados (SPEC.md 1.3).
        pub fn push_account(&mut self, account_id: &[u8; 32], ledgers: &[u32]) -> Result<()> {
            self.directory.extend_from_slice(account_id);
            self.directory
                .extend_from_slice(&(ledgers.len() as u32).to_le_bytes());

            if ledgers.len() == 1 {
                let payload = ledgers[0] as u64;
                self.directory.extend_from_slice(&payload.to_le_bytes());
                self.directory.extend_from_slice(&0u32.to_le_bytes());
            } else {
                let universe = (self.ledger_end - self.ledger_start) as u64 + 1;
                let mut efb = EliasFanoBuilder::new(ledgers.len(), universe);
                for &l in ledgers {
                    efb.push((l - self.ledger_start) as u64);
                }
                let ef: Ef = efb.build_with_seq_and_dict();
                let pad = pad_to(self.ef_blob.len(), CHUNK_ALIGN);
                self.ef_blob.extend(std::iter::repeat(0u8).take(pad));
                let offset = self.ef_blob.len() as u64;
                let before = self.ef_blob.len();
                unsafe {
                    use epserde::ser::Serialize;
                    ef.serialize(&mut self.ef_blob)
                        .context("epserde serialize de EfSeqDict fallo")?;
                }
                let chunk_len = (self.ef_blob.len() - before) as u32;
                self.directory.extend_from_slice(&offset.to_le_bytes());
                self.directory.extend_from_slice(&chunk_len.to_le_bytes());
            }
            self.account_count += 1;
            Ok(())
        }

        pub fn write(self, path: impl AsRef<Path>) -> Result<()> {
            use std::io::Write;
            let mut f = File::create(path)?;
            f.write_all(MAGIC)?;
            f.write_all(&1u16.to_le_bytes())?;
            f.write_all(&self.ledger_start.to_le_bytes())?;
            f.write_all(&self.ledger_end.to_le_bytes())?;
            f.write_all(&self.account_count.to_le_bytes())?;
            f.write_all(&self.directory)?;
            let leading_pad = pad_to(HEADER_LEN + self.directory.len(), CHUNK_ALIGN);
            f.write_all(&vec![0u8; leading_pad])?;
            f.write_all(&self.ef_blob)?;
            Ok(())
        }
    }

    pub struct HybridIndex {
        mmap: Mmap,
        account_count: usize,
        pub ledger_start: u32,
        pub ledger_end: u32,
    }

    impl HybridIndex {
        pub fn open(path: impl AsRef<Path>) -> Result<Self> {
            let f = File::open(path)?;
            let mmap = unsafe { Mmap::map(&f)? };
            if &mmap[0..8] != MAGIC {
                bail!("compact.bin: magic invalida");
            }
            let ledger_start = u32::from_le_bytes(mmap[10..14].try_into().unwrap());
            let ledger_end = u32::from_le_bytes(mmap[14..18].try_into().unwrap());
            let account_count = u64::from_le_bytes(mmap[18..26].try_into().unwrap()) as usize;
            Ok(Self {
                mmap,
                account_count,
                ledger_start,
                ledger_end,
            })
        }

        fn directory(&self) -> &[u8] {
            &self.mmap[HEADER_LEN..HEADER_LEN + self.account_count * DIR_STRIDE]
        }

        fn ef_blob(&self) -> &[u8] {
            &self.mmap[ef_blob_file_start(self.account_count)..]
        }

        fn find(&self, account: &[u8; 32]) -> Option<(u32, u64, u32)> {
            let entry = binary_search_directory(
                self.directory(),
                DIR_STRIDE,
                self.account_count,
                account,
            )?;
            let len = u32::from_le_bytes(entry[32..36].try_into().unwrap());
            let payload = u64::from_le_bytes(entry[36..44].try_into().unwrap());
            let payload_len = u32::from_le_bytes(entry[44..48].try_into().unwrap());
            Some((len, payload, payload_len))
        }

        pub fn get_ledgers(&self, account: &[u8; 32]) -> Option<Vec<u32>> {
            let (len, payload, payload_len) = self.find(account)?;
            if len == 1 {
                return Some(vec![payload as u32]);
            }
            let start = payload as usize;
            let bytes = &self.ef_blob()[start..start + payload_len as usize];
            let ef = unsafe {
                use epserde::deser::Deserialize;
                Ef::deserialize_eps(bytes).expect("deserialize_eps de chunk EF corrupto")
            };
            Some(
                ef.iter()
                    .map(|v| v as u32 + self.ledger_start)
                    .collect(),
            )
        }

        pub fn get_ledgers_range(&self, account: &[u8; 32], from: u32, to: u32) -> Option<Vec<u32>> {
            let (len, payload, payload_len) = self.find(account)?;
            if len == 1 {
                let v = payload as u32;
                return Some(if v >= from && v <= to { vec![v] } else { vec![] });
            }
            let start = payload as usize;
            let bytes = &self.ef_blob()[start..start + payload_len as usize];
            let ef = unsafe {
                use epserde::deser::Deserialize;
                Ef::deserialize_eps(bytes).expect("deserialize_eps de chunk EF corrupto")
            };
            let rel_from = from.saturating_sub(self.ledger_start) as u64;
            let rel_to = to.saturating_sub(self.ledger_start) as u64;
            let mut out = Vec::new();
            if let Some((idx, _)) = ef.succ(rel_from) {
                for v in ef.iter_from(idx) {
                    if v > rel_to {
                        break;
                    }
                    out.push(v as u32 + self.ledger_start);
                }
            }
            Some(out)
        }
    }
}
