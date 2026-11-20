//! Turns grid edits into SQL. Rows are identified by primary key, and every
//! UPDATE also checks the edited column still holds the value the grid showed,
//! so a concurrent change makes the commit fail instead of being overwritten.

use std::collections::{BTreeMap, HashMap};

use arrow_array::{
    Array, BooleanArray, Date32Array, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use arrow_schema::{DataType, TimeUnit};
use serde::{Deserialize, Serialize};
use sqlx::AssertSqlSafe;

use crate::config::DbKind;
use crate::db::columns::{format_date, format_ts};
use crate::db::{Pool, TableStructure, param, qualified, quote_ident};

#[derive(Debug, Clone, Deserialize)]
pub struct CellEdit {
    pub row: usize,
    pub column: String,
    pub value: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct Changes {
    #[serde(default)]
    pub edits: Vec<CellEdit>,
    #[serde(default)]
    pub deletes: Vec<usize>,
    #[serde(default)]
    pub inserts: Vec<HashMap<String, Option<String>>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CommitOutcome {
    pub statements: Vec<String>,
    pub rows_affected: u64,
}

pub struct Stmt {
    pub sql: String,
    pub binds: Vec<Option<String>>,
    pub expect_one: bool,
}

/// The exact value a cell held, as text the database will parse back.
pub fn arrow_text(batch: &RecordBatch, col: usize, row: usize, kind: DbKind) -> Option<String> {
    let a = batch.column(col);
    if row >= a.len() || a.is_null(row) {
        return None;
    }
    Some(match a.data_type() {
        DataType::Int64 => a
            .as_any()
            .downcast_ref::<Int64Array>()?
            .value(row)
            .to_string(),
        DataType::Float64 => a
            .as_any()
            .downcast_ref::<Float64Array>()?
            .value(row)
            .to_string(),
        DataType::Boolean => a
            .as_any()
            .downcast_ref::<BooleanArray>()?
            .value(row)
            .to_string(),
        DataType::Date32 => format_date(a.as_any().downcast_ref::<Date32Array>()?.value(row)),
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let v = a
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()?
                .value(row);
            format_ts(v, tz.is_some() && kind == DbKind::Postgres)
        }
        DataType::Utf8 => a
            .as_any()
            .downcast_ref::<StringArray>()?
            .value(row)
            .to_string(),
        _ => return None,
    })
}

/// Whether the grid's text for this column type round-trips exactly, so it can
/// be used to detect a concurrent change.
fn checkable(data_type: &str) -> bool {
    let t = data_type.to_ascii_lowercase();
    if t.contains("[]")
        || t.contains("json")
        || t.contains("float")
        || t.contains("double")
        || t.contains("real")
        || t.contains("blob")
        || t.contains("binary")
        || t.contains("bytea")
        || t.contains("interval")
        || t.contains("money")
        || t.contains("bit")
        || t.starts_with("time ")
        || t == "time"
        || t.starts_with("time(")
        || t.contains("time without") && !t.contains("timestamp")
    {
        return false;
    }
    [
        "int",
        "serial",
        "numeric",
        "decimal",
        "char",
        "text",
        "uuid",
        "bool",
        "date",
        "timestamp",
        "datetime",
        "enum",
    ]
    .iter()
    .any(|k| t.contains(k))
}

fn display(sql: &str, binds: &[Option<String>]) -> String {
    let lits: Vec<String> = binds
        .iter()
        .map(|b| match b {
            None => "NULL".into(),
            Some(s) => format!("'{}'", s.replace('\'', "''")),
        })
        .collect();
    let mut out = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();
    let mut q = 0usize;
    while let Some(c) = chars.next() {
        if c == '$' || (c == '?' && chars.peek().is_some_and(|d| d.is_ascii_digit())) {
            let mut n = String::new();
            while let Some(d) = chars.peek().filter(|d| d.is_ascii_digit()) {
                n.push(*d);
                chars.next();
            }
            let i: usize = n.parse().unwrap_or(0);
            out.push_str(
                lits.get(i.wrapping_sub(1))
                    .map(String::as_str)
                    .unwrap_or("?"),
            );
        } else if c == '?' {
            out.push_str(lits.get(q).map(String::as_str).unwrap_or("?"));
            q += 1;
        } else {
            out.push(c);
        }
    }
    out
}

pub fn plan(
    kind: DbKind,
    schema: &str,
    table: &str,
    st: &TableStructure,
    batch: Option<&RecordBatch>,
    changes: &Changes,
) -> anyhow::Result<Vec<Stmt>> {
    let q = qualified(kind, schema, table);
    let pk = st.pk();
    let col_index = |name: &str| -> anyhow::Result<usize> {
        batch
            .and_then(|b| b.schema().index_of(name).ok())
            .ok_or_else(|| anyhow::anyhow!("column {name} is not in the loaded rows"))
    };
    if (!changes.edits.is_empty() || !changes.deletes.is_empty()) && pk.is_empty() {
        anyhow::bail!("{table} has no primary key, so rows cannot be edited or deleted safely");
    }
    let pk_where = |row: usize, binds: &mut Vec<Option<String>>| -> anyhow::Result<String> {
        let b = batch.ok_or_else(|| anyhow::anyhow!("no loaded rows"))?;
        let mut parts = vec![];
        for c in &pk {
            let v = arrow_text(b, col_index(&c.name)?, row, kind).ok_or_else(|| {
                anyhow::anyhow!("row {} has no value for key column {}", row + 1, c.name)
            })?;
            binds.push(Some(v));
            parts.push(format!(
                "{} = {}",
                quote_ident(kind, &c.name),
                param(kind, binds.len(), Some(&c.data_type))
            ));
        }
        Ok(parts.join(" AND "))
    };

    let mut stmts = vec![];
    let mut by_row: BTreeMap<usize, Vec<&CellEdit>> = BTreeMap::new();
    for e in &changes.edits {
        if !changes.deletes.contains(&e.row) {
            by_row.entry(e.row).or_default().push(e);
        }
    }
    for (row, edits) in by_row {
        let mut binds = vec![];
        let mut sets = vec![];
        let mut checks = vec![];
        for e in &edits {
            let col = st
                .column(&e.column)
                .ok_or_else(|| anyhow::anyhow!("unknown column {}", e.column))?;
            binds.push(e.value.clone());
            sets.push(format!(
                "{} = {}",
                quote_ident(kind, &col.name),
                param(kind, binds.len(), Some(&col.data_type))
            ));
        }
        let mut wh = pk_where(row, &mut binds)?;
        for e in &edits {
            let col = st.column(&e.column).unwrap();
            if col.is_pk || !checkable(&col.data_type) {
                continue;
            }
            let old = arrow_text(batch.unwrap(), col_index(&col.name)?, row, kind);
            let qc = quote_ident(kind, &col.name);
            match old {
                None => checks.push(format!("{qc} IS NULL")),
                Some(v) => {
                    binds.push(Some(v));
                    let p = param(kind, binds.len(), Some(&col.data_type));
                    checks.push(match kind {
                        DbKind::Postgres => format!("{qc} IS NOT DISTINCT FROM {p}"),
                        DbKind::Mysql => format!("{qc} <=> {p}"),
                        DbKind::Sqlite => format!("{qc} IS {p}"),
                    });
                }
            }
        }
        for c in checks {
            wh.push_str(" AND ");
            wh.push_str(&c);
        }
        stmts.push(Stmt {
            sql: format!("UPDATE {q} SET {} WHERE {wh}", sets.join(", ")),
            binds,
            expect_one: true,
        });
    }
    for &row in &changes.deletes {
        let mut binds = vec![];
        let wh = pk_where(row, &mut binds)?;
        stmts.push(Stmt {
            sql: format!("DELETE FROM {q} WHERE {wh}"),
            binds,
            expect_one: true,
        });
    }
    for ins in &changes.inserts {
        let mut binds = vec![];
        let mut cols = vec![];
        let mut vals = vec![];
        for c in &st.columns {
            if let Some(v) = ins.get(&c.name) {
                binds.push(v.clone());
                cols.push(quote_ident(kind, &c.name));
                vals.push(param(kind, binds.len(), Some(&c.data_type)));
            }
        }
        let sql = if cols.is_empty() {
            match kind {
                DbKind::Mysql => format!("INSERT INTO {q} () VALUES ()"),
                _ => format!("INSERT INTO {q} DEFAULT VALUES"),
            }
        } else {
            format!(
                "INSERT INTO {q} ({}) VALUES ({})",
                cols.join(", "),
                vals.join(", ")
            )
        };
        stmts.push(Stmt {
            sql,
            binds,
            expect_one: true,
        });
    }
    Ok(stmts)
}

pub fn preview(stmts: &[Stmt]) -> Vec<String> {
    stmts.iter().map(|s| display(&s.sql, &s.binds)).collect()
}

macro_rules! run_tx {
    ($pool:expr, $db:ty, $stmts:expr) => {{
        let mut tx = $pool.begin().await?;
        let mut total = 0u64;
        for (i, s) in $stmts.iter().enumerate() {
            // Unnamed statements: named ones collide behind Postgres poolers.
            let mut q = sqlx::query::<$db>(AssertSqlSafe(s.sql.clone())).persistent(false);
            for b in &s.binds {
                q = q.bind(b.clone());
            }
            let n = q.execute(&mut *tx).await.map_err(|e| anyhow::anyhow!("statement {}: {e}", i + 1))?.rows_affected();
            if s.expect_one && n != 1 {
                tx.rollback().await?;
                anyhow::bail!(
                    "statement {} changed {n} rows, expected 1. The row was changed or deleted since it was loaded, so nothing was saved. Refresh and try again.",
                    i + 1
                );
            }
            total += n;
        }
        tx.commit().await?;
        Ok(total)
    }};
}

pub async fn execute(pool: &Pool, stmts: &[Stmt]) -> anyhow::Result<u64> {
    match pool {
        Pool::Pg(p) => run_tx!(p, sqlx::Postgres, stmts),
        Pool::My(p) => run_tx!(p, sqlx::MySql, stmts),
        Pool::Lite { rw, .. } => run_tx!(rw, sqlx::Sqlite, stmts),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::ColumnInfo;
    use crate::db::columns::{Cell, Collector};

    fn fixture() -> (TableStructure, RecordBatch) {
        let st = TableStructure {
            columns: vec![
                ColumnInfo {
                    name: "id".into(),
                    data_type: "integer".into(),
                    nullable: false,
                    default: None,
                    is_pk: true,
                    is_fk: false,
                },
                ColumnInfo {
                    name: "name".into(),
                    data_type: "text".into(),
                    nullable: true,
                    default: None,
                    is_pk: false,
                    is_fk: false,
                },
                ColumnInfo {
                    name: "score".into(),
                    data_type: "double precision".into(),
                    nullable: true,
                    default: None,
                    is_pk: false,
                    is_fk: false,
                },
            ],
            indexes: vec![],
        };
        let mut c = Collector::new(10, 1 << 20);
        c.set_columns(
            vec!["id".into(), "name".into(), "score".into()],
            vec!["INT4".into(), "TEXT".into(), "FLOAT8".into()],
        );
        c.push_row(vec![
            Cell::Int(7),
            Cell::Str("Ann".into()),
            Cell::Float(1.5),
        ]);
        let ipc = c.to_ipc().unwrap();
        let batch = arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(ipc), None)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        (st, batch)
    }

    #[test]
    fn update_checks_old_value_and_casts() {
        let (st, batch) = fixture();
        let ch = Changes {
            edits: vec![
                CellEdit {
                    row: 0,
                    column: "name".into(),
                    value: Some("Bo".into()),
                },
                CellEdit {
                    row: 0,
                    column: "score".into(),
                    value: None,
                },
            ],
            ..Default::default()
        };
        let s = plan(DbKind::Postgres, "public", "people", &st, Some(&batch), &ch).unwrap();
        assert_eq!(
            s[0].sql,
            "UPDATE \"public\".\"people\" SET \"name\" = CAST($1 AS text), \"score\" = CAST($2 AS double precision) \
             WHERE \"id\" = CAST($3 AS integer) AND \"name\" IS NOT DISTINCT FROM CAST($4 AS text)"
        );
        assert_eq!(
            s[0].binds,
            vec![
                Some("Bo".into()),
                None,
                Some("7".into()),
                Some("Ann".into())
            ]
        );
        assert_eq!(
            preview(&s)[0],
            "UPDATE \"public\".\"people\" SET \"name\" = CAST('Bo' AS text), \"score\" = CAST(NULL AS double precision) \
             WHERE \"id\" = CAST('7' AS integer) AND \"name\" IS NOT DISTINCT FROM CAST('Ann' AS text)"
        );
    }

    #[test]
    fn mysql_delete_and_insert() {
        let (st, batch) = fixture();
        let mut ins = HashMap::new();
        ins.insert("name".to_string(), Some("New".to_string()));
        let ch = Changes {
            deletes: vec![0],
            inserts: vec![ins, HashMap::new()],
            ..Default::default()
        };
        let s = plan(DbKind::Mysql, "shop", "people", &st, Some(&batch), &ch).unwrap();
        assert_eq!(s[0].sql, "DELETE FROM `shop`.`people` WHERE `id` = ?");
        assert_eq!(s[1].sql, "INSERT INTO `shop`.`people` (`name`) VALUES (?)");
        assert_eq!(s[2].sql, "INSERT INTO `shop`.`people` () VALUES ()");
    }

    #[test]
    fn no_primary_key_refuses_edits() {
        let (mut st, batch) = fixture();
        st.columns[0].is_pk = false;
        let ch = Changes {
            edits: vec![CellEdit {
                row: 0,
                column: "name".into(),
                value: None,
            }],
            ..Default::default()
        };
        assert!(plan(DbKind::Sqlite, "main", "people", &st, Some(&batch), &ch).is_err());
    }
}
