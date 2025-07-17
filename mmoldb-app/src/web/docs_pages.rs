use std::path::PathBuf;
use include_dir::{include_dir, Dir};
use itertools::Itertools;
use miette::Diagnostic;
use rocket::{get, uri};
use rocket_dyn_templates::{context, Template};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use mmoldb_db::db;
use crate::Db;
use crate::web::error::AppError;
use super::pages::*;

static SCHEMA_DOCS_DIR: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/schema_docs");

#[derive(Debug, Error, Diagnostic)]
pub enum DocsError {
    #[error("Docs file {0} not found")]
    DocsFileMissing(PathBuf),

    #[error("Docs file {0} had no stem")]
    DocsFileHasNoStem(PathBuf),

    #[error("Docs file {0} a non-unicode file name")]
    NonUnicodeFileName(PathBuf),

    #[error("Couldn't deserialize docs file")]
    CouldntDeserializeDocsFile(#[source] toml::de::Error),
}

#[get("/docs")]
pub async fn docs_page() -> Result<Template, AppError> {
    #[derive(Debug, Serialize)]
    struct Column {
        name: String,
        r#type: String,
        description: String,
        nullable_explanation: Option<String>,
    }

    #[derive(Debug, Serialize)]
    struct Table {
        name: String,
        description: String,
        columns: Vec<Column>,
    }

    #[derive(Debug, Serialize)]
    struct Schema {
        display_order: Option<usize>,
        name: &'static str,
        description: String,
        tables: Vec<Table>,
    }

    let mut schemata = SCHEMA_DOCS_DIR.entries()
        .iter()
        .map(|entry| {
            let os_name = entry.path().file_stem()
                .ok_or(DocsError::DocsFileHasNoStem(entry.path().to_path_buf()))?;

            let name = os_name.to_str()
                .ok_or(DocsError::NonUnicodeFileName(entry.path().to_path_buf()))?;

            let file_contents = entry.as_file()
                .ok_or_else(|| DocsError::DocsFileMissing(entry.path().to_path_buf()))?
                .contents();

            let docs: SchemaDocs = toml::from_slice(file_contents)
                .map_err(DocsError::CouldntDeserializeDocsFile)?;

            Ok::<_, DocsError>((name, docs))
        })
        .map_ok(|(name, schema)| {
            Schema {
                display_order: schema.display_order,
                name,
                description: markdown::to_html(&schema.description),
                tables: schema.tables.into_iter()
                    .map(|table| Table {
                        name: table.name,
                        description: markdown::to_html(&table.description),
                        columns: table.columns.into_iter()
                            .map(|column| Column {
                                name: column.name,
                                r#type: column.r#type,
                                description: markdown::to_html(&column.description),
                                nullable_explanation: column.nullable_explanation.as_deref().map(markdown::to_html),
                            })
                            .collect(),
                    })
                    .collect(),
            }
        })
        .collect::<Result<Vec<_>, _>>()?;

    schemata.sort_by_key(|s| s.display_order.unwrap_or(usize::MAX));

    Ok(Template::render(
        "docs",
        context! {
            index_url: uri!(index_page()),
            status_url: uri!(status_page()),
            docs_url: uri!(docs_page()),
            schemata: schemata,
        },
    ))
}

#[get("/docs/<schema_name>")]
pub async fn docs_schema_page(schema_name: &str) -> Result<Template, AppError> {
    #[derive(Debug, Serialize)]
    struct Schema {
        description: String,
    }

    let schema_file = SCHEMA_DOCS_DIR.get_file(schema_name)
        .ok_or(DocsError::DocsFileMissing(schema_name.into()))?;

    let docs: SchemaDocs = toml::from_slice(schema_file.contents())
        .map_err(DocsError::CouldntDeserializeDocsFile)?;
    
    Ok(Template::render(
        "docs_table",
        context! {
            index_url: uri!(index_page()),
            status_url: uri!(status_page()),
            docs_url: uri!(docs_page()),
            table: docs,
        },
    ))
}

#[get("/docs/debug/<table_name>")]
pub async fn docs_debug_page(table_name: String, db: Db) -> Result<Template, AppError> {
    let data = db.run(move |conn| {
        db::tables_for_schema(conn, "mmoldb", &table_name)
    }).await?;

    Ok(Template::render(
        "json_renderer",
        context! {
                data: data,
            },
    ))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ColumnDocs {
    pub name: String,
    pub r#type: String,
    pub description: String,
    #[serde(default)]
    pub nullable_explanation: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TableDocs {
    pub name: String,
    pub description: String,
    // It's more ergonomic to use the singular in the TOML file
    #[serde(default, rename = "column")]
    pub columns: Vec<ColumnDocs>,
    #[serde(default)]
    pub allow_undocumented: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SchemaDocs {
    pub display_order: Option<usize>,
    pub description: String,
    // It's more ergonomic to use the singular in the TOML file
    #[serde(default, rename = "table")]
    pub tables: Vec<TableDocs>,
    #[serde(default)]
    pub allow_undocumented: bool,
}

// TODO Put this in some utils file somewhere
pub fn associate<T, S>(a: Vec<T>, mut b: Vec<S>, is_associated: impl Fn(&T, &S) -> bool) -> (Vec<T>, Vec<(T, S)>, Vec<S>) {
    let mut orphans_a = Vec::new();
    let mut pairs = Vec::new();

    for item_a in a {
        if let Some(item_b_position) = b.iter().position(|item_b| is_associated(&item_a, item_b)) {
            let item_b = b.remove(item_b_position);
            pairs.push((item_a, item_b));
        } else {
            orphans_a.push(item_a);
        }
    }

    // b is now orphans_b
    b.shrink_to_fit();

    (orphans_a, pairs, b)
}

#[cfg(test)]
mod tests {
    use crate::db::{DbColumn, DbTable};
    use crate::models::DbSchema;
    use super::*;

    macro_rules! test_schema_docs {
        ($schema_name:ident) => {
            #[tokio::test]
            async fn $schema_name() {
                check_schema_docs(stringify!($schema_name)).await
            }
        };
    }

    test_schema_docs!(taxa);
    test_schema_docs!(data);
    test_schema_docs!(info);

    // The lifetime could be relaxed from 'static but i'm not sure how
    async fn check_schema_docs(schema_name: &'static str) {
        let filename = format!("{}.toml", schema_name);
        let file = SCHEMA_DOCS_DIR.get_file(&filename)
            .expect(&format!("Missing docs file for schema {schema_name}"));

        let file_contents = file.contents();

        let docs: SchemaDocs = toml::from_slice(file_contents)
            .expect("Couldn't deserialize  schema docs file");

        let db = crate::tests::get_db().await;

        let tables = db.run(move |conn| {
            db::tables_for_schema(conn, "mmoldb", schema_name)
        }).await
            .expect("Database query failed");

        check_schema_table_docs(schema_name, tables, docs);
    }

    fn check_schema_table_docs(schema_name: &str, tables: Vec<DbTable>, docs: SchemaDocs) {
        let (docs_without_schemas, schemas_with_docs, schemas_without_docs) =
            associate(docs.tables, tables, |table, doc| table.name == doc.name);

        for doc in docs_without_schemas {
            assert!(false, "Documented table {schema_name}.{} does not exist", doc.name);
        }

        for (docs, schema) in schemas_with_docs {
            check_schema_column_docs(schema_name, schema, docs);
        }

        if !docs.allow_undocumented {
            for schema in schemas_without_docs {
                assert!(false, "Table {schema_name}.{} is not documented", schema.name);
            }
        }
    }

    fn check_schema_column_docs(schema_name: &str, table: DbTable, docs: TableDocs) {
        let (docs_without_schemas, schemas_with_docs, schemas_without_docs) =
            associate(docs.columns, table.columns, |column, doc| column.name == doc.name);

        for doc in docs_without_schemas {
            assert!(false, "Documented column {} does not exist {schema_name}.{}", doc.name, table.name);
        }

        for (docs, schema) in schemas_with_docs {
            assert_eq!(schema.r#type, docs.r#type, "Type mismatch for column {} in {schema_name}.{}", schema.name, table.name);
            assert_eq!(schema.is_nullable, docs.nullable_explanation.is_some(), "Nullability mismatch for column {} in {schema_name}.{}", schema.name, table.name);
        }

        if !docs.allow_undocumented {
            for schema in schemas_without_docs {
                assert!(false, "Column {} in {schema_name}.{} is not documented", schema.name, table.name);
            }
        }
    }
}
