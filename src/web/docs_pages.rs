use std::collections::HashMap;
use std::path::PathBuf;
use include_dir::{include_dir, Dir};
use itertools::Itertools;
use miette::Diagnostic;
use rocket::{get, uri};
use rocket_dyn_templates::{context, Template};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use crate::{db, Db};
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
    struct Schema {
        description: String,
    }

    let schemata = SCHEMA_DOCS_DIR.entries()
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
        .map_ok(|(key, val)| {
            let new_val = Schema {
                description: markdown::to_html(&val.description),
            };

            (key, new_val)
        })
        .collect::<Result<HashMap<_, _>, _>>()?;

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
    #[serde(rename = "column")]
    pub columns: Vec<ColumnDocs>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SchemaDocs {
    pub description: String,
    // It's more ergonomic to use the singular in the TOML file
    #[serde(rename = "table")]
    pub tables: Vec<TableDocs>,
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

        check_schema_table_docs(schema_name, tables, docs.tables);
    }

    fn check_schema_table_docs(schema_name: &str, tables: Vec<DbTable>, docs: Vec<TableDocs>) {
        let (docs_without_schemas, schemas_with_docs, schemas_without_docs) =
            associate(docs, tables, |table, doc| table.name == doc.name);

        for schema in schemas_without_docs {
            assert!(false, "Table {schema_name}.{} is not documented", schema.name);
        }

        for doc in docs_without_schemas {
            assert!(false, "Documented table {schema_name}.{} does not exist", doc.name);
        }

        for (docs, schema) in schemas_with_docs {
            check_schema_column_docs(schema_name, &schema.name, schema.columns, docs.columns);
        }
    }

    fn check_schema_column_docs(schema_name: &str, table_name: &str, columns: Vec<DbColumn>, docs: Vec<ColumnDocs>) {
        let (docs_without_schemas, schemas_with_docs, schemas_without_docs) =
            associate(docs, columns, |column, doc| column.name == doc.name);

        for schema in schemas_without_docs {
            assert!(false, "Column {} in {schema_name}.{table_name} is not documented", schema.name);
        }

        for doc in docs_without_schemas {
            assert!(false, "Documented column {} does not exist {schema_name}.{table_name}", doc.name);
        }

        for (docs, schema) in schemas_with_docs {
            assert_eq!(schema.r#type, docs.r#type, "Type mismatch for column {} in {schema_name}.{table_name}", schema.name);
            assert_eq!(schema.is_nullable, docs.nullable_explanation.is_some(), "Nullability mismatch for column {} in {schema_name}.{table_name}", schema.name);
        }
    }
}
