use std::collections::HashMap;
use std::path::PathBuf;
use diesel::IntoSql;
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
pub struct SchemaDocs {
    description: String,
}

#[cfg(test)]
mod tests {
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
    }
}