use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use serde::Deserialize;
use std::path::PathBuf;

pub fn postgres_url_from_environment() -> String {
    #[derive(Debug, PartialEq, Deserialize)]
    struct PostgresConfig {
        user: String,
        password: Option<String>,
        password_file: Option<PathBuf>,
        db: String,
    }
    let provider = figment::providers::Env::prefixed("POSTGRES_");
    let postgres_config: PostgresConfig = figment::Figment::from(provider)
        .extract()
        .expect("Postgres configuration environment variable(s) missing or invalid");

    let password = if let Some(password) = postgres_config.password {
        password
    } else if let Some(password_file) = postgres_config.password_file {
        std::fs::read_to_string(password_file).expect("Failed to read postgres password file")
    } else {
        panic!("One of POSTGRES_PASSWORD or POSTGRES_PASSWORD_FILE must be provided");
    };

    // Postgres (or something else in my Postgres pipeline) will _truncate_ the
    // password at the first newline. I don't want to mimic that behavior,
    // because it could lead to people using vastly less secure passwords than
    // they intended to. I can trim a trailing newline without losing (a
    // meaningful amount of) entropy, which I will do because the trailing
    // newline convention is so strong the user might not even realize they
    // have one. But if there are any other newlines in the string, I just exit
    // with an error.
    let password = if let Some(pw) = password.strip_suffix("\n") {
        pw
    } else {
        &password
    };

    if password.contains("\n") {
        // Print this error in the most direct way to maximize the chances that the
        // user can figure out what's going on
        eprintln!(
            "Postgres admin password contains a non-terminal newline. This password will be \
            insecurely truncated. Please try again with a password that does not contain non-\
            terminal newlines."
        );
        // Also panic with the same message
        panic!(
            "Postgres admin password contains a non-terminal newline. This password will be \
            insecurely truncated. Please try again with a password that does not contain non-\
            terminal newlines."
        );
    }

    // Must percent encode password.
    // The return type of utf8_percent_encode implements Display so we can skip the to_string call
    // and provide it directly to the format!().
    let password = utf8_percent_encode(&password, NON_ALPHANUMERIC);

    format!(
        "postgres://{}:{}@db/{}",
        postgres_config.user, password, postgres_config.db
    )
}
