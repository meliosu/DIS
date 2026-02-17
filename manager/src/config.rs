use std::{collections::HashSet, env::VarError, str::FromStr, sync::LazyLock, time::Duration};

use anyhow::{anyhow, bail};

use crate::constants::{DEFAULT_ALPHABET, DEFAULT_TIMEOUT};

pub struct Config {
    pub timeout: std::time::Duration,
    pub alphabet: String,
}

pub static CONFIG: LazyLock<Config> = LazyLock::new(|| {
    let timeout = match var("TIMEOUT") {
        Ok(Some(timeout)) => timeout,
        Ok(None) => DEFAULT_TIMEOUT,
        Err(e) => {
            log::error!("{e}, using default timeout, which is {DEFAULT_TIMEOUT}");
            DEFAULT_TIMEOUT
        },
    };

    let alphabet = match var::<String>("ALPHABET") {
        Ok(Some(alphabet)) => alphabet,
        Ok(None) => DEFAULT_ALPHABET.to_string(),
        Err(e) => {
            log::error!("{e}, using default alphabet, which is {DEFAULT_ALPHABET}");
            DEFAULT_ALPHABET.to_string()
        },
    };

    let alphabet = match check_valid_alphabet(&alphabet) {
        Ok(()) => alphabet,
        Err(e) => {
            log::error!("{e}, using default alphabet, which is {DEFAULT_ALPHABET}");
            DEFAULT_ALPHABET.to_string()
        }
    };

    Config {
        timeout: Duration::from_secs(timeout),
        alphabet
    }
});

fn check_valid_alphabet(alphabet: &str) -> anyhow::Result<()> {
    let mut chars: HashSet<char> = HashSet::new();

    for c in alphabet.chars() {
        if chars.contains(&c) {
            bail!("alphabet contains duplicate symbol: {c}");
        }

        chars.insert(c);
    }

    Ok(())
}

fn var<T: FromStr>(name: &str) -> anyhow::Result<Option<T>> 
    where T::Err: Sync + Send + std::fmt::Display,
{
    let var = match std::env::var(name) {
        Ok(var) => var,
        Err(e) => match e {
            VarError::NotPresent => {
                return Ok(None);
            },

            VarError::NotUnicode(_) => {
                bail!("env var {name} contains non-unicode symbols");
            },
        },
    };

    var.parse().map_err(|e| anyhow!("parsing env var {name}: {e}")).map(|value| Some(value))
}
