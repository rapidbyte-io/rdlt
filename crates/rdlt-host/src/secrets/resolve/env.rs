//! Secrets in the host's environment: the variables its operator lists, and no other.

use std::ffi::{OsStr, OsString};
use std::fmt;

use rdlt_connector::{BoxFuture, Secret};

use super::{SecretFault, SecretKind, SecretReference, SecretResolver, admit};

/// Which variables a resolver reaches.
#[derive(Clone, Debug)]
enum Scope {
    /// `${env:NAME}` for each name listed.
    Names(Vec<String>),
    /// `${env:NAME}` for each name that starts with the prefix.
    Prefix(String),
    /// `${secret:name}` from the variable of the prefix and the name in upper case.
    Named(String),
}

/// Resolves references to the host's environment variables, only those its operator lists.
#[derive(Clone)]
pub struct EnvSecrets {
    scope: Scope,
    variable: fn(&OsStr) -> Option<OsString>,
}

impl fmt::Debug for EnvSecrets {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EnvSecrets")
            .field("scope", &self.scope)
            .finish_non_exhaustive()
    }
}

impl EnvSecrets {
    fn scoped(scope: Scope) -> Self {
        Self {
            scope,
            variable: |name| std::env::var_os(name),
        }
    }

    /// Resolves `${env:NAME}` for each of `names`, and refuses every other variable.
    pub fn allowing(names: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self::scoped(Scope::Names(names.into_iter().map(Into::into).collect()))
    }

    /// Resolves `${env:NAME}` for each name that starts with `prefix`, which may not be
    /// empty, and refuses every other variable.
    pub fn prefixed(prefix: impl Into<String>) -> Self {
        Self::scoped(Scope::Prefix(prefix.into()))
    }

    /// Resolves `${secret:name}` from the variable `<prefix><NAME>`, the name in upper case
    /// with `_` for every character that is neither a letter nor a digit, as
    /// `EnvSecrets::named("RDLT_SECRET_")` does; to be given as a store of named secrets
    /// ([`Secrets::named`](super::Secrets::named)).
    ///
    /// The prefix may not be empty: under an empty one every reference is refused.
    pub fn named(prefix: impl Into<String>) -> Self {
        Self::scoped(Scope::Named(prefix.into()))
    }

    /// Reads variables through `variable`, in place of the host's environment.
    #[cfg(test)]
    pub(crate) fn reading(mut self, variable: fn(&OsStr) -> Option<OsString>) -> Self {
        self.variable = variable;
        self
    }

    /// The variable `reference` reaches, when this resolver lets it reach one.
    fn variable_of(&self, reference: &SecretReference) -> Option<String> {
        let name = &reference.name;
        match (&self.scope, reference.kind) {
            (Scope::Names(names), SecretKind::Env) => names.contains(name).then(|| name.clone()),
            (Scope::Prefix(prefix), SecretKind::Env) => {
                (!prefix.is_empty() && name.starts_with(prefix.as_str())).then(|| name.clone())
            }
            (Scope::Named(prefix), SecretKind::Named) => {
                (!prefix.is_empty()).then(|| named_variable(prefix, name))
            }
            _ => None,
        }
    }

    fn read(&self, variable: &str) -> Result<Secret<String>, SecretFault> {
        let value = (self.variable)(OsStr::new(variable)).ok_or(SecretFault::Missing)?;
        let value = value.into_string().map_err(|_| SecretFault::NotText)?;
        let secret = Secret::new(value);
        admit(secret.expose().len())?;
        Ok(secret)
    }
}

/// The variable `${secret:name}` is read from, under `prefix`.
fn named_variable(prefix: &str, name: &str) -> String {
    let letter = |c: char| match c {
        c if c.is_ascii_alphanumeric() => c.to_ascii_uppercase(),
        _ => '_',
    };
    format!("{prefix}{}", name.chars().map(letter).collect::<String>())
}

impl SecretResolver for EnvSecrets {
    fn resolve<'a>(
        &'a self,
        reference: &'a SecretReference,
    ) -> BoxFuture<'a, Result<Secret<String>, SecretFault>> {
        Box::pin(async move {
            let variable = self.variable_of(reference).ok_or(SecretFault::Refused)?;
            self.read(&variable)
        })
    }
}
