//! How a document's values are read: a value of the wrong kind, a scalar or a container, is
//! refused by its kind and the kind expected, never quoted, since a value may be a secret.

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::fmt;
use std::marker::PhantomData;
use std::path::PathBuf;

use serde::de::{self, Expected, MapAccess, SeqAccess, Unexpected, Visitor};
use serde::{Deserialize, Deserializer};

/// Visitor methods refusing a scalar of each kind named (`bool`, `int`, `fraction`, `text`) by
/// its kind alone.
macro_rules! refuse {
    ($($kind:ident)*) => { $(refuse!(@ $kind);)* };
    (@ bool) => {
        fn visit_bool<E: serde::de::Error>(self, _: bool) -> Result<Self::Value, E> {
            Err(E::invalid_type(serde::de::Unexpected::Other("a boolean"), &self))
        }
    };
    (@ int) => {
        fn visit_i64<E: serde::de::Error>(self, _: i64) -> Result<Self::Value, E> {
            Err(E::invalid_type(serde::de::Unexpected::Other("an integer"), &self))
        }

        fn visit_u64<E: serde::de::Error>(self, _: u64) -> Result<Self::Value, E> {
            Err(E::invalid_type(serde::de::Unexpected::Other("an integer"), &self))
        }

        fn visit_i128<E: serde::de::Error>(self, _: i128) -> Result<Self::Value, E> {
            Err(E::invalid_type(serde::de::Unexpected::Other("an integer"), &self))
        }

        fn visit_u128<E: serde::de::Error>(self, _: u128) -> Result<Self::Value, E> {
            Err(E::invalid_type(serde::de::Unexpected::Other("an integer"), &self))
        }
    };
    (@ wide) => {
        fn visit_i128<E: serde::de::Error>(self, _: i128) -> Result<Self::Value, E> {
            Err(E::invalid_type(serde::de::Unexpected::Other("an integer beyond 64 bits"), &self))
        }

        fn visit_u128<E: serde::de::Error>(self, _: u128) -> Result<Self::Value, E> {
            Err(E::invalid_type(serde::de::Unexpected::Other("an integer beyond 64 bits"), &self))
        }
    };
    (@ fraction) => {
        fn visit_f64<E: serde::de::Error>(self, _: f64) -> Result<Self::Value, E> {
            Err(E::invalid_type(serde::de::Unexpected::Other("a fraction"), &self))
        }
    };
    (@ text) => {
        fn visit_str<E: serde::de::Error>(self, _: &str) -> Result<Self::Value, E> {
            Err(E::invalid_type(serde::de::Unexpected::Other("text"), &self))
        }
    };
}

pub(super) use refuse;

/// Text a document holds.
pub(super) struct Text(pub(super) String);

impl<'de> Deserialize<'de> for Text {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Reading;

        impl Visitor<'_> for Reading {
            type Value = Text;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("text")
            }

            fn visit_str<E: de::Error>(self, text: &str) -> Result<Text, E> {
                Ok(Text(text.to_owned()))
            }

            refuse!(bool int fraction);
        }

        deserializer.deserialize_any(Reading)
    }
}

/// A whole number a document holds, within `T`.
struct Count<T>(T);

impl<'de, T: TryFrom<u64>> Deserialize<'de> for Count<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Reading<T>(PhantomData<T>);

        impl<T: TryFrom<u64>> Visitor<'_> for Reading<T> {
            type Value = Count<T>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a whole number")
            }

            fn visit_u64<E: de::Error>(self, count: u64) -> Result<Count<T>, E> {
                let large = || E::invalid_value(Unexpected::Other("a number that large"), &self);
                T::try_from(count).map(Count).map_err(|_| large())
            }

            fn visit_i64<E: de::Error>(self, count: i64) -> Result<Count<T>, E> {
                let negative = || E::invalid_value(Unexpected::Other("a negative number"), &self);
                let count = u64::try_from(count).map_err(|_| negative())?;
                self.visit_u64(count)
            }

            refuse!(bool wide fraction text);
        }

        deserializer.deserialize_any(Reading(PhantomData))
    }
}

/// A `T` the document holds as a mapping, read as `T` reads one.
pub(super) struct Mapped<T>(pub(super) T);

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Mapped<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Reading<T>(PhantomData<T>);

        impl<'de, T: Deserialize<'de>> Visitor<'de> for Reading<T> {
            type Value = Mapped<T>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a mapping")
            }

            fn visit_map<M: MapAccess<'de>>(self, map: M) -> Result<Mapped<T>, M::Error> {
                T::deserialize(de::value::MapAccessDeserializer::new(map)).map(Mapped)
            }

            refuse!(bool int fraction text);
        }

        deserializer.deserialize_any(Reading(PhantomData))
    }
}

/// Items the document holds as a list, each read as `T` reads it.
struct Listed<T>(Vec<T>);

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Listed<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Reading<T>(PhantomData<T>);

        impl<'de, T: Deserialize<'de>> Visitor<'de> for Reading<T> {
            type Value = Listed<T>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a list")
            }

            fn visit_seq<S: SeqAccess<'de>>(self, mut seq: S) -> Result<Listed<T>, S::Error> {
                let mut items = Vec::new();
                while let Some(item) = seq.next_element()? {
                    items.push(item);
                }
                Ok(Listed(items))
            }

            refuse!(bool int fraction text);
        }

        deserializer.deserialize_any(Reading(PhantomData))
    }
}

/// Entries the document holds as a mapping of names to `T`s, each read as `T` reads it.
struct Named<T>(BTreeMap<String, T>);

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Named<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Reading<T>(PhantomData<T>);

        impl<'de, T: Deserialize<'de>> Visitor<'de> for Reading<T> {
            type Value = Named<T>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a mapping of names")
            }

            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Named<T>, M::Error> {
                let mut named = BTreeMap::new();
                while let Some((Text(name), value)) = map.next_entry()? {
                    named.insert(name, value);
                }
                Ok(Named(named))
            }

            refuse!(bool int fraction text);
        }

        deserializer.deserialize_any(Reading(PhantomData))
    }
}

/// A document's mapping, read as `T` reads one, for `#[serde(deserialize_with)]`.
pub(super) fn mapping<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Mapped::deserialize(deserializer).map(|Mapped(mapped)| mapped)
}

/// A document's optional mapping, read as `T` reads one, for `#[serde(deserialize_with)]`.
pub(super) fn optional_mapping<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<Mapped<T>>::deserialize(deserializer).map(|mapped| mapped.map(|Mapped(mapped)| mapped))
}

/// A document's list of mappings, each read as `T` reads one, for `#[serde(deserialize_with)]`.
pub(super) fn mappings<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    let Listed(mapped) = Listed::<Mapped<T>>::deserialize(deserializer)?;
    Ok(mapped.into_iter().map(|Mapped(mapped)| mapped).collect())
}

/// A document's list, each item read as `T` reads it, for `#[serde(deserialize_with)]`.
pub(super) fn list<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Listed::deserialize(deserializer).map(|Listed(items)| items)
}

/// A document's mapping of names to mappings, each read as `T` reads one, for
/// `#[serde(deserialize_with)]`.
pub(super) fn named_mappings<'de, D, T>(deserializer: D) -> Result<BTreeMap<String, T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    let Named(named) = Named::<Mapped<T>>::deserialize(deserializer)?;
    Ok(named
        .into_iter()
        .map(|(name, Mapped(mapped))| (name, mapped))
        .collect())
}

/// A document's yes or no, for `#[serde(deserialize_with)]`.
pub(super) fn flag<'de, D: Deserializer<'de>>(deserializer: D) -> Result<bool, D::Error> {
    struct Reading;

    impl Visitor<'_> for Reading {
        type Value = bool;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("true or false")
        }

        fn visit_bool<E: de::Error>(self, flag: bool) -> Result<bool, E> {
            Ok(flag)
        }

        refuse!(int fraction text);
    }

    deserializer.deserialize_any(Reading)
}

/// A document's text, for `#[serde(deserialize_with)]`.
pub(super) fn text<'de, D: Deserializer<'de>>(deserializer: D) -> Result<String, D::Error> {
    Text::deserialize(deserializer).map(|Text(text)| text)
}

/// A document's optional text, for `#[serde(deserialize_with)]`.
pub(super) fn optional_text<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<String>, D::Error> {
    Option::<Text>::deserialize(deserializer).map(|text| text.map(|Text(text)| text))
}

/// A document's list of texts, for `#[serde(deserialize_with)]`.
pub(super) fn texts<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<String>, D::Error> {
    let Listed(texts) = Listed::<Text>::deserialize(deserializer)?;
    Ok(texts.into_iter().map(|Text(text)| text).collect())
}

/// A document's list of paths, for `#[serde(deserialize_with)]`.
pub(super) fn paths<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<PathBuf>, D::Error> {
    let Listed(texts) = Listed::<Text>::deserialize(deserializer)?;
    Ok(texts
        .into_iter()
        .map(|Text(text)| PathBuf::from(text))
        .collect())
}

/// A document's whole number, for `#[serde(deserialize_with)]`.
pub(super) fn count<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: TryFrom<u64>,
{
    Count::deserialize(deserializer).map(|Count(count)| count)
}

/// A document's optional whole number, for `#[serde(deserialize_with)]`.
pub(super) fn optional_count<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: TryFrom<u64>,
{
    Option::<Count<T>>::deserialize(deserializer).map(|count| count.map(|Count(count)| count))
}

/// The choice of `choices` whose word the document holds.
pub(super) fn word<'de, D: Deserializer<'de>, T: Copy>(
    deserializer: D,
    choices: &[(&'static str, T)],
) -> Result<T, D::Error> {
    let Text(word) = Text::deserialize(deserializer)?;
    chosen(&word, choices)
}

/// The choice of `choices` `word` names, or a refusal naming the choices and never `word`.
pub(super) fn chosen<T: Copy, E: de::Error>(
    word: &str,
    choices: &[(&'static str, T)],
) -> Result<T, E> {
    let found = choices.iter().find(|(name, _)| *name == word);
    found
        .map(|(_, choice)| *choice)
        .ok_or_else(|| E::invalid_value(Unexpected::Other("a word"), &Choices(choices)))
}

/// What a refused word could have been.
struct Choices<'a, T>(&'a [(&'static str, T)]);

impl<T> Expected for Choices<'_, T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("one of ")?;
        for (index, (name, _)) in self.0.iter().enumerate() {
            if index > 0 {
                formatter.write_str(", ")?;
            }
            formatter.write_str(name)?;
        }
        Ok(())
    }
}
