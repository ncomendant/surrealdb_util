use std::{collections::BTreeMap, time::Duration, str::FromStr};
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use surrealdb::{Datastore, Session, sql::{Value, Object}};
use thiserror::Error;
use serde::Deserialize;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum Error {
    #[error("cast failed")]
    CastFailed,
    #[error("invalid record key '{0}'")]
    InvalidKey(&'static str),
    #[error(transparent)]
    Surrealdb(#[from] surrealdb::Error),
}

type Result<T> = std::result::Result<T, Error>;

pub struct Db (Datastore, Session);

impl Db {
    pub fn new(ds: Datastore, sess: Session) -> Self {
        Self(ds, sess)
    }

    pub fn query(&self, sql: &str) -> QueryBuilder {
        QueryBuilder::new(self, sql)
    }
}

#[derive(Clone, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Deserialize)]
pub struct Record(Object);

impl Record {
    pub fn remove(&mut self, k: &'static str) -> Result<Value> {
        let v = self.0.remove(k).map(|v| v.into()).ok_or(Error::InvalidKey(k))?;
        Ok(v)
    }
}

pub struct QueryBuilder<'a> {
    db: &'a Db,
    sql: String,
    args: ArgsBuilder,
}

impl <'a> QueryBuilder<'a> {
    pub fn new(db: &'a Db, sql: &str) -> Self {
        Self {
            db,
            sql: sql.to_string(),
            args: Default::default(),
        }
    }

    pub fn arg<S: Into<String>, V: Into<Value>>(mut self, key: S, value: V) -> Self {
        self.args.arg(key, value);
        self
    }

    pub fn sub_args<S: Into<String>>(mut self, key: S, f: impl FnOnce(&mut ArgsBuilder)) -> Self {
        self.args.sub_args(key, f);
        self
    }

    pub async fn execute(self, strict: bool) -> Result<Vec<Record>> {
        self.db.0.execute(&self.sql, &self.db.1, Some(self.args.0), strict)
            .await?
            .into_iter()
            .next()
            .map(|r| r.result.map(|v| {
                match v {
                    Value::Object(obj) => vec![Record(obj)],
                    Value::Array(arr) => arr.into_iter().filter_map(|v| match v {
                        Value::Object(obj) => Some(Record(obj)),
                        _ => None,
                    }).collect(),
                    _ => Default::default(),
                }
            }))
            .unwrap_or(Ok(Default::default()))
            .map_err(|e| e.into())
    }
}

#[derive(Debug, Clone, Default)]
pub struct ArgsBuilder(BTreeMap<String, Value>);

impl ArgsBuilder {
    pub fn arg<S: Into<String>, V: Into<Value>>(&mut self, key: S, value: V) -> &mut Self {
        self.0.insert(key.into(), value.into());
        self
    }

    pub fn sub_args<S: Into<String>>(&mut self, key: S, f: impl FnOnce(&mut Self)) -> &mut Self {
        let mut sub_args = ArgsBuilder::default();
        f(&mut sub_args);
        self.0.insert(key.into(), sub_args.into());
        self
    }
}

impl From<ArgsBuilder> for Value {
    fn from(value: ArgsBuilder) -> Self {
        value.0.into()
    }
}

pub trait FromValue where Self: Sized {
    fn from_value(value: Value) -> Result<Self>;
}

impl FromValue for String {
    fn from_value(value: Value) -> Result<Self> {
        match value {
            Value::Strand(_) => Ok(value.as_string()),
            _ => Err(Error::CastFailed),
        }
    }
}

impl FromValue for bool {
    fn from_value(value: Value) -> Result<Self> {
        match value {
            Value::True => Ok(true),
            Value::False => Ok(false),
            _ => Err(Error::CastFailed),
        }
    }
}

impl FromValue for DateTime<Utc> {
    fn from_value(value: Value) -> Result<Self> {
        match value {
            Value::Datetime(_)|Value::Strand(_) => Ok(value.as_datetime().0),
            _ => Err(Error::CastFailed),
        }
    }
}

impl FromValue for i64 {
    fn from_value(value: Value) -> Result<Self> {
        match value {
            Value::Number(_)|Value::Duration(_)|Value::Datetime(_)  => Ok(value.as_int()),
            _ => Err(Error::CastFailed),
        }
    }
}

impl FromValue for f64 {
    fn from_value(value: Value) -> Result<Self> {
        match value {
            Value::Number(_)|Value::Duration(_)|Value::Datetime(_)  => Ok(value.as_float()),
            _ => Err(Error::CastFailed),
        }
    }
}

impl FromValue for BigDecimal {
    fn from_value(value: Value) -> Result<Self> {
        match value {
            Value::Number(_) => Ok(value.as_decimal()),
            _ => Err(Error::CastFailed),
        }
    }
}

impl FromValue for Duration {
    fn from_value(value: Value) -> Result<Self> {
        match value {
            Value::Strand(_)|Value::Duration(_) => Ok(value.as_duration().0),
            _ => Err(Error::CastFailed),
        }
    }
}

impl FromValue for Uuid {
    fn from_value(value: Value) -> Result<Self> {
        let id = match value {
            Value::Strand(s) => Uuid::from_str(s.as_str()).map_err(|_e| Error::CastFailed)?,
            Value::Uuid(id) => id.0,
            Value::Thing(t) => Uuid::from_str(&t.id.to_string()).map_err(|_e| Error::CastFailed)?,
            _ => return Err(Error::CastFailed),
        };
        Ok(id)
    }
}

impl <T: FromValue> FromValue for Option<T> {
    fn from_value(value: Value) -> Result<Option<T>> {
        match value {
            Value::None|Value::Null => Ok(None),
            _ => Some(value.cast()).transpose(),
        }
    }
}

pub trait ValueCast {
    fn cast<T: FromValue>(self) -> Result<T>;
}

impl ValueCast for Value {
    fn cast<T: FromValue>(self) -> Result<T> {
        T::from_value(self)
    }
}