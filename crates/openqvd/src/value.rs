/// A single value in the Qlik "dual" sense: a machine number and the
/// textual form that Qlik used to render it.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct Dual<T> {
    /// The numeric component.
    pub number: T,
    /// The textual form of the value as stored in the file.
    pub text: String,
}

impl<T> Dual<T> {
    /// Build a dual from its numeric and textual parts.
    pub fn new(number: T, text: impl Into<String>) -> Self {
        Self {
            number,
            text: text.into(),
        }
    }
}

/// A decoded symbol value.
///
/// Variants map one-to-one to the five type bytes defined in `SPEC.md`
/// section 2.1. The enum is `#[non_exhaustive]`: keep a wildcard arm.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Value {
    /// Type byte `0x01`: 32-bit signed integer.
    Int(i32),
    /// Type byte `0x02`: IEEE 754 double-precision float.
    Float(f64),
    /// Type byte `0x04`: UTF-8 string.
    Str(String),
    /// Type byte `0x05`: dual integer plus textual form.
    DualInt(Dual<i32>),
    /// Type byte `0x06`: dual double plus textual form.
    DualFloat(Dual<f64>),
}

/// An optional value: `None` represents a NULL (see spec section 4).
pub type Cell = Option<Value>;
