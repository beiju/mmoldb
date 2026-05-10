pub use one_au_derive::*;

const STRING_ONE: &'static str = "string1";
const STRING_TWO: &'static str = "string2";

pub trait OneAu {
    type Field;

    fn fields() -> impl Iterator<Item = Self::Field>;
    fn au(self, field: Self::Field) -> Self;
}

impl<T: OneAu + Default> OneAu for Option<T> {
    type Field = T::Field;

    fn fields() -> impl Iterator<Item=Self::Field> {
        <T as OneAu>::fields()
    }

    fn au(self, field: Self::Field) -> Self {
        Some(self.unwrap_or_default().au(field))
    }
}

macro_rules! one_au_arithmetic {
    ($type_name:ident, $value:expr) => {
        impl OneAu for $type_name {
            type Field = ();
            fn fields() -> impl Iterator<Item = Self::Field> { std::iter::once(()) }
            fn au(self, field: Self::Field) -> Self {
                let () = field;
                self + $value
            }
        }
    };
}

one_au_arithmetic!(i8, 1);
one_au_arithmetic!(i16, 1);
one_au_arithmetic!(i32, 1);
one_au_arithmetic!(i64, 1);
one_au_arithmetic!(i128, 1);
one_au_arithmetic!(u8, 1);
one_au_arithmetic!(u16, 1);
one_au_arithmetic!(u32, 1);
one_au_arithmetic!(u64, 1);
one_au_arithmetic!(u128, 1);
one_au_arithmetic!(f32, 1.0);
one_au_arithmetic!(f64, 1.0);

impl<'a> OneAu for &'a str {
    type Field = ();
    fn fields() -> impl Iterator<Item = Self::Field> { std::iter::once(()) }
    fn au(self, field: Self::Field) -> Self {
        let () = field;
        if self == STRING_ONE {
            STRING_TWO
        } else {
            STRING_ONE
        }
    }
}

#[cfg(feature = "chrono")]
impl OneAu for chrono::NaiveDateTime {
    type Field = ();
    fn fields() -> impl Iterator<Item = Self::Field> { std::iter::once(()) }
    fn au(self, field: Self::Field) -> Self {
        let () = field;
        self + chrono::Duration::seconds(1)
    }
}