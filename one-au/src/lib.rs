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

impl<T: OneAu + Default + Clone> OneAu for Vec<T> {
    type Field = T::Field;

    fn fields() -> impl Iterator<Item=Self::Field> {
        <T as OneAu>::fields()
    }

    fn au(mut self, field: Self::Field) -> Self {
        // This must (a) not ever remove a value and (b) maintain sort, or it will
        // interfere with version closeouts in the test. This is unintentional
        // coupling that should be fixed. It can be assumed that the only entry
        // in a string vec will be the empty string, so it will always be sorted
        // first.
        if let Some(last_entry) = self.last() {
            self.push(last_entry.clone().au(field));
        } else {
            self.push(T::default());
        }
        self
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

// This must not ever return a smaller value or it will interfere with version
// closeouts in the test. This is unintentional coupling that should be fixed.
// It can be assumed that adding 1 will not cause overflow.
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

impl OneAu for bool {
    type Field = ();
    fn fields() -> impl Iterator<Item = Self::Field> { std::iter::once(()) }
    fn au(self, field: Self::Field) -> Self {
        let () = field;
        !self
    }
}

impl OneAu for String {
    type Field = ();
    fn fields() -> impl Iterator<Item = Self::Field> { std::iter::once(()) }
    fn au(self, field: Self::Field) -> Self {
        let () = field;
        format!("{self}1")
    }
}

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