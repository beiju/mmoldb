pub trait AsInsertable<'a> {
    type Insertable;

    fn as_insertable(&self) -> Self::Insertable;
}

#[macro_export]
macro_rules! taxa_main_enum {
    ($table:path, $id_column:path, $(($($derive:ident),*))?, $vis:vis, $enum_name:ident, $(
        $(#[$($attr_name:ident: $attr_type:ty = $attr_value:expr),* $(,)?])?
        $variant_name:ident = $variant_id:expr
    ),+$(,)?) => {
        #[derive(Debug, enum_map::Enum, Eq, PartialEq, Hash, Copy, Clone, strum::Display, strum::IntoStaticStr, strum::EnumIter)]
        $(#[derive($($derive),*)])?
        $vis enum $enum_name {
            $($variant_name,)*
        }

        impl $enum_name {
            pub fn make_id_mapping(conn: &mut PgConnection) -> QueryResult<EnumMap<Self, i64>> {
                let mut mapping: EnumMap<Self, i64> = EnumMap::default();

                for (taxa, key) in mapping.iter_mut() {
                    let new_taxa = taxa.as_insertable();

                    *key = diesel::insert_into($table)
                        .values(&new_taxa)
                        .on_conflict($id_column)
                        .do_update()
                        .set(&new_taxa)
                        .returning($id_column)
                        .get_result(conn)?;
                }

                // Final safety check: Mapping should hold all distinct values
                // Implemented as # of unique keys == # of unique values
                assert_eq!(
                    mapping
                        .iter()
                        .map(|(key, _)| key)
                        .collect::<HashSet<_>>()
                        .len(),
                    mapping
                        .iter()
                        .map(|(_, value)| value)
                        .collect::<HashSet<_>>()
                        .len(),
                );

                Ok(mapping)
            }
        }
    };
}

#[macro_export]
macro_rules! taxa_insertable_enum {
    // This extracts just the data of the first variant and discards the rest (by ignoring tail)
    ($schema:path, $insertable_name:ident, #[$($attr_name:ident: $attr_type:ty = $attr_value:expr),* $(,)?] $($tail:tt)*) => {
        #[derive(Insertable, AsChangeset)]
        #[diesel(table_name = $schema)]
        pub struct $insertable_name<'a> {
            pub id: i64,
            pub name: &'a str,
            $(pub $attr_name: $attr_type,)*
        }
    };
    // This is intended to match when there is no attribute, hopefully it works
    ($schema:path, $insertable_name:ident, $($tail:tt)*) => {
        #[derive(Insertable, AsChangeset)]
        #[diesel(table_name = $schema)]
        pub struct $insertable_name<'a> {
            pub id: i64,
            pub name: &'a str,
        }
    };
}

#[macro_export]
macro_rules! taxa_as_insertable_impl {
    ($enum_name:ty, $insertable_name:ident, $(
        #[$($attr_name:ident: $attr_type:ty = $attr_value:expr),* $(,)?]
        $variant_name:ident = $variant_id:expr
    ),+$(,)?) => {
        impl<'a> AsInsertable<'a> for $enum_name {
            type Insertable = $insertable_name<'a>;

            fn as_insertable(&self) -> Self::Insertable {
                match self {
                    $(Self::$variant_name => {
                        Self::Insertable {
                            id: $variant_id,
                            name: self.into(),
                            $($attr_name: $attr_value,)*
                        }
                    }),*
                }
            }
        }
    };
    ($enum_name:ty, $insertable_name:ident, $(
        $variant_name:ident = $variant_id:expr
    ),+$(,)?) => {
        impl<'a> AsInsertable<'a> for $enum_name {
            type Insertable = $insertable_name<'a>;

            fn as_insertable(&self) -> Self::Insertable {
                match self {
                    $(Self::$variant_name => {
                        Self::Insertable {
                            id: $variant_id,
                            name: self.into(),
                        }
                    }),*
                }
            }
        }
    };
}

#[macro_export]
macro_rules! taxa {
    (
        #[
            schema = $schema:path,
            table = $table:path,
            id_column = $id_column:path
            $(, derive = ($($derive:ident),* $(,)?))?
            $(,)?
        ]
        $vis:vis enum $enum_name:ident {
            $($variants:tt)*
        }
    ) => {
        paste! {
            taxa_main_enum!($table, $id_column, $(($($derive),*))?, $vis, $enum_name, $($variants)*);
            taxa_insertable_enum!($schema, [<New $enum_name>], $($variants)*);
            taxa_as_insertable_impl!($enum_name, [<New $enum_name>], $($variants)*);
        }
    };
}

pub use {taxa, taxa_as_insertable_impl, taxa_insertable_enum, taxa_main_enum};
