use proc_macro::TokenStream;
use proc_macro_error::{abort, proc_macro_error};
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Ident};
use syn::spanned::Spanned;

#[proc_macro_error]
#[proc_macro_derive(OneAu)]
pub fn one_au(input: TokenStream) -> TokenStream {
    // Parse the input tokens into a syntax tree
    let input = parse_macro_input!(input as DeriveInput);
    let Data::Struct(input_struct) = input.data else {
        abort!(input.span(), "OneAu can only be used on struct items");
    };

    // Build the output
    let input_vis = &input.vis;
    let input_ident = &input.ident;
    let input_generics = &input.generics;
    let fields_ident = Ident::new(&format!("{}Fields", input_ident.to_string()), input_ident.span());
    let fields_spec_with_type: Vec<_> = input_struct.fields.iter()
        .map(|field| {
            let Some(field_ident) = &field.ident else {
                abort!(field.span(), "OneAu can only be used with named struct fields")
            };
           (field_ident, &field.ty)
        })
        .collect();
    let fields_spec: Vec<_> = fields_spec_with_type.iter()
        .map(|(field, _)| field)
        .collect();
    let fields_list: Vec<_> = fields_spec.iter()
        .map(|field_spec| {
            quote! { #fields_ident::#field_spec }
        })
        .collect();
    let field_au_matchers: Vec<_> = fields_spec_with_type.iter()
        .map(|(field_spec, field_ty)| {
            // This should be embedded into a match statement
            quote! {
                #fields_ident::#field_spec => {
                    Self {
                        #field_spec: <#field_ty as OneAu>::au(self.#field_spec, ()),  // TODO Handle nesting
                        ..self
                    }
                }
            }
        })
        .collect();
    let expanded = quote! {
        #[derive(Clone, Copy, Debug)]
        #[automatically_derived]
        #[allow(nonstandard_style)]
        #input_vis enum #fields_ident {
            #(#fields_spec),*
        }
        //
        #[automatically_derived]
        impl #input_generics one_au::OneAu for #input_ident #input_generics {
            type Field = #fields_ident;

            fn fields() -> impl Iterator<Item = Self::Field> {
                [
                    #(#fields_list),*
                ].into_iter()
            }

            fn au(self, field: Self::Field) -> Self {
                match field {
                    #(#field_au_matchers)*
                }
            }
        }
    };

    // Hand the output tokens back to the compiler
    TokenStream::from(expanded)
}