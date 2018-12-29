
use serde_dyn::{TypeUuid};
use std::ops::{DerefMut, Deref};
use downcast::{Any, Downcast};
use std::fmt::Debug;


#[derive(Debug, PartialEq, Clone)]
pub enum TypeId {
    Optional(Box<TypeId>),
    Vec(Box<TypeId>),
    Type(u128),
}

pub trait ProcessorType {
    fn get_processor_type() -> TypeId;
}
impl<T: TypeUuid> ProcessorType for Arg<T> {
    fn get_processor_type() -> TypeId {
        TypeId::Type(T::UUID)
    }
}

impl<T: ProcessorType> ProcessorType for Vec<T> {
    fn get_processor_type() -> TypeId {
        TypeId::Vec(Box::new(T::get_processor_type()))
    }
}

pub trait ProcessorAccess {
    fn get_read<T: 'static>(&mut self, index: u32) -> Arg<T>;
    fn get_vec<T: 'static>(&mut self, index: u32) -> Vec<T>;
    fn put_write<T: 'static>(&mut self, index: u32, value: T);
}

pub trait InputData {
    fn get_read<T: ProcessorAccess>(access: &mut T, index: u32) -> Self;
    fn reads() -> Vec<TypeId>;
}

pub trait OutputData {
    fn put_write<T: ProcessorAccess>(access: &mut T, index: u32, value: Self);
    fn writes() -> Vec<TypeId>;
}

pub trait Processor {
    type Inputs: InputData;
    type Outputs: OutputData;
    fn name() -> &'static str;
    fn input_names() -> &'static [&'static str];
    fn output_names() -> &'static [&'static str];
    fn run(inputs: &Self::Inputs) -> Self::Outputs;
}
struct AnyProcessorImpl<T> {
    _marker: std::marker::PhantomData<T>,
}
unsafe impl<T: Processor> Send for AnyProcessorImpl<T> {}
unsafe impl<T: Processor> Sync for AnyProcessorImpl<T> {}

pub trait AnyProcessor: Send + Sync {
    fn name(&self) -> &'static str;
    fn input_names(&self) -> &'static [&'static str];
    fn output_names(&self) -> &'static [&'static str];
    fn inputs(&self) -> Vec<TypeId>;
    fn outputs(&self) -> Vec<TypeId>;
    fn run(&self, access: &mut ProcessorValues);
}
impl<T> AnyProcessor for AnyProcessorImpl<T>
where T: Processor 
{
    fn name(&self) -> &'static str {
        T::name()
    }
    fn input_names(&self) -> &'static [&'static str] {
        T::input_names()
    }
    fn output_names(&self) -> &'static [&'static str] {
        T::output_names()
    }
    fn inputs(&self) -> Vec<TypeId> {
        T::Inputs::reads()
    }
    fn outputs(&self) -> Vec<TypeId> {
        T::Outputs::writes()
    }
    fn run(&self, access: &mut ProcessorValues) {
        <T as RunNow>::run_now(access)
    }
}
pub fn into_any<T: Processor>() -> impl AnyProcessor {
    AnyProcessorImpl::<T> { _marker: std::marker::PhantomData }
}

pub trait RunNow {
    fn run_now<T: ProcessorAccess>(access: &mut T);
}
impl<T> RunNow for T
where T: Processor
{
    fn run_now<PA: ProcessorAccess>(access: &mut PA) {
        let input = T::Inputs::get_read(access, 0);
        T::Outputs::put_write(access, 0, T::run(&input));
    }
}

impl InputData for () {
    fn get_read<T: ProcessorAccess>(_: &mut T, _: u32) -> Self {
        ()
    }

    fn reads() -> Vec<TypeId> {
        Vec::new()
    }
}

impl<T: TypeUuid + 'static> InputData for Arg<T>
{
    fn get_read<P: ProcessorAccess>(access: &mut P, idx: u32) -> Self {
        <P as ProcessorAccess>::get_read(access, idx)
    }

    fn reads() -> Vec<TypeId> {
        vec![<Arg<T> as ProcessorType>::get_processor_type()]
    }
}

impl<T: ProcessorType + 'static> InputData for Vec<T>
{
    fn get_read<P: ProcessorAccess>(access: &mut P, idx: u32) -> Vec<T> {
        <P as ProcessorAccess>::get_vec(access, idx)
    }

    fn reads() -> Vec<TypeId> {
        vec![<Vec<T> as ProcessorType>::get_processor_type()]
    }
}

impl<T: TypeUuid + 'static> OutputData for Arg<T> {
    fn put_write<P: ProcessorAccess>(access: &mut P, index: u32, value: Self) {
        access.put_write(index, value);
    }

    fn writes() -> Vec<TypeId> {
        vec![<Arg<T> as ProcessorType>::get_processor_type()]
    }
}

impl<T: ProcessorType + 'static> OutputData for Vec<T> {
    fn put_write<P: ProcessorAccess>(access: &mut P, index: u32, value: Self) {
        access.put_write(index, value);
    }

    fn writes() -> Vec<TypeId> {
        vec![<Vec<T> as ProcessorType>::get_processor_type()]
    }
}

impl OutputData for () {
    fn put_write<T: ProcessorAccess>(_: &mut T, _: u32, _: Self) {
        
    }

    fn writes() -> Vec<TypeId> {
        Vec::new()
    }
}


pub struct Arg<T> {
    inner: T,
}

impl<T> From<T> for Arg<T> {
    fn from(v: T) -> Arg<T> {
        Arg { inner: v }
    }
}

impl<T> Deref for Arg<T>
{
    type Target = T;

    fn deref(&self) -> &T {
        &self.inner
    }
}

pub struct ArgMut<'a, T: 'a> {
    inner: &'a mut T,
}

impl<'a, T> Deref for ArgMut<'a, T>
{
    type Target = T;

    fn deref(&self) -> &T {
        self.inner
    }
}

impl<'a, T> DerefMut for ArgMut<'a, T>
{
    fn deref_mut(&mut self) -> &mut T {
        self.inner
    }
}

macro_rules! impl_inputs {
    ( $($ty:ident, $idx:expr),* ) => {
        impl<$($ty),*> InputData for ( $( $ty , )* )
            where $( $ty : InputData),*
            {
                fn get_read<PA: ProcessorAccess>(access: &mut PA, index: u32) -> Self {
                    #![allow(unused_variables)]

                    ( $( <$ty as InputData>::get_read(access, $idx), ) *) 
                }

                fn reads() -> Vec<TypeId> {
                    #![allow(unused_mut)]

                    let mut r = Vec::new();

                    $( {
                        let mut reads = <$ty as InputData>::reads();
                        r.append(&mut reads);
                    } )*

                    r
                }
            }
    };
}

macro_rules! impl_outputs {
    ( $($ty:ident, $idx:tt),* ) => {
        impl<$($ty),*> OutputData for ( $( $ty , )* )
            where $( $ty : OutputData),*
            {
                fn put_write<PA: ProcessorAccess>(access: &mut PA, index: u32, value: Self) {
                    #![allow(unused_variables)]

                    $( {
                        <$ty as OutputData>::put_write(access, $idx, value.$idx);
                    } ) *
                }

                fn writes() -> Vec<TypeId> {
                    #![allow(unused_mut)]

                    let mut w = Vec::new();

                    $( {
                        let mut writes = <$ty as OutputData>::writes();
                        w.append(&mut writes);
                    } )*

                    w
                }
            }
    };
}

mod impl_inputs {
    #![cfg_attr(rustfmt, rustfmt_skip)]

    use super::*;

    impl_inputs!(A, 0);
    impl_inputs!(A, 0, B, 1);
    impl_inputs!(A, 0, B, 1, C, 2);
    impl_inputs!(A, 0, B, 1, C, 2, D, 3);
    impl_inputs!(A, 0, B, 1, C, 2, D, 3, E, 4);
    impl_inputs!(A, 0, B, 1, C, 2, D, 3, E, 4, F, 5);
    impl_inputs!(A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6);
    impl_inputs!(A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7);
    impl_inputs!(A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8);
    impl_inputs!(A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9);
    impl_inputs!(A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10);
    impl_inputs!(A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10, L, 11);
    impl_inputs!(A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10, L, 11, M, 12);
    impl_inputs!(A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10, L, 11, M, 12, N, 13);
    impl_inputs!(A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10, L, 11, M, 12, N, 13, O, 14);
    impl_inputs!(A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10, L, 11, M, 12, N, 13, O, 14, P, 15);
    impl_inputs!(A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10, L, 11, M, 12, N, 13, O, 14, P, 15, Q, 16);
    impl_inputs!(A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10, L, 11, M, 12, N, 13, O, 14, P, 15, Q, 16, R, 17);

    impl_outputs!(A, 0);
    impl_outputs!(A, 0, B, 1);
    impl_outputs!(A, 0, B, 1, C, 2);
    impl_outputs!(A, 0, B, 1, C, 2, D, 3);
    impl_outputs!(A, 0, B, 1, C, 2, D, 3, E, 4);
    impl_outputs!(A, 0, B, 1, C, 2, D, 3, E, 4, F, 5);
    impl_outputs!(A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6);
    impl_outputs!(A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7);
    impl_outputs!(A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8);
    impl_outputs!(A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9);
    impl_outputs!(A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10);
    impl_outputs!(A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10, L, 11);
    impl_outputs!(A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10, L, 11, M, 12);
    impl_outputs!(A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10, L, 11, M, 12, N, 13);
    impl_outputs!(A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10, L, 11, M, 12, N, 13, O, 14);
    impl_outputs!(A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10, L, 11, M, 12, N, 13, O, 14, P, 15);
    impl_outputs!(A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10, L, 11, M, 12, N, 13, O, 14, P, 15, Q, 16);
    impl_outputs!(A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10, L, 11, M, 12, N, 13, O, 14, P, 15, Q, 16, R, 17);
}
pub struct ProcessorValues {
    inputs: Vec<Option<Box<Any>>>,
    outputs: Vec<Option<Box<Any>>>,
}
impl ProcessorValues {
    pub fn new(inputs: Vec<Option<Box<Any>>>) -> ProcessorValues {
        ProcessorValues { inputs: inputs, outputs: Vec::new() }
    }
    pub fn outputs(&self) -> &Vec<Option<Box<Any>>> {
        &self.outputs
    }
}

impl ProcessorAccess for ProcessorValues {
    fn get_read<T: 'static>(&mut self, index: u32) -> Arg<T> {
        let val = self.inputs.remove(index as usize);
        self.inputs.insert(index as usize, None);
        Arg { inner: *val.unwrap().downcast().unwrap() }
    }
    fn get_vec<T: 'static>(&mut self, index: u32) -> Vec<T> {
        let val = self.inputs.remove(index as usize);
        self.inputs.insert(index as usize, None);
        *val.unwrap().downcast().unwrap()
    }
    fn put_write<T: 'static>(&mut self, index: u32, value: T) {
        self.outputs.insert(index as usize, Some(Box::new(value)));
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use serde_dyn::uuid;

    uuid!{
        ABC => 14092692613983100637224012401022025107
    }

    struct ABC;
    impl Processor for ABC {
        fn name() -> &'static str { "ABC" }
        fn input_names() -> &'static [&'static str] { &["f", "b"] }
        fn output_names() -> &'static [&'static str] { &["g", "c"] }
        type Inputs = (Vec<Arg<f32>>, Arg<u8>);
        type Outputs = (Arg<u32>, Arg<u16>);
        fn run((f, b): &Self::Inputs) -> Self::Outputs {
            let mut total = 0u32;
            for x in f.iter() {
                total += **x as u32;
            }
            total += **b as u32;
            (Arg::from(total), Arg::from(88u16))
        }
    }

    #[test]
    fn test() {
        let mut values = ProcessorValues::new(vec![ Some(Box::new(vec![Arg{ inner: 3.2f32}])), Some(Box::new(2u8)) ]);
        ABC::run_now(&mut values);
        let out = values.outputs.remove(0).unwrap();
        println!("{}", Downcast::<Arg<u32>>::downcast_ref(out.as_ref()).unwrap().inner);
    }
}