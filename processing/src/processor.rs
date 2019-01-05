
use serde_dyn::{TypeUuid};
use std::ops::{DerefMut, Deref};
use downcast::{Any, Downcast, impl_downcast};
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
impl<'a, T: TypeUuid> ProcessorType for Arg<'a, T> {
    fn get_processor_type() -> TypeId {
        TypeId::Type(T::UUID)
    }
}
impl<T: TypeUuid> ProcessorType for Val<T> {
    fn get_processor_type() -> TypeId {
        TypeId::Type(T::UUID)
    }
}

impl<T: ProcessorType> ProcessorType for Vec<T> {
    fn get_processor_type() -> TypeId {
        TypeId::Vec(Box::new(T::get_processor_type()))
    }
}

pub trait ProcessorObj : Any + Send + Sync {
    fn get_processor_type(&self) -> TypeId;
}
impl_downcast!(ProcessorObj);
impl<T: ProcessorType + Send + Sync + 'static> ProcessorObj for T {
    fn get_processor_type(&self) -> TypeId {
        T::get_processor_type()
    }
}

pub trait ProcessorAccess {
    fn get_read<T: ProcessorObj>(&mut self, index: u32) -> T;
    fn put_write<T: ProcessorObj>(&mut self, index: u32, value: T);
}

pub trait InputData<'a> {
    fn get_read<T: ProcessorAccess>(access: &mut T, index: u32) -> Self;
    fn reads() -> Vec<TypeId>;
}

pub trait OutputData {
    fn put_write<T: ProcessorAccess>(access: &mut T, index: u32, value: Self);
    fn writes() -> Vec<TypeId>;
}

pub trait Processor<'a> {
    type Inputs: InputData<'a>;
    type Outputs: OutputData;
    fn name() -> &'static str;
    fn input_names() -> Vec<String>;
    fn output_names() -> Vec<String>;
    fn run(inputs: Self::Inputs) -> Self::Outputs;
}
struct AnyProcessorImpl<T> {
    _marker: std::marker::PhantomData<T>,
}
unsafe impl<'a, T: Processor<'a>> Send for AnyProcessorImpl<T> {}
unsafe impl<'a, T: Processor<'a>> Sync for AnyProcessorImpl<T> {}

pub trait AnyProcessor: Send + Sync {
    fn name(&self) -> &'static str;
    fn input_names(&self) -> Vec<String>;
    fn output_names(&self) -> Vec<String>;
    fn inputs(&self) -> Vec<TypeId>;
    fn outputs(&self) -> Vec<TypeId>;
    fn run(&self, access: &mut ProcessorValues);
}
impl<'a, T: 'a> AnyProcessor for AnyProcessorImpl<T>
where T: Processor<'a> 
{
    fn name(&self) -> &'static str {
        T::name()
    }
    fn input_names(&self) -> Vec<String> {
        T::input_names()
    }
    fn output_names(&self) -> Vec<String> {
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
pub fn into_any<'a, T: Processor<'a> + 'a>() -> impl AnyProcessor {
    AnyProcessorImpl::<T> { _marker: std::marker::PhantomData }
}

pub trait RunNow {
    fn run_now<T: ProcessorAccess>(access: &mut T);
}
impl<'a, T> RunNow for T
where T: Processor<'a>
{
    fn run_now<PA: ProcessorAccess>(access: &mut PA) {
        let input = T::Inputs::get_read(access, 0);
        T::Outputs::put_write(access, 0, T::run(input));
    }
}

impl<'a> InputData<'a> for () {
    fn get_read<T: ProcessorAccess>(_: &mut T, _: u32) -> Self {
        ()
    }

    fn reads() -> Vec<TypeId> {
        Vec::new()
    }
}

impl<'a, T: TypeUuid + Send + Sync + 'static> InputData<'a> for Arg<'a, T>
{
    fn get_read<P: ProcessorAccess>(access: &mut P, idx: u32) -> Self {
        <P as ProcessorAccess>::get_read(access, idx)
    }

    fn reads() -> Vec<TypeId> {
        vec![<Arg<T> as ProcessorType>::get_processor_type()]
    }
}

impl<'a, T: ProcessorType + 'static + Send + Sync> InputData<'a> for Vec<T>
{
    fn get_read<P: ProcessorAccess>(access: &mut P, idx: u32) -> Self {
        <P as ProcessorAccess>::get_read(access, idx)
    }

    fn reads() -> Vec<TypeId> {
        vec![<Vec<T> as ProcessorType>::get_processor_type()]
    }
}

impl<T: TypeUuid + Send + Sync + 'static> OutputData for Val<T> {
    fn put_write<P: ProcessorAccess>(access: &mut P, index: u32, value: Self) {
        access.put_write(index, value);
    }

    fn writes() -> Vec<TypeId> {
        vec![<Arg<T> as ProcessorType>::get_processor_type()]
    }
}

impl<T: ProcessorType + Send + Sync + 'static> OutputData for Vec<T> {
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

pub struct Val<T> {
    inner: T,
}

impl<T> From<T> for Val<T> {
    fn from(v: T) -> Val<T> {
        Val { inner: v }
    }
}

impl<T> Deref for Val<T>
{
    type Target = T;

    fn deref(&self) -> &T {
        &self.inner
    }
}

pub struct Arg<'a, T: 'static> {
    inner: &'a T,
}

impl<'a, T: 'a> From<&'a T> for Arg<'a, T> {
    fn from(v: &'a T) -> Arg<'a, T> {
        Arg { inner: v }
    }
}

impl<'a, T: 'a> Deref for Arg<'a, T>
{
    type Target = T;

    fn deref(&self) -> &T {
        self.inner
    }
}

macro_rules! impl_inputs {
    ( $($ty:ident, $idx:expr),* ) => {
        impl<'a, $($ty),*> InputData<'a> for ( $( $ty , )* )
            where $( $ty : InputData<'a>),*
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
    inputs: Vec<Option<Box<ProcessorObj>>>,
    outputs: Vec<Option<Box<ProcessorObj>>>,
}

impl ProcessorValues {
    pub fn new(inputs: Vec<Option<Box<ProcessorObj>>>) -> ProcessorValues {
        ProcessorValues { inputs: inputs, outputs: Vec::new() }
    }
    pub fn outputs(&self) -> &Vec<Option<Box<ProcessorObj>>> {
        &self.outputs
    }
}

impl ProcessorAccess for ProcessorValues {
    fn get_read<T: ProcessorObj>(&mut self, index: u32) -> T {
        let val = self.inputs.remove(index as usize);
        self.inputs.insert(index as usize, None);
        // println!("expected {:?} got {:?}", <T as ProcessorType>::get_processor_type(), ProcessorObj::get_processor_type(val.as_ref().unwrap().as_ref()));
        *val.unwrap().downcast().unwrap()
    }
    fn put_write<T: ProcessorObj>(&mut self, index: u32, value: T) {
        self.outputs.insert(index as usize, Some(Box::new(value)));
    }
}
pub struct IOData {
    pub value: Option<Box<ProcessorObj>>,
    pub name: String,
}
impl IOData {
    pub fn new(name: String, value: Option<Box<ProcessorObj>>) -> IOData {
        IOData {
            value, 
            name,
        }
    }
}
pub struct ConstantProcessor {
    outputs: Vec<IOData>,
}
impl ConstantProcessor {
    pub fn new(values: Vec<IOData>) -> ConstantProcessor {
        ConstantProcessor { outputs: values }
    }
}

impl AnyProcessor for ConstantProcessor {
     fn name(&self) -> &'static str { "Constants" }
     fn input_names(&self) -> Vec<String> { vec![] }
     fn output_names(&self) -> Vec<String> { self.outputs.iter().map(|d| d.name.clone()).collect() }
     fn inputs(&self) -> Vec<TypeId> { vec![] }
     fn outputs(&self) -> Vec<TypeId> { self.outputs.iter().filter(|d| d.value.is_some()).map(|d| ProcessorObj::get_processor_type(d.value.as_ref().unwrap().as_ref())).collect() }
     fn run(&self, _access: &mut ProcessorValues) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_dyn::uuid;
    use std::marker::PhantomData;

    uuid!{
        ABC => 14092692613983100637224012401022025107
    }

    struct ABC {
    }
    impl<'a> Processor<'a> for ABC {
        fn name() -> &'static str { "ABC" }
        fn input_names() -> Vec<String> { vec!["f", "b"].iter().map(|d| d.to_string()).collect() }
        fn output_names() -> Vec<String> { vec!["g", "c"].iter().map(|d| d.to_string()).collect() }
        type Inputs = (Vec<Arg<'a, f32>>, Arg<'a, u16>);
        type Outputs = (Val<u32>, Val<u16>);
        fn run((f, b): Self::Inputs) -> Self::Outputs {
            let mut total = 0u32;
            for x in f.iter() {
                total += **x as u32;
            }
            total += **b as u32;
            (Val::from(total), Val::from(88u16))
        }
    }

    #[test]
    fn test() {
        let mut values = ProcessorValues::new(vec![ Some(Box::new(vec![Arg::from(3.2f32)])), Some(Box::new(Arg::from(2u16))) ]);
        ABC::run_now(&mut values);
        let out = values.outputs.remove(0).unwrap();
        println!("{}", Downcast::<Arg<u32>>::downcast_ref(out.as_ref()).unwrap().inner);
    }
}