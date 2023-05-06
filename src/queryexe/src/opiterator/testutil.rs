use crate::opiterator::OpIterator;
use common::CrustyError;
use common::Field;
use std::env;

use log::Level;
use log::info;
use common::testutil::init;

#[allow(dead_code)]
/// Returns the count of the number of tuples in an OpIterator.
///
/// This function consumes the iterator.
///
/// # Arguments
///
/// * `iter` - Iterator to count.
pub fn num_tuples(iter: &mut impl OpIterator) -> Result<u32, CrustyError> {
    let mut counter = 0;
    while iter.next()?.is_some() {
        counter += 1;
    }
    Ok(counter)
}

#[allow(dead_code)]
/// Sums all the int fields iterated over by an OpIterator
///
/// # Panics
///
/// Panics if a non-int field is present
pub fn sum_int_fields(iter: &mut impl OpIterator) -> Result<i32, CrustyError> {
    let mut sum = 0;
    while let Some(t) = iter.next()? {
        for i in 0..t.size() {
            sum += *match t.get_field(i).unwrap() {
                Field::IntField(n) => n,
                _ => panic!("Not an IntField"),
            }
        }
    }
    Ok(sum)
}

#[allow(dead_code)]
/// Asserts that iter1 and iter2 contain all the same tuples
pub fn match_all_tuples(
    mut iter1: Box<dyn OpIterator>,
    mut iter2: Box<dyn OpIterator>,
) -> Result<(), CrustyError> {
    let mut t1s = Vec::new();
    let mut t2s = Vec::new();
    while let Some(t1) = iter1.next()? {
        t1s.push(t1.clone());
    }
    while let Some(t2) = iter2.next()? {
        t2s.push(t2.clone());
    }

    assert_eq!(t1s.len(), t2s.len());

    for t1 in t1s.iter() {
        let mut found = false;
        for (i, t2) in t2s.iter().enumerate() {
            if t1 == t2 {
                found = true;
                t2s.remove(i);
                break;
            }
        }
        assert!(found);
    }
    Ok(())
}
