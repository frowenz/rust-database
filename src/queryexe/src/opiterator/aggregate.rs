#[allow(dead_code)]
#[allow(unused_imports)]
#[allow(unused_variables)]
use super::{OpIterator, TupleIterator};
use common::{AggOp, Attribute, CrustyError, DataType, Field, TableSchema, Tuple};
use std::cmp::{max, min};
use std::collections::HashMap;
use std::collections::HashSet;
use std::ops::Deref;
use std::vec::IntoIter;

/// Contains the index of the field to aggregate and the operator to apply to the column of each group. (You can add any other fields that you think are neccessary)
#[derive(Clone)]
pub struct AggregateField {
    /// Index of field being aggregated.
    pub field: usize,
    /// Agregate operation to aggregate the column with.
    pub op: AggOp,
}

/// Computes an aggregation function over multiple columns and grouped by multiple fields. (You can add any other fields that you think are neccessary)
struct Aggregator {
    /// Aggregated fields.
    agg_fields: Vec<AggregateField>,
    /// Group by fields
    groupby_fields: Vec<usize>,
    /// Schema of the output.
    schema: TableSchema,

    // Maps fields that define group to aggregation fields
    hashmap: HashMap<Vec<Field>, Vec<Field>>,

    // If we have an average aggregate field, then we have to keep track of a sum and a count field
    // Because the number of fields change, it is usefull to know if this is the case
    has_avg: bool,
}

impl Aggregator {
    /// Aggregator constructor.
    ///
    /// # Arguments
    ///
    /// * `agg_fields` - List of `AggregateField`s to aggregate over. `AggregateField`s contains the aggregation function and the field to aggregate over.
    /// * `groupby_fields` - Indices of the fields to groupby over.
    /// * `schema` - TableSchema of the form [groupby_field attributes ..., agg_field attributes ...]).
    fn new(
        agg_fields: Vec<AggregateField>,
        groupby_fields: Vec<usize>,
        schema: &TableSchema,
    ) -> Self {
        // Checks if there is an average field
        let mut has_avg = false;
        for agg_field in agg_fields.iter() {
            match agg_field.op {
                AggOp::Avg => {
                    has_avg = true;
                    break;
                }
                _ => continue,
            }
        }

        return Aggregator {
            agg_fields,
            groupby_fields,
            schema: schema.clone(),
            hashmap: HashMap::new(),
            has_avg,
        };
    }

    /// Handles the creation of groups for aggregation.
    ///
    /// If a group exists, then merge the tuple into the group's accumulated value.
    /// Otherwise, create a new group aggregate result.
    ///
    /// # Arguments
    ///
    /// * `tuple` - Tuple to add to a group.
    pub fn merge_tuple_into_group(&mut self, tuple: &Tuple) {
        // Get fields from tuple for each relevant groupby attribute
        let mut group_key = Vec::new();
        for groupby_field in self.groupby_fields.iter() {
            let field = tuple.get_field(*groupby_field).unwrap().clone();
            group_key.push(field);
        }

        // If the aggregate field(s) already exist(s) (i.e. we have already passed in tuple(s) with this group key)
        // Then we updadte the aggregate fields
        if let Some(stored_agg_fields) = self.hashmap.get_mut(&group_key) {
            // For each aggregate field
            for (index, agg_field) in self.agg_fields.iter().enumerate() {
                // Get the current tuple's relevant field
                let tuple_val = tuple.get_field(agg_field.field).unwrap().clone();

                // Check what aggregate operation we are performing
                match agg_field.op {
                    AggOp::Count => {
                        stored_agg_fields[index] =
                            Field::IntField(stored_agg_fields[index].unwrap_int_field() + 1);
                    }
                    AggOp::Max => {
                        stored_agg_fields[index] = max(stored_agg_fields[index].clone(), tuple_val);
                    }
                    AggOp::Min => {
                        stored_agg_fields[index] = min(stored_agg_fields[index].clone(), tuple_val);
                    }

                    // If average we just store a sum in this field
                    _ => {
                        stored_agg_fields[index] = Field::IntField(
                            stored_agg_fields[index].unwrap_int_field()
                                + tuple_val.unwrap_int_field(),
                        );
                    }
                }
            }
            // If an average field exists, we need to know the number of items in each group
            // Hence we had in an extra count field at the end
            if self.has_avg {
                if let Field::IntField(stored_agg_field) = stored_agg_fields[self.agg_fields.len()]
                {
                    stored_agg_fields[self.agg_fields.len()] = Field::IntField(
                        stored_agg_fields[self.agg_fields.len()].unwrap_int_field() + 1,
                    );
                }
            }
        }
        // Otherwise we must set new aggregate fields in the hashmap
        else {
            let mut agg_vals = Vec::new();
            for agg_field in self.agg_fields.iter() {
                let val = tuple.get_field(agg_field.field).unwrap().clone();
                match agg_field.op {
                    AggOp::Count => agg_vals.push(Field::IntField(1)),
                    AggOp::Min | AggOp::Max => agg_vals.push(val),
                    _ => agg_vals.push(Field::IntField(val.unwrap_int_field())),
                }
            }
            // Initialize the extra count field at the end if needed
            if self.has_avg {
                agg_vals.push(Field::IntField(1));
            }
            self.hashmap.insert(group_key, agg_vals);
        }
    }

    /// Returns a `TupleIterator` over the results.
    ///
    /// Resulting tuples must be of the form: (group by fields ..., aggregate fields ...)
    pub fn iterator(&self) -> TupleIterator {
        let mut all_tuples = Vec::new();

        // Finding the indexes of any aggregate field that performs an average
        // This allows us quickly check if we have an avg field when iterating through the hashmap
        let mut avg_indices: HashSet<usize> = HashSet::new();
        if self.has_avg {
            for (index, agg_field) in self.agg_fields.iter().enumerate() {
                match agg_field.op {
                    AggOp::Avg => {
                        avg_indices.insert(index);
                    }
                    _ => continue,
                }
            }
        }

        // Iterate through each bucket in the hashmap
        for (group_key, agg_vals) in self.hashmap.iter() {
            let mut tuple_fields = group_key.clone();

            // If an aggregate average field exists then we will have an extra count at the end that we do not want to return
            // Hence, go through each index except for the index corresponding to the final field (if it it exists)
            for index in 0..(agg_vals.len() - (self.has_avg as usize)) {
                let val = agg_vals[index].clone();

                // If this index is an average, then it currently has the value of sum stored in it
                // Hence, we must divide by the final count field and push this values
                if avg_indices.contains(&index) {
                    tuple_fields.push(Field::IntField(
                        val.unwrap_int_field() / agg_vals[agg_vals.len() - 1].unwrap_int_field(),
                    ));
                // Otherwise, just push the field
                } else {
                    tuple_fields.push(val)
                }
            }
            // Build a new tuple from these fields and push it
            all_tuples.push(Tuple::new(tuple_fields));
        }
        return TupleIterator::new(all_tuples, self.schema.clone());
    }
}

/// Aggregate operator. (You can add any other fields that you think are neccessary)
pub struct Aggregate {
    /// Fields to groupby over.
    groupby_fields: Vec<usize>,
    /// Aggregation fields and corresponding aggregation functions.
    agg_fields: Vec<AggregateField>,
    /// Aggregation iterators for results.
    agg_iter: Option<TupleIterator>,
    /// Output schema of the form [groupby_field attributes ..., agg_field attributes ...]).
    schema: TableSchema,
    /// Boolean if the iterator is open.
    open: bool,
    /// Child operator to get the data from.
    child: Box<dyn OpIterator>,
}

impl Aggregate {
    /// Aggregate constructor.
    ///
    /// # Arguments
    ///
    /// * `groupby_indices` - the indices of the group by fields
    /// * `groupby_names` - the names of the group_by fields in the final aggregation
    /// * `agg_indices` - the indices of the aggregate fields
    /// * `agg_names` - the names of the aggreagte fields in the final aggregation
    /// * `ops` - Aggregate operations, 1:1 correspondence with the indices in agg_indices
    /// * `child` - child operator to get the input data from.
    pub fn new(
        groupby_indices: Vec<usize>,
        groupby_names: Vec<&str>,
        agg_indices: Vec<usize>,
        agg_names: Vec<&str>,
        ops: Vec<AggOp>,
        child: Box<dyn OpIterator>,
    ) -> Self {
        // Scheme includes both grouby_attributes and agg_field attributes
        // We start building a datatype vector of for the groupby_attributes
        let child_schema = child.get_schema();
        let mut datatypes = Vec::new();
        for i in 0..groupby_indices.len() {
            let attribute = child_schema.get_attribute(groupby_indices[i]).unwrap();
            datatypes.push(attribute.dtype.clone());
        }

        // Building agg_fields
        let mut agg_fields = Vec::new();
        for i in 0..agg_indices.len() {
            let agg_field = AggregateField {
                field: agg_indices[i],
                op: ops[i],
            };
            agg_fields.push(agg_field);

            // Adding the datatypes for the agg_field attributes
            // We have to handle the case where we have a string and a min or max is being performed
            match ops[i] {
                AggOp::Min | AggOp::Max => {
                    match child_schema.get_attribute(agg_indices[i]).unwrap().dtype {
                        DataType::String => {
                            datatypes.push(DataType::String);
                        }
                        DataType::Int => {
                            datatypes.push(DataType::Int);
                        }
                    }
                }
                _ => {
                    datatypes.push(DataType::Int);
                }
            }
        }

        // Putting namesa and dataypes togehter to make the schema
        let mut names: Vec<&str> = groupby_names.clone();
        names.extend(agg_names);
        let schema = TableSchema::from_vecs(names, datatypes);

        return Aggregate {
            groupby_fields: groupby_indices,
            agg_fields,
            agg_iter: None,
            schema,
            open: false,
            child,
        };
    }
}

impl OpIterator for Aggregate {
    fn open(&mut self) -> Result<(), CrustyError> {
        if self.open {
            panic!("Operator has already been opened")
        }
        self.child.open()?;

        // Build new aggregator
        let mut aggregator = Aggregator::new(
            self.agg_fields.clone(),
            self.groupby_fields.clone(),
            &self.schema,
        );

        // Pass in every tuple
        while let Some(tuple) = self.child.next()? {
            aggregator.merge_tuple_into_group(&tuple);
        }

        // Initialize and open iterator
        let mut iter = aggregator.iterator();
        iter.open()?;
        self.agg_iter = Some(iter);
        self.open = true;

        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>, CrustyError> {
        if !self.open {
            panic!("Operator has not been opened")
        }
        return self.agg_iter.as_mut().unwrap().next();
    }

    fn close(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            panic!("Operator has not been opened")
        }
        self.agg_iter.as_mut().unwrap().close()?;
        self.child.close()?;
        self.open = false;
        return Ok(());
    }

    fn rewind(&mut self) -> Result<(), CrustyError> {
        self.agg_iter.as_mut().unwrap().rewind()?;
        return Ok(());
    }

    fn get_schema(&self) -> &TableSchema {
        &self.schema
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::opiterator::testutil::*;

    /// Creates a vector of tuples to create the following table:
    ///
    /// 1 1 3 E
    /// 2 1 3 G
    /// 3 1 4 A
    /// 4 2 4 G
    /// 5 2 5 G
    /// 6 2 5 G
    fn tuples() -> Vec<Tuple> {
        let tuples = vec![
            Tuple::new(vec![
                Field::IntField(1),
                Field::IntField(1),
                Field::IntField(3),
                Field::StringField("E".to_string()),
            ]),
            Tuple::new(vec![
                Field::IntField(2),
                Field::IntField(1),
                Field::IntField(3),
                Field::StringField("G".to_string()),
            ]),
            Tuple::new(vec![
                Field::IntField(3),
                Field::IntField(1),
                Field::IntField(4),
                Field::StringField("A".to_string()),
            ]),
            Tuple::new(vec![
                Field::IntField(4),
                Field::IntField(2),
                Field::IntField(4),
                Field::StringField("G".to_string()),
            ]),
            Tuple::new(vec![
                Field::IntField(5),
                Field::IntField(2),
                Field::IntField(5),
                Field::StringField("G".to_string()),
            ]),
            Tuple::new(vec![
                Field::IntField(6),
                Field::IntField(2),
                Field::IntField(5),
                Field::StringField("G".to_string()),
            ]),
        ];
        tuples
    }

    mod aggregator {
        use super::*;
        use common::{DataType, Field};

        /// Set up testing aggregations without grouping.
        ///
        /// # Arguments
        ///
        /// * `op` - Aggregation Operation.
        /// * `field` - Field do aggregation operation over.
        /// * `expected` - The expected result.
        fn test_no_group(op: AggOp, field: usize, expected: i32) -> Result<(), CrustyError> {
            let schema = TableSchema::new(vec![Attribute::new("agg".to_string(), DataType::Int)]);
            let mut agg = Aggregator::new(vec![AggregateField { field, op }], Vec::new(), &schema);
            let ti = tuples();
            for t in &ti {
                agg.merge_tuple_into_group(t);
            }

            let mut ai = agg.iterator();
            ai.open()?;
            assert_eq!(
                Field::IntField(expected),
                *ai.next()?.unwrap().get_field(0).unwrap()
            );
            assert_eq!(None, ai.next()?);
            Ok(())
        }

        #[test]
        fn test_merge_tuples_count() -> Result<(), CrustyError> {
            test_no_group(AggOp::Count, 0, 6)
        }

        #[test]
        fn test_merge_tuples_sum() -> Result<(), CrustyError> {
            test_no_group(AggOp::Sum, 1, 9)
        }

        #[test]
        fn test_merge_tuples_max() -> Result<(), CrustyError> {
            test_no_group(AggOp::Max, 0, 6)
        }

        #[test]
        fn test_merge_tuples_min() -> Result<(), CrustyError> {
            test_no_group(AggOp::Min, 0, 1)
        }

        #[test]
        fn test_merge_tuples_avg() -> Result<(), CrustyError> {
            test_no_group(AggOp::Avg, 0, 3)
        }

        #[test]
        #[should_panic]
        fn test_merge_tuples_not_int() {
            let _ = test_no_group(AggOp::Avg, 3, 3);
        }

        #[test]
        fn test_merge_multiple_ops() -> Result<(), CrustyError> {
            let schema = TableSchema::new(vec![
                Attribute::new("agg1".to_string(), DataType::Int),
                Attribute::new("agg2".to_string(), DataType::Int),
            ]);

            let mut agg = Aggregator::new(
                vec![
                    AggregateField {
                        field: 0,
                        op: AggOp::Max,
                    },
                    AggregateField {
                        field: 3,
                        op: AggOp::Count,
                    },
                ],
                Vec::new(),
                &schema,
            );

            let ti = tuples();
            for t in &ti {
                agg.merge_tuple_into_group(t);
            }

            let expected = vec![Field::IntField(6), Field::IntField(6)];
            let mut ai = agg.iterator();
            ai.open()?;
            assert_eq!(Tuple::new(expected), ai.next()?.unwrap());
            Ok(())
        }

        #[test]
        fn test_merge_tuples_one_group() -> Result<(), CrustyError> {
            let schema = TableSchema::new(vec![
                Attribute::new("group".to_string(), DataType::Int),
                Attribute::new("agg".to_string(), DataType::Int),
            ]);
            let mut agg = Aggregator::new(
                vec![AggregateField {
                    field: 0,
                    op: AggOp::Sum,
                }],
                vec![2],
                &schema,
            );

            let ti = tuples();
            for t in &ti {
                agg.merge_tuple_into_group(t);
            }

            let mut ai = agg.iterator();
            ai.open()?;
            let rows = num_tuples(&mut ai)?;
            assert_eq!(3, rows);
            Ok(())
        }

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

        #[test]
        fn test_merge_tuples_multiple_groups() -> Result<(), CrustyError> {
            let schema = TableSchema::new(vec![
                Attribute::new("group1".to_string(), DataType::Int),
                Attribute::new("group2".to_string(), DataType::Int),
                Attribute::new("agg".to_string(), DataType::Int),
            ]);

            let mut agg = Aggregator::new(
                vec![AggregateField {
                    field: 0,
                    op: AggOp::Sum,
                }],
                vec![1, 2],
                &schema,
            );

            let ti = tuples();
            for t in &ti {
                agg.merge_tuple_into_group(t);
            }

            let mut ai = agg.iterator();
            ai.open()?;
            let rows = num_tuples(&mut ai)?;
            assert_eq!(4, rows);
            Ok(())
        }
    }

    mod aggregate {
        use super::super::TupleIterator;
        use super::*;
        use common::{DataType, Field};

        fn tuple_iterator() -> TupleIterator {
            let names = vec!["1", "2", "3", "4"];
            let dtypes = vec![
                DataType::Int,
                DataType::Int,
                DataType::Int,
                DataType::String,
            ];
            let schema = TableSchema::from_vecs(names, dtypes);
            let tuples = tuples();
            TupleIterator::new(tuples, schema)
        }

        #[test]
        fn test_open() -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![0],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            assert!(!ai.open);
            ai.open()?;
            assert!(ai.open);
            Ok(())
        }

        fn test_single_agg_no_group(
            op: AggOp,
            op_name: &str,
            col: usize,
            expected: Field,
        ) -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![col],
                vec![op_name],
                vec![op],
                Box::new(ti),
            );
            ai.open()?;
            assert_eq!(
                // Field::IntField(expected),
                expected,
                *ai.next()?.unwrap().get_field(0).unwrap()
            );
            assert_eq!(None, ai.next()?);
            Ok(())
        }

        #[test]
        fn test_single_agg() -> Result<(), CrustyError> {
            test_single_agg_no_group(AggOp::Count, "count", 0, Field::IntField(6))?;
            test_single_agg_no_group(AggOp::Sum, "sum", 0, Field::IntField(21))?;
            test_single_agg_no_group(AggOp::Max, "max", 0, Field::IntField(6))?;
            test_single_agg_no_group(AggOp::Min, "min", 0, Field::IntField(1))?;
            test_single_agg_no_group(AggOp::Avg, "avg", 0, Field::IntField(3))?;
            test_single_agg_no_group(AggOp::Count, "count", 3, Field::IntField(6))?;
            test_single_agg_no_group(AggOp::Max, "max", 3, Field::StringField("G".to_string()))?;
            test_single_agg_no_group(AggOp::Min, "min", 3, Field::StringField("A".to_string()))
        }

        #[test]
        fn test_multiple_aggs() -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![3, 0, 0],
                vec!["count", "avg", "max"],
                vec![AggOp::Count, AggOp::Avg, AggOp::Max],
                Box::new(ti),
            );
            ai.open()?;

            let first_row: Vec<Field> = ai.next()?.unwrap().field_vals().cloned().collect();
            assert_eq!(
                vec![Field::IntField(6), Field::IntField(3), Field::IntField(6)],
                first_row
            );
            ai.close()
        }

        /// Consumes an OpIterator and returns a corresponding 2D Vec of fields
        pub fn iter_to_vec(iter: &mut impl OpIterator) -> Result<Vec<Vec<Field>>, CrustyError> {
            let mut rows = Vec::new();
            iter.open()?;
            while let Some(t) = iter.next()? {
                rows.push(t.field_vals().cloned().collect());
            }
            iter.close()?;
            Ok(rows)
        }

        #[test]
        fn test_multiple_aggs_groups() -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                vec![1, 2],
                vec!["group1", "group2"],
                vec![3, 0],
                vec!["count", "max"],
                vec![AggOp::Count, AggOp::Max],
                Box::new(ti),
            );
            let mut result = iter_to_vec(&mut ai)?;
            result.sort();
            let expected = vec![
                vec![
                    Field::IntField(1),
                    Field::IntField(3),
                    Field::IntField(2),
                    Field::IntField(2),
                ],
                vec![
                    Field::IntField(1),
                    Field::IntField(4),
                    Field::IntField(1),
                    Field::IntField(3),
                ],
                vec![
                    Field::IntField(2),
                    Field::IntField(4),
                    Field::IntField(1),
                    Field::IntField(4),
                ],
                vec![
                    Field::IntField(2),
                    Field::IntField(5),
                    Field::IntField(2),
                    Field::IntField(6),
                ],
            ];
            assert_eq!(expected, result);
            ai.open()?;
            let num_rows = num_tuples(&mut ai)?;
            ai.close()?;
            assert_eq!(4, num_rows);
            Ok(())
        }

        #[test]
        #[should_panic]
        fn test_next_not_open() {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![0],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            ai.next().unwrap();
        }

        #[test]
        fn test_close() -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![0],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            ai.open()?;
            assert!(ai.open);
            ai.close()?;
            assert!(!ai.open);
            Ok(())
        }

        #[test]
        #[should_panic]
        fn test_close_not_open() {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![0],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            ai.close().unwrap();
        }

        #[test]
        #[should_panic]
        fn test_rewind_not_open() {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![0],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            ai.rewind().unwrap();
        }

        #[test]
        fn test_rewind() -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                vec![2],
                vec!["group"],
                vec![3],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            ai.open()?;
            let count_before = num_tuples(&mut ai);
            ai.rewind()?;
            let count_after = num_tuples(&mut ai);
            ai.close()?;
            assert_eq!(count_before, count_after);
            Ok(())
        }

        #[test]
        fn test_get_schema() {
            let mut agg_names = vec!["count", "max"];
            let mut groupby_names = vec!["group1", "group2"];
            let ti = tuple_iterator();
            let ai = Aggregate::new(
                vec![1, 2],
                groupby_names.clone(),
                vec![3, 0],
                agg_names.clone(),
                vec![AggOp::Count, AggOp::Max],
                Box::new(ti),
            );
            groupby_names.append(&mut agg_names);
            let expected_names = groupby_names;
            let schema = ai.get_schema();
            for (i, attr) in schema.attributes().enumerate() {
                assert_eq!(expected_names[i], attr.name());
                assert_eq!(DataType::Int, *attr.dtype());
            }
        }
    }
}
